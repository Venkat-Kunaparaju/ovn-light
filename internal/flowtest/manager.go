// SPDX-License-Identifier: Apache-2.0
package flowtest

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/trozet/ovn-light/internal/model"
	batch "k8s.io/api/batch/v1"
	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes"
	"k8s.io/utils/ptr"
)

const namespace = "ovn-light-tests"
const jobName = "cross-node-tcp"
const runLabel = "ovn-light.io/run"

type TestSettings struct {
	MinimumGbps *float64 `json:"minimumGbps,omitempty"`
	SenderMbps  int      `json:"senderMbps,omitempty"`
}

type Request struct {
	TestSettings
	SourceNode      string `json:"sourceNode"`
	DestinationNode string `json:"destinationNode"`
	Network         string `json:"network"`
	Test            string `json:"test"`
	Case            string `json:"case"`
}
type Status struct {
	Enabled bool `json:"enabled"`
	Run     *Run `json:"run,omitempty"`
}
type Manager struct {
	client                    kubernetes.Interface
	image, device, rdma, file string
	helmNamespace             string
	wake                      chan struct{}
	operation                 sync.Mutex
	mu                        sync.Mutex
	run                       *Run
	previous                  []*Run
}

func New(client kubernetes.Interface, image, device, rdma, dir string) (*Manager, error) {
	for _, device := range []string{device, rdma} {
		if device != "" && (len(validation.IsQualifiedName(device)) > 0 || !strings.Contains(device, "/") || strings.HasPrefix(device, "kubernetes.io/")) {
			return nil, fmt.Errorf("invalid traffic test device resource")
		}
	}
	m := &Manager{rdma: rdma, wake: make(chan struct{}, 1), client: client, image: image, device: device, file: filepath.Join(dir, "traffic-test.json")}
	raw, err := os.ReadFile(m.file)
	if err == nil {
		if bytes.HasPrefix(bytes.TrimSpace(raw), []byte("[")) {
			var runs []*Run
			err = json.Unmarshal(raw, &runs)
			if err == nil && len(runs) > 0 {
				m.run, m.previous = runs[0], runs[1:]
			}
		} else {
			err = json.Unmarshal(raw, &m.run)
		}
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	for _, run := range append([]*Run{m.run}, m.previous...) {
		if run == nil {
			continue
		}
		// Earlier releases stored only the cross-node TCP profile.
		profile, _ := lookupProfile(run.Test)
		scenario, _ := lookupCase(run.Case)
		run.Test, run.Case = profile.ID, scenario.ID
		run.TestName, run.CaseName = profile.Name, scenario.Name
		if run.Traffic != nil {
			run.readOffloadPackets(run.Traffic.Log)
		}
	}
	return m, nil
}
func (m *Manager) saveLocked() error {
	if m.helmNamespace != "" {
		m.trimHistoryLocked()
	}
	b, err := json.Marshal(append([]*Run{m.run}, m.previous...))
	if err != nil {
		return err
	}
	if err = os.WriteFile(m.file+".tmp", b, 0600); err != nil {
		return err
	}
	return os.Rename(m.file+".tmp", m.file)
}

const historyLimit = 30

func (m *Manager) replaceLocked(next *Run) {
	if m.run != nil {
		if m.run.Finished == nil {
			now := time.Now().UTC()
			m.run.Finished, m.run.Phase = &now, "failed"
			m.run.Traffic = &Traffic{Error: "The test Job was replaced before its result was recorded."}
		}
		m.previous = append([]*Run{m.run}, m.previous[:min(len(m.previous), historyLimit-2)]...)
	}
	m.run = next
}

// List omits flow details so refreshing the main page stays cheap.
func (m *Manager) List() []Run {
	result := []Run{}
	if m == nil {
		return result
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.run == nil {
		return result
	}
	for _, run := range append([]*Run{m.run}, m.previous...) {
		summary := *run
		summary.Nodes = nil
		result = append(result, summary)
	}
	return result
}

func (m *Manager) Lookup(id string) *Run {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, run := range append([]*Run{m.run}, m.previous...) {
		if run != nil && run.ID == id {
			return copyRun(run)
		}
	}
	return nil
}

func copyRun(run *Run) *Run {
	b, _ := json.Marshal(run)
	var result *Run
	_ = json.Unmarshal(b, &result)
	return result
}

func (m *Manager) Status() Status {
	if m == nil {
		return Status{}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	result := Status{Enabled: true}
	if m.run != nil {
		result.Run = copyRun(m.run)
	}
	return result
}
func (m *Manager) Observe(v model.View, r model.OffloadReport, inventory model.Inventory) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, run := range append([]*Run{m.run}, m.previous...) {
		if run != nil && run.JobUID != "" && run.Finished == nil {
			run.observe(v, r, inventory)
		}
	}
}

func jobSpec(image, device, id string, q Request) *batch.Job {
	profile, _ := lookupProfile(q.Test)
	testCase, _ := lookupCase(q.Case)
	labels := map[string]string{runLabel: id, "app.kubernetes.io/managed-by": "ovn-light"}
	runner := core.Container{
		Name: "runner", Image: image, ImagePullPolicy: core.PullIfNotPresent,
		Env: []core.EnvVar{
			{Name: "RUN_ID", Value: id},
			{Name: "TEST_IMAGE", Value: image},
			{Name: "TEST_TYPE", Value: profile.ID},
			{Name: "TEST_CASE", Value: testCase.ID},
			{Name: "TEST_SECONDS", Value: fmt.Sprint(profile.Seconds)},
			{Name: "SOURCE_NODE", Value: q.SourceNode},
			{Name: "DESTINATION_NODE", Value: q.DestinationNode},
			{Name: "DEVICE_RESOURCE", Value: device},
			{Name: "JOB_UID", ValueFrom: &core.EnvVarSource{FieldRef: &core.ObjectFieldSelector{
				FieldPath: "metadata.labels['batch.kubernetes.io/controller-uid']",
			}}},
		},
		SecurityContext: &core.SecurityContext{
			AllowPrivilegeEscalation: ptr.To(false), ReadOnlyRootFilesystem: ptr.To(true),
			Capabilities: &core.Capabilities{Drop: []core.Capability{"ALL"}},
		},
		Resources: core.ResourceRequirements{
			Requests: core.ResourceList{core.ResourceCPU: resource.MustParse("10m"), core.ResourceMemory: resource.MustParse("32Mi")},
			Limits:   core.ResourceList{core.ResourceCPU: resource.MustParse("500m"), core.ResourceMemory: resource.MustParse("160Mi")},
		},
		VolumeMounts: []core.VolumeMount{{Name: "tmp", MountPath: "/tmp"}},
	}
	settings, _ := json.Marshal(q.TestSettings)
	if q.MinimumGbps != nil {
		runner.Env = append(runner.Env, core.EnvVar{Name: "MINIMUM_GBPS", Value: strconv.FormatFloat(*q.MinimumGbps, 'g', -1, 64)})
	}
	if q.SenderMbps > 0 {
		runner.Env = append(runner.Env, core.EnvVar{Name: "SENDER_MBPS", Value: strconv.Itoa(q.SenderMbps)})
	}
	return &batch.Job{
		ObjectMeta: meta.ObjectMeta{Name: jobName, Labels: labels, Annotations: map[string]string{
			"ovn-light.io/settings": string(settings), "ovn-light.io/test": profile.ID, "ovn-light.io/case": testCase.ID,
			"ovn-light.io/source": q.SourceNode, "ovn-light.io/destination": q.DestinationNode,
		}},
		Spec: batch.JobSpec{
			BackoffLimit: ptr.To(int32(0)), ActiveDeadlineSeconds: ptr.To(int64(340)), TTLSecondsAfterFinished: ptr.To(int32(60)),
			Template: core.PodTemplateSpec{ObjectMeta: meta.ObjectMeta{Labels: labels}, Spec: core.PodSpec{
				RestartPolicy: core.RestartPolicyNever, ServiceAccountName: "traffic-runner",
				Containers: []core.Container{runner}, TerminationGracePeriodSeconds: ptr.To(int64(2)),
				SecurityContext: &core.PodSecurityContext{
					RunAsNonRoot: ptr.To(true), RunAsUser: ptr.To(int64(10001)),
					SeccompProfile: &core.SeccompProfile{Type: core.SeccompProfileTypeRuntimeDefault},
				},
				Volumes: []core.Volume{{Name: "tmp", VolumeSource: core.VolumeSource{EmptyDir: &core.EmptyDirVolumeSource{
					SizeLimit: resource.NewQuantity(16<<20, resource.BinarySI),
				}}}},
			}},
		},
	}
}

func runFromJob(j *batch.Job) *Run {
	src, dst := j.Annotations["ovn-light.io/source"], j.Annotations["ovn-light.io/destination"]
	nodes := []NodeEvidence{{Node: src, State: "unknown"}}
	if src != dst {
		nodes = append(nodes, NodeEvidence{Node: dst, State: "unknown"})
	}
	profile, _ := lookupProfile(j.Annotations["ovn-light.io/test"])
	testCase, _ := lookupCase(j.Annotations["ovn-light.io/case"])
	var settings TestSettings
	_ = json.Unmarshal([]byte(j.Annotations["ovn-light.io/settings"]), &settings)
	serverPort, _ := strconv.Atoi(j.Annotations["ovn-light.io/server-port"])
	return &Run{
		TestNamespace: j.Annotations["ovn-light.io/test-namespace"], ServerPort: serverPort,
		TestSettings: settings, Test: profile.ID, TestName: profile.Name, Case: testCase.ID, CaseName: testCase.Name,
		ID: j.Labels[runLabel], JobName: j.Name, JobUID: string(j.UID), Network: "default", Phase: "running",
		Source: Endpoint{Node: src}, Destination: Endpoint{Node: dst}, Started: j.CreationTimestamp.Time, Nodes: nodes,
	}
}
func (m *Manager) Start(ctx context.Context, q Request) (Status, error) {
	if m == nil {
		return Status{}, fmt.Errorf("traffic tests are disabled")
	}
	profile, ok := lookupProfile(q.Test)
	if !ok {
		return Status{}, fmt.Errorf("unknown suite test %q", q.Test)
	}
	testCase, ok := lookupCase(q.Case)
	if !ok || testCase.Service && !profile.Service {
		return Status{}, fmt.Errorf("unsupported test case for %s", profile.Name)
	}
	if m.helmNamespace != "" {
		if profile.ID != defaultTest {
			return Status{}, fmt.Errorf("only TCP throughput tests are supported")
		}
		if q.MinimumGbps != nil && *q.MinimumGbps != 0 {
			return Status{}, fmt.Errorf("minimum throughput is fixed at 0 Gbit/s")
		}
		if q.SenderMbps != 0 {
			return Status{}, fmt.Errorf("TCP throughput uses the available bandwidth")
		}
		q.TestSettings = TestSettings{}
	} else {
		iperf := profile.ID == defaultTest || profile.ID == "iperf-udp"
		if q.MinimumGbps != nil {
			value := *q.MinimumGbps
			if !(iperf || profile.RDMA || profile.ID == "netperf-tcp-stream") || value <= 0 || math.IsNaN(value) || math.IsInf(value, 0) {
				return Status{}, fmt.Errorf("choose a bandwidth test and a positive, finite minimum in Gbit/s")
			}
		}
		if q.SenderMbps < 0 || q.SenderMbps > 1000000 || q.SenderMbps != 0 && (!iperf || q.MinimumGbps == nil) {
			return Status{}, fmt.Errorf("choose an iperf throughput check to set a sender rate of 1–1000000 Mbit/s")
		}
		if iperf && q.SenderMbps == 0 {
			q.SenderMbps = 1
		}
	}
	if q.Network != "default" {
		return Status{}, fmt.Errorf("choose the default network")
	}
	if (q.SourceNode == q.DestinationNode) != testCase.SameNode {
		return Status{}, fmt.Errorf("node selection must match the test case %s", testCase.ID)
	}
	dev := m.device
	if profile.RDMA {
		dev = m.rdma
		if dev == "" {
			return Status{}, fmt.Errorf("configure the RDMA device resource to enable this test")
		}
	}
	m.operation.Lock()
	defer m.operation.Unlock()
	if s := m.Status(); m.helmNamespace == "" && s.Run != nil && s.Run.Finished == nil {
		return s, fmt.Errorf("a traffic test is already running")
	}
	nodeNames := []string{q.SourceNode}
	if !testCase.SameNode {
		nodeNames = append(nodeNames, q.DestinationNode)
	}
	for _, name := range nodeNames {
		if len(validation.IsDNS1123Subdomain(name)) != 0 {
			return Status{}, fmt.Errorf("invalid node name")
		}
		n, err := m.client.CoreV1().Nodes().Get(ctx, name, meta.GetOptions{})
		if err != nil {
			return Status{}, err
		}
		if !readyNode(n) {
			return Status{}, fmt.Errorf("%s must be a schedulable Ready Linux node", name)
		}
		if dev != "" {
			needed := int64(1)
			if testCase.SameNode {
				needed = 2
			}
			qty := n.Status.Allocatable[core.ResourceName(dev)]
			if qty.Value() < needed {
				return Status{}, fmt.Errorf("%s needs %d allocatable %s devices", name, needed, dev)
			}
		}
	}
	// TFT reuses its pod and Service names. Job deletion can finish while an
	// endpoint still terminates, so keep a new run from attaching to it.
	if m.helmNamespace == "" {
		selector := meta.ListOptions{LabelSelector: runLabel, Limit: 1}
		pods, err := m.client.CoreV1().Pods(namespace).List(ctx, selector)
		if err != nil {
			return Status{}, err
		}
		if len(pods.Items) > 0 {
			return m.Status(), fmt.Errorf("previous test pods are being cleaned up; try again shortly")
		}
		if testCase.Service {
			services, err := m.client.CoreV1().Services(namespace).List(ctx, selector)
			if err != nil {
				return Status{}, err
			}
			if len(services.Items) > 0 {
				return m.Status(), fmt.Errorf("previous test Service is being cleaned up; try again shortly")
			}
		}
	}
	id := strings.ToLower(rand.Text())
	if m.helmNamespace != "" {
		return m.enqueue(id, q)
	}
	j, err := m.client.BatchV1().Jobs(namespace).Create(ctx, jobSpec(m.image, dev, id, q), meta.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		return m.Status(), fmt.Errorf("a test is running or its pods are being cleaned up")
	}
	if err != nil {
		return Status{}, err
	}
	m.mu.Lock()
	m.replaceLocked(runFromJob(j))
	err = m.saveLocked()
	m.mu.Unlock()
	if err != nil {
		log.Printf("persist traffic test: %v", err)
	}
	select {
	case m.wake <- struct{}{}:
	default:
	}
	return m.Status(), nil
}

func (m *Manager) Monitor(ctx context.Context) {
	for {
		err := m.reconcile(ctx)
		if err != nil && ctx.Err() == nil {
			log.Printf("traffic test: %v", err)
		}
		m.mu.Lock()
		active := false
		for _, run := range append([]*Run{m.run}, m.previous...) {
			active = active || run != nil && run.Finished == nil
		}
		m.mu.Unlock()
		var retry <-chan time.Time
		if err != nil || active {
			retry = time.After(4 * time.Second)
		}
		select {
		case <-m.wake:
		case <-ctx.Done():
			return
		case <-retry:
		}
	}
}
func (m *Manager) reconcile(ctx context.Context) error {
	if m.helmNamespace != "" {
		return m.reconcileHelm(ctx)
	}
	m.operation.Lock()
	defer m.operation.Unlock()
	jobs := m.client.BatchV1().Jobs(m.jobNamespace())
	j, err := jobs.Get(ctx, jobName, meta.GetOptions{})
	if apierrors.IsNotFound(err) {
		m.mu.Lock()
		defer m.mu.Unlock()
		if m.run != nil && m.run.Finished == nil {
			now := time.Now().UTC()
			m.run.Finished = &now
			m.run.Phase = "failed"
			m.run.Traffic = &Traffic{Error: "The test Job disappeared before its result was recorded."}
			return m.saveLocked()
		}
		return nil
	}
	if err != nil {
		return err
	}
	if j.Labels["app.kubernetes.io/managed-by"] != "ovn-light" || j.Labels[runLabel] == "" {
		return nil
	}
	m.mu.Lock()
	if m.run == nil || m.run.JobUID != string(j.UID) {
		m.replaceLocked(runFromJob(j))
	}
	run := m.run
	m.mu.Unlock()
	return m.reconcileJob(ctx, j, run)
}

func (m *Manager) reconcileJob(ctx context.Context, j *batch.Job, run *Run) error {
	m.mu.Lock()
	finished := run.Finished != nil
	m.mu.Unlock()
	if !finished {
		terminal, complete := false, false
		reason := ""
		for _, c := range j.Status.Conditions {
			if c.Status == core.ConditionTrue && (c.Type == batch.JobComplete || c.Type == batch.JobFailed) {
				terminal, complete = true, c.Type == batch.JobComplete
				reason = strings.TrimSpace(c.Reason + " " + c.Message)
				break
			}
		}
		if !terminal {
			if m.helmNamespace != "" {
				pods, err := m.client.CoreV1().Pods(m.jobNamespace()).List(ctx, meta.ListOptions{LabelSelector: "batch.kubernetes.io/controller-uid=" + string(j.UID)})
				if err != nil {
					return err
				}
				phase := "starting"
				for _, pod := range pods.Items {
					for _, c := range pod.Status.ContainerStatuses {
						if c.Name == "ovn-sdn-ft" && (c.State.Running != nil || c.State.Terminated != nil) {
							phase = "running"
						}
					}
				}
				m.mu.Lock()
				run.Phase = phase
				m.mu.Unlock()
			}
			return nil
		}
		result := Traffic{Error: reason}
		pods, err := m.client.CoreV1().Pods(m.jobNamespace()).List(ctx, meta.ListOptions{LabelSelector: "batch.kubernetes.io/controller-uid=" + string(j.UID)})
		if err != nil {
			return err
		}
		for _, p := range pods.Items {
			if !meta.IsControlledBy(&p, j) {
				continue
			}
			if m.helmNamespace != "" {
				result, err = m.helmResult(ctx, &p, j)
				if err != nil {
					return err
				}
				if !complete && !result.offloadFailed() && (result.Evaluation == nil || result.Evaluation.Passed) {
					result.Delivered = false
					result.Error = strings.TrimSpace("TFT Job failed. " + reason + " " + result.Error)
				}
				break
			}
			for _, c := range p.Status.ContainerStatuses {
				if t := c.State.Terminated; c.Name == "runner" && t != nil {
					var rec Traffic
					if json.Unmarshal([]byte(t.Message), &rec) == nil {
						result = rec
					}
					profile, _ := lookupProfile(run.Test)
					legacy := result.Test == "" && result.Case == "" && profile.ID == defaultTest && run.Case == crossPod
					if !legacy && (result.Test != run.Test || result.Case != run.Case) && result.Delivered {
						result.Delivered = false
						result.Error = "The runner returned a different test or case. Update the runner image."
					}
					if !complete || t.ExitCode != 0 || (profile.ID == defaultTest || profile.ID == "iperf-udp") && result.Bytes <= 0 {
						result.Delivered = false
					}
				}
			}
		}
		m.mu.Lock()
		now := time.Now().UTC()
		run.Finished = &now
		run.Traffic = &result
		run.Phase = "completed"
		if result.offloadFailed() {
			run.Phase = "failed"
		}
		if result.Delivered && run.MinimumGbps != nil {
			eval := result.Evaluation
			switch {
			case eval == nil || eval.MinimumGbps != *run.MinimumGbps:
				result.Evaluation = nil
				run.Phase = "failed"
				result.Error = "The runner did not return the requested throughput evaluation."
			case !eval.Passed:
				run.Phase = "failed"
			}
		}
		if !result.Delivered {
			run.Phase = "failed"
			if result.Error == "" {
				result.Error = "The runner did not report a completed transfer."
			}
		}
		m.mu.Unlock()
	}
	m.mu.Lock()
	err := m.saveLocked()
	m.mu.Unlock()
	if err != nil {
		return err
	}
	if j.DeletionTimestamp != nil {
		return nil
	}
	if err := m.cleanupHelmConfig(ctx, j); err != nil {
		return err
	}
	uid := types.UID(j.UID)
	err = m.client.BatchV1().Jobs(m.jobNamespace()).Delete(ctx, j.Name, meta.DeleteOptions{Preconditions: &meta.Preconditions{UID: &uid}, PropagationPolicy: ptr.To(meta.DeletePropagationForeground)})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}
