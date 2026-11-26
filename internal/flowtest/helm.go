// SPDX-License-Identifier: Apache-2.0
package flowtest

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	batch "k8s.io/api/batch/v1"
	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes"
	"k8s.io/utils/ptr"
)

const chartCronJob = "ovn-sdn-flow-tests"
const chartConfig = "ovn-sdn-ft-config"
const userConfig = "ovn-sdn-ft-config-user"

func NewHelm(client kubernetes.Interface, ns, dir string) (*Manager, error) {
	if ns == "" {
		ns = "ovn-k8s-flow-tests"
	}
	if len(validation.IsDNS1123Label(ns)) != 0 {
		return nil, fmt.Errorf("invalid TFT chart namespace")
	}
	m, err := New(client, "", "", "", dir)
	if err != nil {
		return nil, err
	}
	m.helmNamespace = ns
	return m, nil
}

func (m *Manager) jobNamespace() string {
	if m.helmNamespace != "" {
		return m.helmNamespace
	}
	return namespace
}

func (m *Manager) startHelm(ctx context.Context, id string, q Request) (*batch.Job, error) {
	ns := m.helmNamespace
	cron, err := m.client.BatchV1().CronJobs(ns).Get(ctx, chartCronJob, meta.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("read TFT Helm CronJob: %w", err)
	}
	if cron.Spec.Suspend == nil || !*cron.Spec.Suspend {
		return nil, fmt.Errorf("the TFT CronJob must be suspended before running tests from OVN-Light")
	}
	base, err := m.client.CoreV1().ConfigMaps(ns).Get(ctx, chartConfig, meta.GetOptions{})
	if err != nil {
		return nil, err
	}
	caseIDs := map[string]string{localPod: "1", crossPod: "2", localService: "5", crossService: "6"}
	scenario, _ := lookupCase(q.Case)
	for _, skip := range strings.Fields(base.Data["TFT_SKIP_TESTS"]) {
		if skip == scenario.ID || skip == caseIDs[scenario.ID] {
			return nil, fmt.Errorf("%s is disabled by the chart's TFT_SKIP_TESTS", scenario.ID)
		}
	}
	profile, _ := lookupProfile(q.Test)
	testNS := namespace + "-" + id
	data := map[string]string{
		"TFT_TEST_CASES":                    scenario.ID,
		"TFT_REQ_REMOTE_CLIENT_NODE":        q.SourceNode,
		"TFT_REQ_SERVER_NODE":               q.DestinationNode,
		"TFT_NAMESPACE":                     testNS,
		"TFT_IPERF":                         "true",
		"TFT_CURL":                          "false",
		"TFT_IPERF_TIME":                    strconv.Itoa(profile.Seconds),
		"TFT_PERF_MIN_TPUT":                 "0",
		"TFT_ENABLE_TARGET_ACCESS_SUBTESTS": "false",
		"TFT_JOB_DEFER_CLEANUP":             "false",
	}
	data["TFT_PLUGINS"] = "validate_offload"
	j := &batch.Job{
		ObjectMeta: meta.ObjectMeta{Name: helmJobName(id), Namespace: ns, Labels: map[string]string{runLabel: id, "app.kubernetes.io/managed-by": "ovn-light"}},
		Spec:       *cron.Spec.JobTemplate.Spec.DeepCopy(),
	}
	port := base.Data["TFT_IPERF_POD_PORT"]
	if port == "" {
		port = "5201"
	}
	settings, err := json.Marshal(q.TestSettings)
	if err != nil {
		return nil, err
	}
	j.Annotations = map[string]string{
		"ovn-light.io/settings": string(settings), "ovn-light.io/test": profile.ID,
		"ovn-light.io/case": scenario.ID, "ovn-light.io/source": q.SourceNode,
		"ovn-light.io/destination": q.DestinationNode, "ovn-light.io/test-namespace": testNS,
		"ovn-light.io/server-port": port,
	}
	if j.Spec.ActiveDeadlineSeconds == nil {
		j.Spec.ActiveDeadlineSeconds = ptr.To(int64(900))
	}
	j.Spec.BackoffLimit = ptr.To(int32(0))
	j.Spec.PodReplacementPolicy = ptr.To(batch.Failed)
	j.Spec.Template.Spec.RestartPolicy = core.RestartPolicyNever
	var runner *core.Container
	for i := range j.Spec.Template.Spec.Containers {
		if j.Spec.Template.Spec.Containers[i].Name == "ovn-sdn-ft" {
			runner = &j.Spec.Template.Spec.Containers[i]
		}
	}
	if runner == nil {
		return nil, fmt.Errorf("TFT CronJob is missing its ovn-sdn-ft container")
	}
	if len(runner.EnvFrom) != 2 || runner.EnvFrom[0].ConfigMapRef == nil || runner.EnvFrom[0].ConfigMapRef.Name != chartConfig || runner.EnvFrom[1].ConfigMapRef == nil || runner.EnvFrom[1].ConfigMapRef.Name != userConfig {
		return nil, fmt.Errorf("TFT CronJob must import %s followed by %s", chartConfig, userConfig)
	}
	for _, env := range runner.Env {
		if _, controlled := data[env.Name]; controlled {
			return nil, fmt.Errorf("TFT CronJob directly overrides %s; configure it through the chart ConfigMap", env.Name)
		}
	}
	cms := m.client.CoreV1().ConfigMaps(ns)
	cm, err := cms.Get(ctx, userConfig, meta.GetOptions{})
	if apierrors.IsNotFound(err) {
		cm, err = cms.Create(ctx, &core.ConfigMap{
			ObjectMeta: meta.ObjectMeta{Name: userConfig, Namespace: ns, Labels: map[string]string{runLabel: id}}, Data: data,
		}, meta.CreateOptions{})
	}
	if err == nil && cm.Labels[runLabel] != id {
		return nil, fmt.Errorf("%s is in use by another run", userConfig)
	}
	if err != nil {
		return nil, err
	}
	created, err := m.client.BatchV1().Jobs(ns).Create(ctx, j, meta.CreateOptions{})
	select {
	case m.wake <- struct{}{}:
	default:
	}
	// A timed-out create may have succeeded; its Job still needs the ConfigMap.
	if apierrors.IsForbidden(err) || apierrors.IsInvalid(err) {
		cleanup := m.client.CoreV1().ConfigMaps(ns).Delete(ctx, cm.Name, meta.DeleteOptions{Preconditions: &meta.Preconditions{UID: &cm.UID}})
		if cleanup != nil && !apierrors.IsNotFound(cleanup) {
			return nil, fmt.Errorf("create TFT Job: %w; clean up user ConfigMap: %v", err, cleanup)
		}
	}
	return created, err
}

var chartSummary = regexp.MustCompile(`Test ID: \([0-9]+\) ([A-Z0-9_]+), Test Type: (IPERF_TCP), (.*)`)
var chartBitrate = regexp.MustCompile(`TX Bitrate: ([0-9.eE+\-]+) Gbps, RX Bitrate: ([0-9.eE+\-]+) Gbps`)
var chartOffload = regexp.MustCompile(`plugin (validate_offload)(?: \(([^)]+)\))?, (succeeded|failed)(?:: ([^\r\n]*))?`)
var chartPackets = regexp.MustCompile(`ethtool statistics for pod (\S+) on node (\S+):\r?\nrx_packet_start: ([0-9]+|N/A)\r?\ntx_packet_start: ([0-9]+|N/A)\r?\nrx_packet_end: ([0-9]+|N/A)\r?\ntx_packet_end: ([0-9]+|N/A)`)

func packetCounter(start, end string) *PacketCounter {
	first, err := strconv.ParseUint(start, 10, 64)
	if err != nil {
		return nil
	}
	last, err := strconv.ParseUint(end, 10, 64)
	if err != nil {
		return nil
	}
	counter := &PacketCounter{Start: first, End: last}
	if last >= first {
		packets := last - first
		counter.Packets = &packets
	}
	return counter
}

func (r *Run) readOffloadPackets(raw string) {
	if r.Traffic == nil {
		return
	}
	for _, match := range chartPackets.FindAllStringSubmatch(raw, -1) {
		role := r.nativeRole(match[1])
		if role == "" || role == "client" && match[2] != r.Source.Node || role == "server" && match[2] != r.Destination.Node {
			continue
		}
		for i := range r.Traffic.Offload {
			result := &r.Traffic.Offload[i]
			if result.Plugin == "validate_offload" && strings.EqualFold(result.Role, role) {
				result.RX = packetCounter(match[3], match[5])
				result.TX = packetCounter(match[4], match[6])
			}
		}
	}
}

func (m *Manager) helmResult(ctx context.Context, pod *core.Pod, job *batch.Job) (Traffic, error) {
	result := Traffic{Test: job.Annotations["ovn-light.io/test"], Case: job.Annotations["ovn-light.io/case"]}
	if pod.Status.Phase != core.PodSucceeded && pod.Status.Phase != core.PodFailed {
		result.Error = "The TFT Job ended before its runner pod completed."
		return result, nil
	}
	raw, err := m.client.CoreV1().Pods(m.helmNamespace).GetLogs(pod.Name, &core.PodLogOptions{Container: "ovn-sdn-ft", TailLines: ptr.To(int64(500)), LimitBytes: ptr.To(int64(1 << 20))}).DoRaw(ctx)
	if err != nil {
		if apierrors.IsBadRequest(err) {
			for _, status := range pod.Status.ContainerStatuses {
				if status.Name == "ovn-sdn-ft" && status.State.Waiting != nil {
					result.Error = strings.TrimSpace(status.State.Waiting.Reason + " " + status.State.Waiting.Message)
					return result, nil
				}
			}
		}
		return result, fmt.Errorf("read TFT result log: %w", err)
	}
	result.Log = string(raw[max(0, len(raw)-(32<<10)):])
	for _, match := range chartOffload.FindAllStringSubmatch(string(raw), -1) {
		verdict := OffloadValidation{Plugin: match[1], Role: match[2], Passed: match[3] == "succeeded", Reason: match[4]}
		found := false
		for i, old := range result.Offload {
			if old.Plugin == verdict.Plugin && old.Role == verdict.Role {
				result.Offload[i], found = verdict, true
				break
			}
		}
		if !found {
			result.Offload = append(result.Offload, verdict)
		}
	}
	run := runFromJob(job)
	run.Traffic = &result
	run.readOffloadPackets(string(raw))
	for _, match := range chartSummary.FindAllStringSubmatch(string(raw), -1) {
		if match[1] != result.Case || match[2] != "IPERF_TCP" {
			continue
		}
		rates := chartBitrate.FindStringSubmatch(match[3])
		if len(rates) != 3 {
			continue
		}
		tx, _ := strconv.ParseFloat(rates[1], 64)
		rx, _ := strconv.ParseFloat(rates[2], 64)
		result.Delivered, result.BPS = tx > 0 && rx > 0, rx*1e9
		result.TXGbps, result.RXGbps = &tx, &rx
	}
	if !result.Delivered {
		result.Error = "TFT did not report a successful transfer for the selected test and case. See the TFT runner log."
	}
	return result, nil
}

func (m *Manager) cleanupHelmConfig(ctx context.Context, j *batch.Job) error {
	if m.helmNamespace == "" {
		return nil
	}
	cms := m.client.CoreV1().ConfigMaps(m.helmNamespace)
	cm, err := cms.Get(ctx, userConfig, meta.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if cm.Labels[runLabel] != j.Labels[runLabel] {
		return nil
	}
	err = cms.Delete(ctx, cm.Name, meta.DeleteOptions{Preconditions: &meta.Preconditions{UID: &cm.UID}})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}
