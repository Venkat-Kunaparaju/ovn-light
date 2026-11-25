// SPDX-License-Identifier: Apache-2.0
package flowtest

import (
	"context"
	"sort"

	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	defaultTest  = "iperf-tcp"
	crossPod     = "POD_TO_POD_DIFF_NODE"
	localPod     = "POD_TO_POD_SAME_NODE"
	crossService = "POD_TO_CLUSTER_IP_TO_POD_DIFF_NODE"
	localService = "POD_TO_CLUSTER_IP_TO_POD_SAME_NODE"
)

type Profile struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description"`
	Seconds     int    `json:"seconds"`
	Protocol    string `json:"protocol,omitempty"`
	Service     bool   `json:"service"`
	RDMA        bool   `json:"rdma,omitempty"`
}

var profiles = []Profile{
	{defaultTest, "iperf3 TCP", "One TCP stream at 1 Mbit/s for 120 seconds.", 120, "tcp", true, false},
	{"iperf-udp", "iperf3 UDP", "One UDP stream at 1 Mbit/s for 120 seconds. Reports receiver bitrate, loss and jitter.", 120, "udp", true, false},
	{"http", "HTTP response", "Fetch the suite’s test page once and verify its contents.", 10, "tcp", true, false},
	{"simple", "Simple TCP echo", "Exchange and verify small messages for 10 seconds.", 10, "tcp", true, false},
	{"netperf-tcp-stream", "Netperf TCP stream", "Measure throughput for 5 seconds at the available bandwidth. Uses a separate TCP data socket.", 5, "tcp", true, false},
	{"netperf-tcp-rr", "Netperf TCP request/response", "Measure transactions per second for 5 seconds over a separate TCP data socket.", 5, "tcp", true, false},
	{"ib-write-bw", "RDMA write bandwidth", "Measure RDMA write bandwidth between device-backed pods for 10 seconds.", 10, "", false, true},
	{"ib-read-bw", "RDMA read bandwidth", "Measure RDMA read bandwidth between device-backed pods for 10 seconds.", 10, "", false, true},
	{"ib-send-bw", "RDMA send bandwidth", "Measure RDMA send bandwidth for 1,000 iterations.", 10, "", false, true},
}

type TestCase struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	SameNode bool   `json:"sameNode"`
	Service  bool   `json:"service"`
}

var cases = []TestCase{
	{crossPod, "Pod to pod · different nodes", false, false},
	{localPod, "Pod to pod · same node", true, false},
	{crossService, "Pod through ClusterIP · different nodes", false, true},
	{localService, "Pod through ClusterIP · same node", true, true},
}

func lookupProfile(id string) (Profile, bool) {
	for _, p := range profiles {
		if p.ID == id || id == "" && p.ID == defaultTest {
			return p, true
		}
	}
	return Profile{}, false
}
func lookupCase(id string) (TestCase, bool) {
	for _, c := range cases {
		if c.ID == id || id == "" && c.ID == crossPod {
			return c, true
		}
	}
	return TestCase{}, false
}

func readyNode(n *core.Node) bool {
	if n.Spec.Unschedulable || n.Labels[core.LabelOSStable] != "linux" {
		return false
	}
	for _, c := range n.Status.Conditions {
		if c.Type == core.NodeReady {
			return c.Status == core.ConditionTrue
		}
	}
	return false
}

type NodeOption struct {
	Name        string `json:"name"`
	Devices     int64  `json:"devices"`
	RDMADevices int64  `json:"rdmaDevices"`
}
type Options struct {
	Runner       string       `json:"runner,omitempty"`
	Profiles     []Profile    `json:"profiles"`
	Cases        []TestCase   `json:"cases"`
	Nodes        []NodeOption `json:"nodes"`
	Device       string       `json:"device,omitempty"`
	RDMAResource string       `json:"rdmaResource,omitempty"`
}

// Only list nodes when the picker opens. Launch rechecks readiness and device capacity.
func (m *Manager) Options(ctx context.Context) (Options, error) {
	o := Options{Profiles: profiles, Cases: cases, Nodes: []NodeOption{}}
	if m == nil {
		return o, nil
	}
	if m.helmNamespace != "" {
		o.Runner = "helm"
		o.Profiles = []Profile{profiles[0]}
		o.Profiles[0].Description = "Run TCP iperf for 120 seconds using the installed TFT chart."
	}
	o.Device, o.RDMAResource = m.device, m.rdma
	nodes, err := m.client.CoreV1().Nodes().List(ctx, meta.ListOptions{})
	if err != nil {
		return o, err
	}
	for _, n := range nodes.Items {
		if readyNode(&n) {
			d := n.Status.Allocatable[core.ResourceName(m.device)]
			rd := n.Status.Allocatable[core.ResourceName(m.rdma)]
			o.Nodes = append(o.Nodes, NodeOption{n.Name, d.Value(), rd.Value()})
		}
	}
	sort.Slice(o.Nodes, func(i, j int) bool { return o.Nodes[i].Name < o.Nodes[j].Name })
	return o, nil
}
