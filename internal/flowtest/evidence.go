// SPDX-License-Identifier: Apache-2.0
package flowtest

import (
	"strconv"
	"strings"
	"time"

	"github.com/trozet/ovn-light/internal/model"
)

type Endpoint struct {
	Node string `json:"node"`
	Pod  string `json:"pod,omitempty"`
	UID  string `json:"uid,omitempty"`
	IP   string `json:"ip,omitempty"`
}
type NodeEvidence struct {
	Flows   []ObservedFlow      `json:"flows,omitempty"`
	Node    string              `json:"node"`
	State   string              `json:"state"`
	At      time.Time           `json:"at,omitempty"`
	Paths   []model.OffloadPath `json:"paths,omitempty"`
	Limited bool                `json:"limited,omitempty"`
}
type Evaluation struct {
	Passed      bool     `json:"passed"`
	MinimumGbps float64  `json:"minimumGbps"`
	RXGbps      *float64 `json:"rxGbps,omitempty"`
	TXGbps      *float64 `json:"txGbps,omitempty"`
	Reason      string   `json:"reason,omitempty"`
}
type Traffic struct {
	TXGbps        *float64            `json:"txGbps,omitempty"`
	RXGbps        *float64            `json:"rxGbps,omitempty"`
	Offload       []OffloadValidation `json:"offload,omitempty"`
	Evaluation    *Evaluation         `json:"evaluation,omitempty"`
	Test          string              `json:"test,omitempty"`
	Case          string              `json:"case,omitempty"`
	TargetIP      string              `json:"targetIP,omitempty"`
	Transactions  float64             `json:"transactionsPerSecond,omitempty"`
	Loss          *float64            `json:"lostPercent,omitempty"`
	Jitter        *float64            `json:"jitterMs,omitempty"`
	Delivered     bool                `json:"delivered"`
	Bytes         int64               `json:"bytes,omitempty"`
	BPS           float64             `json:"bps,omitempty"`
	SourceIP      string              `json:"sourceIP,omitempty"`
	DestinationIP string              `json:"destinationIP,omitempty"`
	Error         string              `json:"error,omitempty"`
	Log           string              `json:"log,omitempty"`
}

type OffloadValidation struct {
	Plugin string         `json:"plugin"`
	Role   string         `json:"role,omitempty"`
	Passed bool           `json:"passed"`
	Reason string         `json:"reason,omitempty"`
	RX     *PacketCounter `json:"rx,omitempty"`
	TX     *PacketCounter `json:"tx,omitempty"`
}

type PacketCounter struct {
	Start   uint64  `json:"start"`
	End     uint64  `json:"end"`
	Packets *uint64 `json:"packets,omitempty"`
}

func (t Traffic) offloadFailed() bool {
	for _, result := range t.Offload {
		if !result.Passed {
			return true
		}
	}
	return false
}

type Run struct {
	TestSettings
	TestNamespace string         `json:"testNamespace,omitempty"`
	ServerPort    int            `json:"serverPort,omitempty"`
	TestName      string         `json:"testName,omitempty"`
	CaseName      string         `json:"caseName,omitempty"`
	Test          string         `json:"test,omitempty"`
	Case          string         `json:"case,omitempty"`
	ServiceIP     string         `json:"serviceIP,omitempty"`
	ID            string         `json:"id"`
	JobName       string         `json:"jobName,omitempty"`
	JobUID        string         `json:"jobUID"`
	Phase         string         `json:"phase"`
	Network       string         `json:"network"`
	Source        Endpoint       `json:"source"`
	Destination   Endpoint       `json:"destination"`
	Started       time.Time      `json:"started"`
	Finished      *time.Time     `json:"finished,omitempty"`
	Nodes         []NodeEvidence `json:"nodes,omitempty"`
	Traffic       *Traffic       `json:"traffic,omitempty"`
	Drop          string         `json:"drop,omitempty"`
}

func (p Profile) ports() (client, server string) {
	switch p.ID {
	case defaultTest, "iperf-udp":
		return "35201", "5201"
	case "netperf-tcp-rr", "netperf-tcp-stream":
		return "35202", "5202"
	default:
		return "", "5201"
	}
}

func (r *Run) ports() (client, server string) {
	if r.TestNamespace != "" {
		return "", strconv.Itoa(r.ServerPort)
	}
	profile, _ := lookupProfile(r.Test)
	return profile.ports()
}

func (r *Run) nativeRole(name string) string {
	if name == "normal-pod-server-"+strconv.Itoa(r.ServerPort) {
		return "server"
	}
	if name == "normal-pod-same-node-client" || name == "normal-pod-diff-node-client" {
		return "client"
	}
	return ""
}

func (r *Run) matches(p model.OffloadPath) bool {
	profile, _ := lookupProfile(r.Test)
	t := p.Tuple
	if r.Source.UID == "" || r.Destination.UID == "" || t == nil || profile.Protocol == "" || t.Protocol != profile.Protocol || p.Network != r.Network || p.ObservedAt.Before(r.Started) {
		return false
	}
	src, dst := r.Source, r.Destination
	clientPort, serverPort := t.SourcePort, t.DestinationPort
	wantClient, wantServer := r.ports()
	if t.SourcePort == wantServer {
		src, dst = dst, src
		clientPort, serverPort = t.DestinationPort, t.SourcePort
	}
	if serverPort != wantServer || wantClient != "" && clientPort != wantClient {
		return false
	}
	return src.IP == t.SourceIP && dst.IP == t.DestinationIP && (p.Source.UID == "" || p.Source.UID == src.UID) && (p.Destination.UID == "" || p.Destination.UID == dst.UID)
}

// Preserve a problem seen during the run even if the flow expires before the user opens the result.
func severity(p model.OffloadPath) int {
	switch p.Status {
	case "failure":
		return 5
	case "partial":
		return 4
	case "software":
		return 3
	case "hardware":
		return 2
	default:
		return 1
	}
}

func (r *Run) observe(v model.View, report model.OffloadReport, inv model.Inventory) {
	// TFT connects to status.podIP. A dual-stack pod can have two attachments
	// in the inventory; only the one on the test socket can identify this run.
	primary := map[string]string{}
	testNS := namespace
	if r.TestNamespace != "" {
		testNS = r.TestNamespace
	}
	for _, pod := range inv.Pods {
		if pod.Namespace != testNS {
			continue
		}
		if r.TestNamespace != "" {
			if pod.Labels["tft-pod-name"] == pod.Name && r.nativeRole(pod.Name) != "" && !pod.CreationTimestamp.Time.Before(r.Started) {
				primary[string(pod.UID)] = pod.Status.PodIP
			}
			continue
		}
		if pod.Labels[runLabel] != r.ID {
			continue
		}
		for _, owner := range pod.OwnerReferences {
			if owner.Kind == "Job" && owner.Name == jobName && string(owner.UID) == r.JobUID {
				primary[string(pod.UID)] = pod.Status.PodIP
			}
		}
	}
	scenario, _ := lookupCase(r.Case)
	if scenario.Service && r.ServiceIP == "" {
		for _, svc := range inv.Services {
			if svc.Namespace != testNS {
				continue
			}
			if r.TestNamespace != "" {
				if r.nativeRole(svc.Spec.Selector["tft-pod-name"]) == "server" && !svc.CreationTimestamp.Time.Before(r.Started) {
					r.ServiceIP = svc.Spec.ClusterIP
				}
				continue
			}
			if svc.Labels[runLabel] != r.ID {
				continue
			}
			for _, owner := range svc.OwnerReferences {
				if owner.Kind == "Job" && owner.Name == jobName && string(owner.UID) == r.JobUID {
					r.ServiceIP = svc.Spec.ClusterIP
				}
			}
		}
	}
	for _, pod := range v.Pods {
		if pod.Namespace != testNS || pod.Network != r.Network || pod.Role != "primary" || pod.IP == "" || pod.IP != primary[pod.UID] {
			continue
		}
		role := pod.Labels["ovn-light.io/role"]
		if r.TestNamespace != "" {
			role = r.nativeRole(pod.Pod)
		}
		var e *Endpoint
		switch role {
		case "client":
			e = &r.Source
		case "server":
			e = &r.Destination
		default:
			continue
		}
		if pod.Node == e.Node && (e.UID == "" || e.UID == pod.UID) {
			e.Pod, e.UID, e.IP = pod.Pod, pod.UID, pod.IP
		}
	}
	for i := range r.Nodes {
		n := &r.Nodes[i]
		r.recordTestFlows(n, v.TestPaths)
		n.Limited = n.Limited || v.TestPathsLimited[n.Node]
		before := n.At
		for _, node := range report.Nodes {
			if node.Name == n.Node && node.CapturedAt.After(n.At) && !node.CapturedAt.Before(r.Started) {
				n.At = node.CapturedAt
				if node.DiagnosticsAt.Before(r.Started) {
					continue
				}
				n.State = node.State
			}
		}
		// Only comparable intervals wholly within this run can demonstrate activity.
		if before.IsZero() || !n.At.After(before) {
			continue
		}
		for _, p := range report.Paths {
			if p.Node != n.Node || !p.ObservedAt.Equal(n.At) || !r.matches(p) || (p.ActivityDelta == nil || *p.ActivityDelta == 0) && (p.PartialDelta == nil || *p.PartialDelta == 0) {
				continue
			}
			p.Stages = append([]model.OffloadStage(nil), p.Stages...)
			direction := "client → server"
			if p.Tuple.SourceIP == r.Destination.IP {
				direction = "server → client"
			}
			for i := range p.Stages {
				if p.Stages[i].Raw != "" {
					n.keepFlow(p.Stages[i], direction, true, p.ObservedAt)
				}
				p.Stages[i].Raw = ""
			}
			p.Conntrack = append([]model.OffloadCT(nil), p.Conntrack...)
			for i := range p.Conntrack {
				ct := &p.Conntrack[i]
				ct.Raw = ""
				if ct.CapturedAt.Before(r.Started) {
					*ct = model.OffloadCT{Zone: ct.Zone, Status: "unknown", Reason: "Conntrack sample predates this test."}
					if p.Status == "hardware" || (p.CTProblem && p.Status == "software") {
						p.Status = "unknown"
						p.Summary = "Current conntrack evidence unavailable."
						p.Issue = false
					}
				}
			}
			// Limit stored evidence independently of the cluster's number of flows.
			if len(p.Stages) > 32 {
				p.Stages = p.Stages[:32]
				n.Limited = true
			}
			if len(p.Conntrack) > 8 {
				p.Conntrack = p.Conntrack[:8]
				n.Limited = true
			}
			slot := -1
			for j, old := range n.Paths {
				if old.ID == p.ID {
					slot = j
					break
				}
			}
			if slot >= 0 {
				old := n.Paths[slot]
				if severity(p) > severity(old) || severity(p) == severity(old) && (p.Issue || !old.Issue) && (old.PartialDelta == nil || p.PartialDelta != nil && *p.PartialDelta >= *old.PartialDelta) {
					n.Paths[slot] = p
				}
			} else if len(n.Paths) < 12 {
				n.Paths = append(n.Paths, p)
			} else {
				n.Limited = true
			}
		}
	}
	profile, _ := lookupProfile(r.Test)
	_, serverPort := r.ports()
	for _, f := range v.Findings {
		if r.Source.UID != "" && r.Destination.UID != "" && f.Source.UID == r.Source.UID && f.Destination.UID == r.Destination.UID && f.Source.Network == r.Network && strconv.Itoa(f.Destination.Port) == serverPort && profile.Protocol != "" && strings.EqualFold(f.Protocol, profile.Protocol) && f.Delta != nil && *f.Delta > 0 && !f.SharedCounter && f.ObservedAt.After(r.Started) {
			for _, c := range f.Causes {
				if c.ACL != "" {
					r.Drop = strings.TrimSpace(strings.Join([]string{c.Type, c.Namespace, c.Name, c.Direction, c.Action, c.ACL}, " "))
					break
				}
			}
		}
	}
}
