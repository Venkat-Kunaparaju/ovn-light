// SPDX-License-Identifier: Apache-2.0
package flowtest

import (
	"strings"
	"time"

	"github.com/trozet/ovn-light/internal/datapath"
	"github.com/trozet/ovn-light/internal/model"
)

type ObservedFlow struct {
	model.OffloadStage
	Direction string    `json:"direction"`
	Exact     bool      `json:"exact"`
	At        time.Time `json:"at"`
}

func (n *NodeEvidence) keepFlow(s model.OffloadStage, direction string, exact bool, at time.Time) {
	if s.Packets == 0 && s.PartialPackets == 0 {
		return
	}
	if s.PartialPackets > 0 || s.PartialBytes > 0 {
		s.Status = "partial"
	}
	if len(s.Raw) > 4096 {
		s.Raw = s.Raw[:4096] + "…"
	}
	f := ObservedFlow{OffloadStage: s, Direction: direction, Exact: exact, At: at}
	for i, old := range n.Flows {
		if old.ID == s.ID && old.Direction == direction {
			if (!old.Exact || exact) && !at.Before(old.At) {
				n.Flows[i] = f
			}
			return
		}
	}
	if len(n.Flows) >= 32 {
		n.Limited = true
		return
	}
	n.Flows = append(n.Flows, f)
}

func (r *Run) recordTestFlows(n *NodeEvidence, samples []model.TestPathSample) {
	profile, _ := lookupProfile(r.Test)
	if profile.Protocol == "" || r.Source.UID == "" || r.Destination.UID == "" {
		return
	}
	for _, p := range samples {
		if p.Node != n.Node || p.At.Before(r.Started) {
			continue
		}
		client := p.PodUID == r.Source.UID && n.Node == r.Source.Node
		server := p.PodUID == r.Destination.UID && n.Node == r.Destination.Node
		if !client && !server {
			continue
		}
		// The request enters OVS at the client port and leaves OVS at the server port.
		forward := client == p.Outbound
		src, dst := r.Source.IP, r.Destination.IP
		sport, dport := r.ports()
		direction := "client → server"
		if !forward {
			src, dst = dst, src
			sport, dport = dport, sport
			direction = "server → client"
		}
		family, eth, proto := "ipv4", "2048", "6"
		if strings.Contains(src, ":") {
			family, eth = "ipv6", "34525"
		}
		if profile.Protocol == "udp" {
			proto = "17"
		}
		if _, ok := p.Match[family+".proto"]; !ok {
			continue
		}
		fields := map[string]string{"eth_type": eth, family + ".proto": proto, family + ".src": src, family + ".dst": dst, profile.Protocol + ".src": sport, profile.Protocol + ".dst": dport}
		compatible, exact := true, true
		serviceField := family + ".dst"
		if !forward {
			serviceField = family + ".src"
		}
		for k, v := range fields {
			observed := p.Match[k]
			if observed == "" || v == "" {
				exact = false
				continue
			}
			_, matches := datapath.Intersect(k, observed, v)
			// A Service path can expose the virtual IP before translation.
			if !matches && k == serviceField && r.ServiceIP != "" {
				_, matches = datapath.Intersect(k, observed, r.ServiceIP)
			}
			if !matches {
				compatible = false
				break
			}
			if datapath.Exact(k, observed) == "" {
				exact = false
			}
		}
		if compatible {
			for _, st := range p.Stages {
				n.keepFlow(st, direction, exact, p.At)
			}
		}
	}
}
