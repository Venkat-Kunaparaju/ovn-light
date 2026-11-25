// SPDX-License-Identifier: Apache-2.0
package ovn

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/trozet/ovn-light/internal/datapath"
	"github.com/trozet/ovn-light/internal/model"
)

type trafficIndex struct {
	testPorts   map[string]map[string]string
	attachments []Attachment
	ip, mac     map[string][]model.Endpoint
	hosts       map[string]string
}

func newTrafficIndex(inv model.Inventory, attachments []Attachment) trafficIndex {
	x := trafficIndex{testPorts: map[string]map[string]string{}, attachments: attachments, ip: map[string][]model.Endpoint{}, mac: map[string][]model.Endpoint{}, hosts: map[string]string{}}
	for _, a := range attachments {
		ep := a.Endpoint
		legacyTest := ep.Namespace == "ovn-light-tests" && ep.Labels["ovn-light.io/run"] != ""
		chartTest := strings.HasPrefix(ep.Namespace, "ovn-light-tests-") && ep.Labels["tft-pod-name"] != ""
		if (legacyTest || chartTest) && ep.Network == "default" && a.Primary {
			if x.testPorts[a.Node] == nil {
				x.testPorts[a.Node] = map[string]string{}
			}
			x.testPorts[a.Node][a.LogicalPort] = ep.UID
		}
		x.ip[a.Endpoint.Network+"/"+a.Endpoint.IP] = append(x.ip[a.Endpoint.Network+"/"+a.Endpoint.IP], a.Endpoint)
		if a.MAC != "" {
			x.mac[a.Endpoint.Network+"/"+a.MAC] = append(x.mac[a.Endpoint.Network+"/"+a.MAC], a.Endpoint)
		}
	}
	for _, p := range inv.Pods {
		if p.Status.HostIP != "" {
			x.hosts[p.Status.HostIP] = p.Spec.NodeName
		}
	}
	return x
}

func flowTime(n model.NodeSnapshot) time.Time {
	if !n.FlowCapturedAt.IsZero() {
		return n.FlowCapturedAt
	}
	return n.CapturedAt
}

func (x trafficIndex) endpoint(network string, m map[string]string, side string) model.Endpoint {
	ip := exactIP(m, side)
	e := model.Endpoint{IP: ip, Network: network}
	if network == "" {
		return e
	}
	candidates := x.ip[network+"/"+ip]
	if ip == "" {
		mac := datapath.Exact("eth."+side, m["eth."+side])
		for _, c := range x.mac[network+"/"+mac] {
			if sameFamily(m, c.IP) {
				candidates = append(candidates, c)
			}
		}
	}
	if len(candidates) == 1 {
		return candidates[0]
	}
	return e
}

func (x trafficIndex) traffic(n model.NodeSnapshot, paths []datapath.Path, old map[string]datapath.Flow, before model.NodeSnapshot, findings []model.Finding) []model.TrafficObservation {
	ports := map[string]string{}
	for _, p := range portRE.FindAllStringSubmatch(n.Ports, -1) {
		ports[p[1]] = p[2]
	}
	logical := map[string][]model.Endpoint{}
	for _, a := range x.attachments {
		if a.Node == n.Name {
			logical[a.LogicalPort] = append(logical[a.LogicalPort], a.Endpoint)
		}
	}
	interfaces := map[string][]model.Endpoint{}
	for _, row := range n.Interfaces {
		ext := Map(row["external_ids"])
		for _, e := range logical[Str(ext["iface-id"])] {
			if uid := Str(ext["iface-id-ver"]); uid == "" || uid == e.UID {
				interfaces[Str(row["name"])] = append(interfaces[Str(row["name"])], e)
			}
		}
	}
	local := func(port string, m map[string]string) model.Endpoint {
		if name := ports[port]; name != "" {
			port = name
		}
		var found []model.Endpoint
		for _, e := range interfaces[port] {
			if sameFamily(m, e.IP) {
				found = append(found, e)
			}
		}
		if len(found) == 1 {
			return found[0]
		}
		return model.Endpoint{}
	}
	byCounter := map[string][]model.TrafficObservation{}
	sources := map[string]model.Endpoint{}
	for _, p := range paths {
		last := p.Flows[len(p.Flows)-1]
		if last.Packets == 0 {
			continue
		}
		root := p.Flows[0]
		src, ok := sources[root.ID]
		if !ok {
			src = local(root.InPort, p.Original)
			if src.UID == "" {
				src = resolveSource(n, p, x.attachments)
			}
			sources[root.ID] = src
		}
		dst := model.Endpoint{}
		if p.Output != "" {
			dst = local(p.Output, p.Identity)
		}
		if dst.UID == "" {
			dst = x.endpoint(src.Network, p.Identity, "dst")
		}
		if dst.UID == "" {
			candidates := logicalDestinations(n, p, x.attachments, src.Network)
			if len(candidates) == 1 {
				dst = candidates[0]
			}
		}
		if src.UID == "" && dst.Network != "" {
			src = x.endpoint(dst.Network, p.Identity, "src")
			if src.UID == "" {
				src = x.endpoint(dst.Network, last.Match, "src")
			}
			if src.UID == "" {
				src = x.endpoint(dst.Network, p.Original, "src")
			}
		}
		if src.Network != "" && dst.Network != "" && src.Network != dst.Network {
			continue
		}
		network := src.Network
		if network == "" {
			network = dst.Network
		}
		if network == "" {
			continue
		}
		if src.Node == "" {
			src.Node = x.hosts[datapath.Exact("tunnel.src", p.Original["tunnel.src"])]
		}
		if dst.Node == "" {
			dst.Node = x.hosts[datapath.Exact("tunnel.dst", p.Identity["tunnel.dst"])]
		}
		proto, port := transport(p.Identity)
		dst.Port = port
		location := "transit"
		if src.Node == n.Name {
			location = "source"
		}
		if dst.Node == n.Name {
			location = "destination"
		}
		if src.Node == n.Name && dst.Node == n.Name {
			location = "local"
		}
		partial := !p.Complete
		for _, w := range p.Warnings {
			if strings.Contains(w, "not modeled") || strings.Contains(w, "Conditional") || strings.Contains(w, "requires additional") || strings.Contains(w, "limit reached") {
				partial = true
			}
		}
		o := model.TrafficObservation{CounterID: n.Name + "/" + last.ID, Node: n.Name, Network: network, Source: src, Destination: dst, Protocol: proto, Verdict: p.Verdict, Location: location, Packets: last.Packets, Bytes: last.Bytes, End: flowTime(n), Partial: partial, Offloaded: last.Offloaded, Warnings: append([]string{}, p.Warnings...)}
		o.CounterScope = "terminal"
		if p.ConditionalCounter {
			o.CounterScope = "branch-input"
		}
		if f, ok := old[o.CounterID]; ok && !strings.HasPrefix(last.ID, "synthetic-") && before.Datapath == n.Datapath && f.Actions == last.Actions && reflect.DeepEqual(f.Match, last.Match) && f.Offloaded == last.Offloaded && f.Packets <= last.Packets && f.Bytes <= last.Bytes && flowTime(before).Before(o.End) {
			dp, db := last.Packets-f.Packets, last.Bytes-f.Bytes
			o.PacketDelta, o.ByteDelta, o.Start = &dp, &db, flowTime(before)
		}
		byCounter[o.CounterID] = append(byCounter[o.CounterID], o)
	}
	ids := make([]string, 0, len(byCounter))
	for id := range byCounter {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	var out []model.TrafficObservation
	for _, id := range ids {
		candidates := byCounter[id]
		o := candidates[0]
		for _, other := range candidates[1:] {
			if trafficIdentity(o) == trafficIdentity(other) {
				continue
			}
			o.Shared = true
			o.Network = common(o.Network, other.Network)
			if o.Source.UID != other.Source.UID || o.Source.IP != other.Source.IP {
				o.Source = model.Endpoint{Node: common(o.Source.Node, other.Source.Node), Network: o.Network}
			}
			if o.Destination.UID != other.Destination.UID || o.Destination.IP != other.Destination.IP {
				o.Destination = model.Endpoint{Node: common(o.Destination.Node, other.Destination.Node), Network: o.Network}
			}
			if o.Destination.Port != other.Destination.Port {
				o.Destination.Port = 0
			}
			if o.Protocol != other.Protocol {
				o.Protocol = ""
			}
			if o.Location != other.Location {
				o.Location = "transit"
			}
		}
		if o.Shared {
			o.Source.Network, o.Destination.Network = o.Network, o.Network
			o.Warnings = append(o.Warnings, "This counter covers multiple compatible paths; per-connection rates are unavailable.")
		}
		for _, f := range findings {
			if f.CounterID == id && len(o.FindingIDs) < 16 {
				o.FindingIDs = append(o.FindingIDs, f.ID)
			}
		}
		out = append(out, o)
	}
	return out
}

func common(a, b string) string {
	if a == b {
		return a
	}
	return ""
}
func trafficIdentity(o model.TrafficObservation) string {
	return fmt.Sprintf("%s|%s|%s|%s|%s|%s|%s|%s|%d|%s|%s", o.Network, o.Source.Node, o.Source.UID, o.Source.IP, o.Destination.Node, o.Destination.UID, o.Destination.IP, o.Protocol, o.Destination.Port, o.Verdict, o.Location)
}
