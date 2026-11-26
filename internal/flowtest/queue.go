// SPDX-License-Identifier: Apache-2.0
package flowtest

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	batch "k8s.io/api/batch/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func helmJobName(id string) string { return "ovn-light-tft-" + id }

func (m *Manager) trimHistoryLocked() {
	finished := 0
	if m.run != nil && m.run.Finished != nil {
		finished++
	}
	var previous []*Run
	for _, run := range m.previous {
		if run.Finished != nil {
			finished++
			if finished > historyLimit {
				continue
			}
		}
		previous = append(previous, run)
	}
	m.previous = previous
}

func (m *Manager) enqueue(id string, q Request) (Status, error) {
	profile, _ := lookupProfile(q.Test)
	scenario, _ := lookupCase(q.Case)
	run := &Run{
		ID: id, JobName: helmJobName(id), TestNamespace: namespace + "-" + id,
		Test: profile.ID, TestName: profile.Name, Case: scenario.ID, CaseName: scenario.Name,
		Network: q.Network, Source: Endpoint{Node: q.SourceNode}, Destination: Endpoint{Node: q.DestinationNode},
		Phase: "queued", Started: time.Now().UTC(),
	}
	m.mu.Lock()
	previous := m.previous
	latest := m.run
	if latest != nil {
		m.previous = append([]*Run{latest}, previous...)
	}
	m.run = run
	err := m.saveLocked()
	if err != nil {
		m.run, m.previous = latest, previous
	}
	result := Status{Enabled: true, Run: copyRun(run)}
	m.mu.Unlock()
	if err != nil {
		return Status{}, err
	}
	select {
	case m.wake <- struct{}{}:
	default:
	}
	return result, nil
}

func (m *Manager) failRun(run *Run, message string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now().UTC()
	run.Finished, run.Phase, run.Traffic = &now, "failed", &Traffic{Error: message}
	return m.saveLocked()
}

func (m *Manager) reconcileHelm(ctx context.Context) error {
	m.operation.Lock()
	defer m.operation.Unlock()
	jobs, err := m.client.BatchV1().Jobs(m.helmNamespace).List(ctx, meta.ListOptions{LabelSelector: "app.kubernetes.io/managed-by=ovn-light"})
	if err != nil {
		return err
	}
	m.mu.Lock()
	runs := append([]*Run{m.run}, m.previous...)
	byID := map[string]*Run{}
	for _, run := range runs {
		if run != nil {
			byID[run.ID] = run
		}
	}
	for i := range jobs.Items {
		j := &jobs.Items[i]
		id := j.Labels[runLabel]
		if id == "" {
			continue
		}
		run := byID[id]
		if run == nil {
			run = runFromJob(j)
			if m.run != nil {
				m.previous = append(m.previous, run)
			} else {
				m.run = run
			}
			byID[id] = run
		} else if run.JobUID == "" {
			*run = *runFromJob(j)
		}
	}
	runs = append([]*Run{m.run}, m.previous...)
	err = m.saveLocked()
	m.mu.Unlock()
	if err != nil {
		return err
	}
	present := map[string]bool{}
	var failures []error
	for i := range jobs.Items {
		j := &jobs.Items[i]
		run := byID[j.Labels[runLabel]]
		if run == nil {
			continue
		}
		present[run.ID] = true
		if run.JobUID != string(j.UID) {
			failures = append(failures, fmt.Errorf("TFT Job %s has a different UID", j.Name))
			continue
		}
		if err := m.reconcileJob(ctx, j, run); err != nil {
			failures = append(failures, err)
		}
	}
	for _, run := range runs {
		if run == nil || run.JobUID == "" || present[run.ID] {
			continue
		}
		if run.Finished == nil {
			if err := m.failRun(run, "The test Job disappeared before its result was recorded."); err != nil {
				failures = append(failures, err)
			}
		}
	}
	if err := m.launchQueued(ctx, runs); err != nil {
		failures = append(failures, err)
	}
	return errors.Join(failures...)
}

func (m *Manager) launchQueued(ctx context.Context, runs []*Run) error {
	cms := m.client.CoreV1().ConfigMaps(m.helmNamespace)
	cm, err := cms.Get(ctx, userConfig, meta.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	if err == nil {
		for _, run := range runs {
			if run == nil || run.ID != cm.Labels[runLabel] || run.Finished == nil {
				continue
			}
			pods, err := m.client.CoreV1().Pods(m.helmNamespace).List(ctx, meta.ListOptions{LabelSelector: "batch.kubernetes.io/controller-uid=" + run.JobUID})
			if err != nil || len(pods.Items) != 0 {
				return err
			}
			j := &batch.Job{ObjectMeta: meta.ObjectMeta{Labels: map[string]string{runLabel: run.ID}}}
			return m.cleanupHelmConfig(ctx, j)
		}
	}
	var next *Run
	for i := len(runs) - 1; i >= 0; i-- {
		run := runs[i]
		if run != nil && run.Finished == nil && run.JobUID == "" {
			next = run
			break
		}
	}
	if next == nil || err == nil && cm.Labels[runLabel] != next.ID {
		return nil
	}
	m.mu.Lock()
	next.Phase = "starting"
	err = m.saveLocked()
	m.mu.Unlock()
	if err != nil {
		return err
	}
	j, startErr := m.startHelm(ctx, next.ID, Request{
		Test: next.Test, Case: next.Case, Network: next.Network,
		SourceNode: next.Source.Node, DestinationNode: next.Destination.Node,
	})
	if startErr != nil {
		j, err = m.client.BatchV1().Jobs(m.helmNamespace).Get(ctx, next.JobName, meta.GetOptions{})
		if err != nil {
			if !apierrors.IsNotFound(err) {
				return errors.Join(startErr, err)
			}
			_, err = cms.Get(ctx, userConfig, meta.GetOptions{})
			var networkError net.Error
			if !apierrors.IsNotFound(err) || errors.As(startErr, &networkError) || apierrors.IsTimeout(startErr) || apierrors.IsServerTimeout(startErr) || apierrors.IsServiceUnavailable(startErr) || apierrors.IsInternalError(startErr) {
				return startErr
			}
			return errors.Join(startErr, m.failRun(next, startErr.Error()))
		}
		if j.Labels[runLabel] != next.ID {
			return fmt.Errorf("TFT Job %s belongs to another run", j.Name)
		}
	}
	m.mu.Lock()
	*next = *runFromJob(j)
	next.Phase = "starting"
	err = m.saveLocked()
	m.mu.Unlock()
	return err
}
