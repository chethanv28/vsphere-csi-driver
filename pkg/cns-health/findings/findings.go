/*
Copyright 2026 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package findings defines the structured Finding produced by the cns-health
// rules engine and the in-memory Store that holds the agent's working set.
//
// A Finding is the unit of agentic output: it captures what the agent
// observed, what it concluded, what evidence supports the conclusion, and
// what action (if any) was taken. The HTTP dashboard renders findings
// directly, and the rules engine consults the Store to avoid acting on the
// same condition repeatedly.
package findings

import (
	"sort"
	"sync"
	"time"
)

// Severity is an ordered severity level for a Finding.
type Severity string

const (
	SeverityInfo     Severity = "info"
	SeverityWarning  Severity = "warning"
	SeverityCritical Severity = "critical"
)

// Status is the lifecycle status of a Finding.
type Status string

const (
	StatusOpen       Status = "open"
	StatusActed      Status = "acted"
	StatusVerified   Status = "verified"
	StatusEscalated  Status = "escalated"
	StatusSuppressed Status = "suppressed"
	StatusResolved   Status = "resolved"
)

// ResourceRef points at a Kubernetes object affected by the Finding.
type ResourceRef struct {
	Kind      string `json:"kind"`
	Namespace string `json:"namespace,omitempty"`
	Name      string `json:"name"`
}

// Finding is the agent's reasoned conclusion about the cluster's storage state.
// The Key uniquely identifies the (rule, target) pair so repeated observations
// of the same condition update one Finding rather than spawning new ones.
type Finding struct {
	Key         string        `json:"key"`
	RuleID      string        `json:"ruleId"`
	Severity    Severity      `json:"severity"`
	Status      Status        `json:"status"`
	Title       string        `json:"title"`
	Description string        `json:"description"`
	Affected    []ResourceRef `json:"affected,omitempty"`
	Evidence    []string      `json:"evidence,omitempty"`
	Action      string        `json:"action,omitempty"`
	FirstSeen   time.Time     `json:"firstSeen"`
	LastSeen    time.Time     `json:"lastSeen"`
}

// Store is a thread-safe in-memory collection of Findings keyed by Finding.Key.
type Store struct {
	mu     sync.RWMutex
	byKey  map[string]*Finding
	maxAge time.Duration
}

// NewStore returns a new empty Store. Findings older than maxAge with status
// Resolved are garbage-collected.
func NewStore(maxAge time.Duration) *Store {
	return &Store{
		byKey:  make(map[string]*Finding),
		maxAge: maxAge,
	}
}

// Upsert inserts or updates a Finding by its Key. The FirstSeen timestamp is
// preserved on update; LastSeen is always refreshed.
func (s *Store) Upsert(f Finding) *Finding {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	if existing, ok := s.byKey[f.Key]; ok {
		existing.Severity = f.Severity
		existing.Status = f.Status
		existing.Title = f.Title
		existing.Description = f.Description
		existing.Affected = f.Affected
		existing.Evidence = f.Evidence
		if f.Action != "" {
			existing.Action = f.Action
		}
		existing.LastSeen = now
		return existing
	}
	f.FirstSeen = now
	f.LastSeen = now
	cp := f
	s.byKey[f.Key] = &cp
	return &cp
}

// MarkResolved transitions the Finding with the given key to Resolved.
// No-op if the key is not present.
func (s *Store) MarkResolved(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if f, ok := s.byKey[key]; ok {
		f.Status = StatusResolved
		f.LastSeen = time.Now().UTC()
	}
}

// List returns a snapshot of all Findings, sorted by severity desc then LastSeen desc.
func (s *Store) List() []Finding {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Finding, 0, len(s.byKey))
	for _, f := range s.byKey {
		out = append(out, *f)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Severity != out[j].Severity {
			return severityRank(out[i].Severity) > severityRank(out[j].Severity)
		}
		return out[i].LastSeen.After(out[j].LastSeen)
	})
	return out
}

// GarbageCollect drops Resolved findings older than the configured maxAge.
func (s *Store) GarbageCollect() {
	s.mu.Lock()
	defer s.mu.Unlock()
	cutoff := time.Now().UTC().Add(-s.maxAge)
	for k, f := range s.byKey {
		if f.Status == StatusResolved && f.LastSeen.Before(cutoff) {
			delete(s.byKey, k)
		}
	}
}

func severityRank(s Severity) int {
	switch s {
	case SeverityCritical:
		return 3
	case SeverityWarning:
		return 2
	case SeverityInfo:
		return 1
	default:
		return 0
	}
}
