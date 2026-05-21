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

// Package state holds the agent's coarse-grained "world model": the
// derived health state of each CSI driver component, plus inventory
// snapshots. The rules engine reads this; observers write to it.
package state

import (
	"sync"
	"time"
)

// Health is the coarse health state of a CSI driver component.
type Health string

const (
	HealthHealthy  Health = "healthy"
	HealthDegraded Health = "degraded"
	HealthDown     Health = "down"
	HealthUnknown  Health = "unknown"
)

// CSIDriverInfo describes the deployed CSI driver, gathered from pod
// containers and metrics endpoints.
type CSIDriverInfo struct {
	Name              string    `json:"name"`
	Version           string    `json:"version"`
	Namespace         string    `json:"namespace"`
	ControllerImage   string    `json:"controllerImage"`
	SyncerImage       string    `json:"syncerImage"`
	NodeImage         string    `json:"nodeImage"`
	ControllerReplicas int      `json:"controllerReplicas"`
	NodeReplicas       int      `json:"nodeReplicas"`
	LastObserved      time.Time `json:"lastObserved"`
}

// HealthSnapshot is a point-in-time view of all driver components.
type HealthSnapshot struct {
	Controller         Health    `json:"controller"`
	Syncer             Health    `json:"syncer"`
	Node               Health    `json:"node"`     // worst across daemonset pods
	CNSReachability    Health    `json:"cnsReachability"`
	SyntheticCanary    Health    `json:"syntheticCanary"`
	NodePodsHealthy    int       `json:"nodePodsHealthy"`
	NodePodsTotal      int       `json:"nodePodsTotal"`
	LastUpdate         time.Time `json:"lastUpdate"`
}

// VolumeInventory summarises persistent-volume counts across the cluster.
type VolumeInventory struct {
	TotalPVs            int       `json:"totalPVs"`
	TotalPVCs           int       `json:"totalPVCs"`
	PVCsBound           int       `json:"pvcsBound"`
	PVCsPending         int       `json:"pvcsPending"`
	PVCsLost            int       `json:"pvcsLost"`
	VolumeAttachments   int       `json:"volumeAttachments"`
	AttachmentsAttached int       `json:"attachmentsAttached"`
	OldestPendingPVC    string    `json:"oldestPendingPvc,omitempty"`
	OldestPendingAge    string    `json:"oldestPendingAge,omitempty"`
	LastUpdate          time.Time `json:"lastUpdate"`
}

// Store is a thread-safe holder of the agent's world model.
type Store struct {
	mu        sync.RWMutex
	driver    CSIDriverInfo
	health    HealthSnapshot
	inventory VolumeInventory
}

// New constructs a Store seeded with Unknown health.
func New() *Store {
	now := time.Now().UTC()
	return &Store{
		health: HealthSnapshot{
			Controller:      HealthUnknown,
			Syncer:          HealthUnknown,
			Node:            HealthUnknown,
			CNSReachability: HealthUnknown,
			SyntheticCanary: HealthUnknown,
			LastUpdate:      now,
		},
		inventory: VolumeInventory{LastUpdate: now},
		driver:    CSIDriverInfo{LastObserved: now},
	}
}

func (s *Store) SetDriver(d CSIDriverInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d.LastObserved = time.Now().UTC()
	s.driver = d
}

func (s *Store) Driver() CSIDriverInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.driver
}

func (s *Store) SetHealth(h HealthSnapshot) {
	s.mu.Lock()
	defer s.mu.Unlock()
	h.LastUpdate = time.Now().UTC()
	s.health = h
}

func (s *Store) Health() HealthSnapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.health
}

func (s *Store) SetInventory(inv VolumeInventory) {
	s.mu.Lock()
	defer s.mu.Unlock()
	inv.LastUpdate = time.Now().UTC()
	s.inventory = inv
}

func (s *Store) Inventory() VolumeInventory {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.inventory
}
