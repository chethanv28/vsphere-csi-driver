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

// Package agent runs the cns-health main reconcile loop:
//
//	observe → reason → act → remember
//
// The agent never blocks the HTTP server: observers, the rules engine,
// and the in-memory state store all expose thread-safe APIs and the agent
// itself runs in its own goroutine.
package agent

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/klog/v2"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/cns-health/findings"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/cns-health/observe"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/cns-health/rules"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/cns-health/state"
)

// Agent stitches the observers, the rules engine, and the in-memory stores
// together. It is constructed by the entry-point (cmd/cns-health/main.go)
// and started once.
type Agent struct {
	K8s             *observe.K8sObserver
	Metrics         *observe.MetricsObserver
	State           *state.Store
	Findings        *findings.Store
	Engine          *rules.Engine
	ReconcileEvery  time.Duration
	MetricScrapeEvery time.Duration

	lastReconcile atomic.Int64 // unix seconds
	lastErr       atomic.Value // string
	reconciles    atomic.Uint64

	mu sync.Mutex
}

// Run blocks until ctx is cancelled. It first runs the K8s observer's
// informer start (which waits for cache sync), then enters the periodic
// reconcile loop.
func (a *Agent) Run(ctx context.Context) error {
	if a.ReconcileEvery == 0 {
		a.ReconcileEvery = 30 * time.Second
	}
	if a.MetricScrapeEvery == 0 {
		a.MetricScrapeEvery = 60 * time.Second
	}

	klog.InfoS("starting cns-health agent",
		"reconcileEvery", a.ReconcileEvery,
		"metricScrapeEvery", a.MetricScrapeEvery)

	if err := a.K8s.Start(ctx); err != nil {
		return err
	}

	a.reconcileOnce(ctx)

	metricsTick := time.NewTicker(a.MetricScrapeEvery)
	defer metricsTick.Stop()
	reconcileTick := time.NewTicker(a.ReconcileEvery)
	defer reconcileTick.Stop()
	gcTick := time.NewTicker(10 * time.Minute)
	defer gcTick.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-metricsTick.C:
			a.Metrics.Scrape(ctx)
		case <-reconcileTick.C:
			a.reconcileOnce(ctx)
		case <-gcTick.C:
			a.Findings.GarbageCollect()
		}
	}
}

func (a *Agent) reconcileOnce(ctx context.Context) {
	a.mu.Lock()
	defer a.mu.Unlock()

	scrapeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	a.Metrics.Scrape(scrapeCtx)
	cancel()

	if err := a.K8s.Reconcile(ctx); err != nil {
		klog.ErrorS(err, "k8s observer reconcile failed")
		a.lastErr.Store(err.Error())
		return
	}

	health := a.State.Health()
	if a.Metrics.AnyReachable() {
		health.CNSReachability = state.HealthHealthy
	} else {
		health.CNSReachability = state.HealthDegraded
	}
	a.State.SetHealth(health)

	ruleCtx := rules.Context{
		Driver:    a.State.Driver(),
		Health:    a.State.Health(),
		Inventory: a.State.Inventory(),
		MetricsBy: a.Metrics.Summaries(),
	}
	newFindings := a.Engine.Run(ruleCtx)

	emitted := make(map[string]struct{}, len(newFindings))
	for _, f := range newFindings {
		emitted[f.Key] = struct{}{}
		a.Findings.Upsert(f)
	}
	for _, existing := range a.Findings.List() {
		if _, stillEmitted := emitted[existing.Key]; !stillEmitted &&
			existing.Status != findings.StatusResolved {
			a.Findings.MarkResolved(existing.Key)
		}
	}

	a.lastReconcile.Store(time.Now().Unix())
	a.reconciles.Add(1)
	a.lastErr.Store("")
}

// LastReconcile returns the time of the most recent successful reconcile.
func (a *Agent) LastReconcile() time.Time {
	return time.Unix(a.lastReconcile.Load(), 0).UTC()
}

// LastError returns the most recent reconcile error (empty if none).
func (a *Agent) LastError() string {
	if v := a.lastErr.Load(); v != nil {
		return v.(string)
	}
	return ""
}

// ReconcileCount returns the cumulative number of successful reconciles.
func (a *Agent) ReconcileCount() uint64 { return a.reconciles.Load() }
