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

// Package rules holds the cns-health agent's reasoning step. A Rule
// consumes the current world model (state.Store + recent metric summaries)
// and emits zero or more Findings.
//
// Rules are deterministic functions — auditable, predictable, no external
// dependencies. The agent applies them in declared order on each reconcile.
package rules

import (
	"fmt"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/cns-health/findings"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/cns-health/observe"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/cns-health/state"
)

// Context groups the inputs available to every Rule.
type Context struct {
	Driver     state.CSIDriverInfo
	Health     state.HealthSnapshot
	Inventory  state.VolumeInventory
	MetricsBy  map[string]observe.MetricsSummary // keyed by endpoint URL
}

// Rule is the unit of reasoning. Each Rule returns Findings it wants to
// publish. The agent merges these into the Finding store (which keeps a
// single instance per key across reconciles).
type Rule interface {
	ID() string
	Evaluate(ctx Context) []findings.Finding
}

// Engine runs a list of rules in order.
type Engine struct {
	rules []Rule
}

// NewEngine returns an Engine seeded with the built-in rule set.
func NewEngine() *Engine {
	return &Engine{rules: BuiltinRules()}
}

// Add appends a rule.
func (e *Engine) Add(r Rule) { e.rules = append(e.rules, r) }

// Run evaluates every rule against ctx and returns the merged Finding list.
func (e *Engine) Run(ctx Context) []findings.Finding {
	var out []findings.Finding
	for _, r := range e.rules {
		out = append(out, r.Evaluate(ctx)...)
	}
	return out
}

// ---------------------------------------------------------------------------
// Built-in rules
// ---------------------------------------------------------------------------

func BuiltinRules() []Rule {
	return []Rule{
		controllerDownRule{},
		syncerDownRule{},
		nodePodsDegradedRule{},
		pendingPVCsRule{},
		inaccessibleVolumesRule{},
		highErrorRateRule{},
		highLatencyRule{},
		driverImageUnknownRule{},
	}
}

// ---------------------------------------------------------------------------

type controllerDownRule struct{}

func (controllerDownRule) ID() string { return "controller-down" }
func (controllerDownRule) Evaluate(c Context) []findings.Finding {
	if c.Health.Controller != state.HealthDown {
		return nil
	}
	return []findings.Finding{{
		Key:         "controller-down",
		RuleID:      "controller-down",
		Severity:    findings.SeverityCritical,
		Status:      findings.StatusOpen,
		Title:       "CSI controller pod is down",
		Description: "No vsphere-csi-controller pod is currently Ready. Volume provisioning, attachment, and snapshot operations cannot proceed until this is resolved.",
		Evidence:    []string{fmt.Sprintf("controller replicas observed: %d", c.Driver.ControllerReplicas)},
		Action:      "Page on-call; investigate controller pod restart logs and resource pressure.",
	}}
}

type syncerDownRule struct{}

func (syncerDownRule) ID() string { return "syncer-down" }
func (syncerDownRule) Evaluate(c Context) []findings.Finding {
	if c.Health.Syncer != state.HealthDown {
		return nil
	}
	return []findings.Finding{{
		Key:         "syncer-down",
		RuleID:      "syncer-down",
		Severity:    findings.SeverityCritical,
		Status:      findings.StatusOpen,
		Title:       "CSI syncer is down",
		Description: "The vsphere-syncer container is not Ready. Periodic reconciliation between Kubernetes objects and CNS volumes is stalled; drift will grow over time.",
		Action:      "Restart vsphere-syncer container and inspect logs for connectivity to vCenter.",
	}}
}

type nodePodsDegradedRule struct{}

func (nodePodsDegradedRule) ID() string { return "node-pods-degraded" }
func (nodePodsDegradedRule) Evaluate(c Context) []findings.Finding {
	if c.Health.NodePodsTotal == 0 || c.Health.Node == state.HealthHealthy {
		return nil
	}
	sev := findings.SeverityWarning
	if c.Health.Node == state.HealthDown {
		sev = findings.SeverityCritical
	}
	return []findings.Finding{{
		Key:         "node-pods-degraded",
		RuleID:      "node-pods-degraded",
		Severity:    sev,
		Status:      findings.StatusOpen,
		Title:       fmt.Sprintf("CSI node DaemonSet degraded: %d/%d Ready", c.Health.NodePodsHealthy, c.Health.NodePodsTotal),
		Description: "Some vsphere-csi-node pods are not Ready. Pods scheduled on the affected nodes will fail to mount persistent volumes.",
		Action:      "Inspect affected node-pods, container logs, and the underlying nodes.",
	}}
}

type pendingPVCsRule struct{}

func (pendingPVCsRule) ID() string { return "pending-pvcs" }
func (pendingPVCsRule) Evaluate(c Context) []findings.Finding {
	if c.Inventory.PVCsPending == 0 {
		return nil
	}
	sev := findings.SeverityInfo
	if c.Inventory.PVCsPending >= 5 {
		sev = findings.SeverityWarning
	}
	if c.Inventory.PVCsPending >= 50 {
		sev = findings.SeverityCritical
	}
	return []findings.Finding{{
		Key:      "pending-pvcs",
		RuleID:   "pending-pvcs",
		Severity: sev,
		Status:   findings.StatusOpen,
		Title:    fmt.Sprintf("%d PersistentVolumeClaim(s) Pending", c.Inventory.PVCsPending),
		Description: fmt.Sprintf("Pending PVCs prevent associated workloads from starting. Oldest pending: %s (age %s).",
			c.Inventory.OldestPendingPVC, c.Inventory.OldestPendingAge),
		Action: "Inspect oldest pending PVC events; check StorageClass parameters and CSI controller logs.",
	}}
}

type inaccessibleVolumesRule struct{}

func (inaccessibleVolumesRule) ID() string { return "inaccessible-volumes" }
func (inaccessibleVolumesRule) Evaluate(c Context) []findings.Finding {
	var inaccessible float64
	for _, m := range c.MetricsBy {
		if m.InaccessibleVolumes > inaccessible {
			inaccessible = m.InaccessibleVolumes
		}
	}
	if inaccessible <= 0 {
		return nil
	}
	sev := findings.SeverityWarning
	if inaccessible >= 5 {
		sev = findings.SeverityCritical
	}
	return []findings.Finding{{
		Key:         "inaccessible-volumes",
		RuleID:      "inaccessible-volumes",
		Severity:    sev,
		Status:      findings.StatusOpen,
		Title:       fmt.Sprintf("%.0f CNS volume(s) reported inaccessible on the storage backend", inaccessible),
		Description: "The CSI driver's volume-health gauge reports volumes that vCenter considers inaccessible. Workloads using these volumes will see I/O errors.",
		Action:      "Run CNS Manager orphan-volume detection; verify datastore health in vCenter.",
	}}
}

type highErrorRateRule struct{}

func (highErrorRateRule) ID() string { return "high-error-rate" }
func (highErrorRateRule) Evaluate(c Context) []findings.Finding {
	var totalOps, totalErrs float64
	worstFault := ""
	var worstFaultRate float64
	for _, m := range c.MetricsBy {
		for op, v := range m.OpsPerMinByOp {
			_ = op
			totalOps += v
		}
		for f, v := range m.ErrorsPerMinByFault {
			totalErrs += v
			if v > worstFaultRate {
				worstFaultRate = v
				worstFault = f
			}
		}
	}
	if totalOps == 0 {
		return nil
	}
	errPct := totalErrs / totalOps * 100
	if errPct < 5 {
		return nil
	}
	sev := findings.SeverityWarning
	if errPct >= 15 {
		sev = findings.SeverityCritical
	}
	return []findings.Finding{{
		Key:         "high-error-rate",
		RuleID:      "high-error-rate",
		Severity:    sev,
		Status:      findings.StatusOpen,
		Title:       fmt.Sprintf("CSI operation error rate %.1f%% (last scrape interval)", errPct),
		Description: fmt.Sprintf("Largest contributing fault type: %s at %.2f errors/min. Sustained error rates above 5%% typically indicate backend storage or vCenter saturation.", worstFault, worstFaultRate),
		Action:      "Inspect controller logs; check vCenter health; correlate with recent storage changes.",
	}}
}

type highLatencyRule struct{}

func (highLatencyRule) ID() string { return "high-latency" }
func (highLatencyRule) Evaluate(c Context) []findings.Finding {
	var worstOp string
	var worstP95 float64
	for _, m := range c.MetricsBy {
		for op, p95 := range m.P95LatencySecondsByOp {
			if p95 > worstP95 {
				worstP95 = p95
				worstOp = op
			}
		}
	}
	if worstP95 < 15 {
		return nil
	}
	sev := findings.SeverityWarning
	if worstP95 >= 30 {
		sev = findings.SeverityCritical
	}
	return []findings.Finding{{
		Key:         "high-latency",
		RuleID:      "high-latency",
		Severity:    sev,
		Status:      findings.StatusOpen,
		Title:       fmt.Sprintf("%s p95 latency %.1fs (threshold 15s)", worstOp, worstP95),
		Description: "A CSI operation's p95 latency has crossed the warning threshold. Latency in the tens-of-seconds range often precedes timeout-induced failures.",
		Action:      "Investigate vCenter storage load; check datastore queue depths.",
	}}
}

type driverImageUnknownRule struct{}

func (driverImageUnknownRule) ID() string { return "driver-image-unknown" }
func (driverImageUnknownRule) Evaluate(c Context) []findings.Finding {
	if c.Driver.ControllerImage != "" {
		return nil
	}
	return []findings.Finding{{
		Key:         "driver-image-unknown",
		RuleID:      "driver-image-unknown",
		Severity:    findings.SeverityInfo,
		Status:      findings.StatusOpen,
		Title:       "CSI driver image could not be determined",
		Description: "The agent did not observe any vsphere-csi-controller pod via informers. This usually indicates RBAC misconfiguration or that the agent is running before the controller Deployment has scheduled.",
		Action:      "Verify the cns-health ServiceAccount has 'pods' read access in the driver namespace.",
	}}
}
