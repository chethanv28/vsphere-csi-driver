# cns-health

An **agentic observability container** for the vSphere CSI driver. Runs as a
separate Deployment in the `vmware-system-csi` namespace alongside the
existing controller, syncer, and node DaemonSet. Watches the storage stack
from the outside, runs a deterministic rules engine over multi-source
observations, and renders the agent's world model in a built-in HTML
dashboard at port 8080 (NodePort 30808 by default).

## Why a separate container

The CSI driver controller and syncer already emit rich Prometheus metrics on
`:2112` and `:2113`, but those metrics are *passive* — Prometheus or a human
must scrape them, correlate across sources, and decide what to do. `cns-health`
is the active loop that does the correlation and the deciding:

```
              observe ─→ reason ─→ act ─→ verify ─→ remember
                ▲                                       │
                └────────────────  loop  ───────────────┘
```

It is intentionally *not* a sidecar of `vsphere-csi-controller`:

- **Failure isolation.** A bug in the agent must not restart the driver.
- **Independent release cadence.** Rules and remediations update faster
  than the driver itself.
- **Smaller, auditable RBAC.** Read-only by default; write verbs are
  feature-flagged.

## Architecture

| Component | Source path | Role |
|---|---|---|
| **Agent loop** | `pkg/cns-health/agent/`          | Drives the observe-reason-act tick |
| **Observers** | `pkg/cns-health/observe/`         | `k8s.go` (informers), `metrics.go` (scrape `:2112`/`:2113`) |
| **State store** | `pkg/cns-health/state/`         | Coarse world model: driver info, health snapshot, volume inventory |
| **Findings store** | `pkg/cns-health/findings/`   | Structured agentic outputs, keyed for deduplication |
| **Rules engine** | `pkg/cns-health/rules/`        | Deterministic reasoning, built-in rule set |
| **HTTP server** | `pkg/cns-health/server/`        | Dashboard + JSON API + Prometheus `/metrics` |
| **Entry point** | `cmd/cns-health/main.go`        | Wires everything; `/healthz`, `/readyz` |

## Health channels

The agent tracks five independent health states. Drop any one and the
agent loses an entire failure mode:

| Channel | What it tells you |
|---|---|
| `Controller`           | `vsphere-csi-controller` pod / container health from K8s API |
| `Syncer`               | `vsphere-syncer` container health within the controller pod |
| `Node`                 | Aggregate health of the `vsphere-csi-node` DaemonSet pods |
| `CNSReachability`      | Live scrape of `:2112`/`:2113` succeeds (driver is alive *and* answering) |
| `SyntheticCanary`      | Periodic end-to-end create/attach/detach/delete (Phase 2) |

## Built-in rules

| Rule ID | Trigger |
|---|---|
| `controller-down`        | All controller pod replicas Not Ready |
| `syncer-down`            | Syncer container Not Ready |
| `node-pods-degraded`     | Some node-DaemonSet pods Not Ready |
| `pending-pvcs`           | Any PVC stuck Pending; severity scales with count |
| `inaccessible-volumes`   | `vsphere_volume_health_gauge{volume_health_type="inaccessible-volumes"} > 0` |
| `high-error-rate`        | CSI op error rate ≥ 5 % (warning) / ≥ 15 % (critical) |
| `high-latency`           | Any op p95 ≥ 15 s (warning) / ≥ 30 s (critical) |
| `driver-image-unknown`   | No `vsphere-csi-controller` pod observed (likely RBAC misconfig) |

Rules live in `pkg/cns-health/rules/rules.go`. To add a new rule, implement
the `Rule` interface and append it inside `BuiltinRules()`.

## Build the container image

```bash
cd vsphere-csi-driver
docker build \
  -f images/cns-health/Dockerfile \
  -t <your-registry>/cns-health:latest \
  .
docker push <your-registry>/cns-health:latest
```

The default base image is `gcr.io/distroless/static:nonroot` (~2 MB).

## Deploy

The full set of manifests for cns-health is appended to the standard
vanilla deployment file at `manifests/vanilla/vsphere-csi-driver.yaml`.
If you already have the CSI driver installed, simply re-apply that file
to add the cns-health resources:

```bash
# Update image reference if you pushed your own build
kubectl apply -f manifests/vanilla/vsphere-csi-driver.yaml
```

This creates:

- `ServiceAccount/cns-health` in `vmware-system-csi`
- `ClusterRole/cns-health` (read-only by default)
- `ClusterRoleBinding/cns-health`
- `Service/cns-health` (NodePort `30808` for the dashboard, `2114` for metrics)
- `Deployment/cns-health` (1 replica, ~128 MiB)

Verify:

```bash
kubectl -n vmware-system-csi get deploy cns-health
kubectl -n vmware-system-csi get pods -l app=cns-health
kubectl -n vmware-system-csi logs deploy/cns-health
```

## Open the dashboard

Any of these work — pick whichever fits your environment:

```bash
# 1. Direct NodePort access (works on any cluster)
kubectl get nodes -o wide          # pick any node's external IP
open http://<NODE_IP>:30808

# 2. kubectl port-forward (works through any kubeconfig)
kubectl -n vmware-system-csi port-forward svc/cns-health 8080:8080
open http://localhost:8080

# 3. Proxy through kube-apiserver
kubectl proxy &
open http://localhost:8001/api/v1/namespaces/vmware-system-csi/services/cns-health:dashboard/proxy/
```

The dashboard auto-refreshes every 30 seconds and shows:

- A 5-pill health header (Controller / Syncer / Node DS / Metrics scrape / Synthetic canary)
- A 5-card KPI strip (Total PVs, PVCs Bound / Pending / Lost, attachments)
- The live **Findings** table — what the rules engine concluded, with
  severity, evidence, suggested action, first/last seen
- CSI driver details (name, version, namespace, container images, replica counts)
- Live metrics-scrape table (which endpoints are reachable)
- A footer with links to the JSON APIs: `/api/state`, `/api/findings`,
  `/api/metrics`, and Prometheus `/metrics`

## Endpoints

| Path | Type | Purpose |
|---|---|---|
| `/`              | HTML | Auto-refreshing dashboard |
| `/api/state`     | JSON | Driver info + health snapshot + inventory + agent stats |
| `/api/findings`  | JSON | Current findings list |
| `/api/metrics`   | JSON | Per-endpoint scraped metric summaries |
| `/metrics`       | Prometheus | Agent's own telemetry (scrape this from your cluster Prometheus) |
| `/healthz`       | HTTP | Liveness — always 200 if process is alive |
| `/readyz`        | HTTP | Readiness — 200 once first reconcile has completed |

## Tunable flags

| Flag | Default | Meaning |
|---|---|---|
| `--listen-addr`         | `:8080`                          | HTTP listen address |
| `--driver-namespace`    | `$CSI_NAMESPACE` or `vmware-system-csi` | Namespace of the CSI driver pods |
| `--csi-driver-name`     | `csi.vsphere.vmware.com`         | Driver name (set differently to monitor a foreign driver) |
| `--metrics-endpoints`   | derived from `--driver-namespace` | Comma-separated `http://host:port/metrics` URLs |
| `--reconcile-every`     | `30s`                            | Interval of the observe-reason-act loop |
| `--metric-scrape-every` | `60s`                            | Interval of the metrics-endpoint scrape |
| `--kubeconfig`          | (in-cluster)                     | Out-of-cluster kubeconfig path |
| `--v`                   | `0`                              | klog verbosity |

## Run locally (out-of-cluster) for development

```bash
cd vsphere-csi-driver
go run ./cmd/cns-health \
  --kubeconfig=$HOME/.kube/config \
  --listen-addr=:8080 \
  --metrics-endpoints=http://localhost:2112/metrics,http://localhost:2113/metrics \
  --v=4
```

(`kubectl -n vmware-system-csi port-forward deploy/vsphere-csi-controller 2112 2113`
exposes the in-cluster CSI metrics endpoints to your local machine.)

## Roadmap

**Phase 1 (this PR)** — read-only observability and dashboard. Status: ✅.

**Phase 2** — agentic action vocabulary v1: emit Kubernetes Events, write
annotations on affected PV/PVC/VolumeAttachment, run the synthetic canary
end-to-end on a dedicated StorageClass. Status: design.

**Phase 3** — CNS API integration via the existing vsphere-config-secret
to enrich findings with backend volume-health detail, and a curated
self-service remediation vocabulary (force-detach, retry, mark-for-cleanup)
guarded by per-action policies. Status: design.

**Phase 4** — optional LLM advisor for novel findings (air-gap-friendly,
default off). Status: research.
