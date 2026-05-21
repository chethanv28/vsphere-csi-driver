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

// Package observe holds the cns-health agent's observation sources. The
// agent's reasoning depends on multiple independent observers feeding a
// single world model — when one observer fails (network partition,
// missing RBAC) the others must remain useful.
package observe

import (
	"context"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	storagelisters "k8s.io/client-go/listers/storage/v1"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/cns-health/state"
)

// K8sObserver watches Kubernetes objects relevant to CSI volume operations
// and feeds the world-model state.Store.
//
// It is intentionally bounded to a small set of resources (PV, PVC,
// VolumeAttachment, Pods in the driver namespace) to keep memory usage
// predictable on clusters with 120k+ volumes.
type K8sObserver struct {
	client            kubernetes.Interface
	driverNamespace   string // e.g. "vmware-system-csi"
	csiDriverName     string // e.g. "csi.vsphere.vmware.com"
	store             *state.Store
	informerFactory   informers.SharedInformerFactory
	nsInformerFactory informers.SharedInformerFactory

	pvLister  corelisters.PersistentVolumeLister
	pvcLister corelisters.PersistentVolumeClaimLister
	vaLister  storagelisters.VolumeAttachmentLister
	podLister corelisters.PodLister
}

// NewK8sObserver constructs a K8sObserver. driverNamespace scopes the
// pod informer; PV/PVC/VA informers are cluster-scoped.
func NewK8sObserver(
	client kubernetes.Interface,
	driverNamespace, csiDriverName string,
	store *state.Store,
) *K8sObserver {
	clusterFactory := informers.NewSharedInformerFactory(client, 30*time.Second)
	nsFactory := informers.NewSharedInformerFactoryWithOptions(client, 30*time.Second,
		informers.WithNamespace(driverNamespace))

	return &K8sObserver{
		client:            client,
		driverNamespace:   driverNamespace,
		csiDriverName:     csiDriverName,
		store:             store,
		informerFactory:   clusterFactory,
		nsInformerFactory: nsFactory,
		pvLister:          clusterFactory.Core().V1().PersistentVolumes().Lister(),
		pvcLister:         clusterFactory.Core().V1().PersistentVolumeClaims().Lister(),
		vaLister:          clusterFactory.Storage().V1().VolumeAttachments().Lister(),
		podLister:         nsFactory.Core().V1().Pods().Lister(),
	}
}

// Start launches informer goroutines and blocks until caches have synced
// or ctx is cancelled.
func (o *K8sObserver) Start(ctx context.Context) error {
	// Trigger informer goroutines.
	_ = o.informerFactory.Core().V1().PersistentVolumes().Informer()
	_ = o.informerFactory.Core().V1().PersistentVolumeClaims().Informer()
	_ = o.informerFactory.Storage().V1().VolumeAttachments().Informer()
	_ = o.nsInformerFactory.Core().V1().Pods().Informer()

	o.informerFactory.Start(ctx.Done())
	o.nsInformerFactory.Start(ctx.Done())

	synced := []bool{}
	for _, s := range o.informerFactory.WaitForCacheSync(ctx.Done()) {
		synced = append(synced, s)
	}
	for _, s := range o.nsInformerFactory.WaitForCacheSync(ctx.Done()) {
		synced = append(synced, s)
	}
	for _, ok := range synced {
		if !ok {
			return fmt.Errorf("informer cache failed to sync")
		}
	}
	return nil
}

// Reconcile is called periodically by the agent. It rebuilds the
// state.VolumeInventory and the pod-derived parts of state.HealthSnapshot.
func (o *K8sObserver) Reconcile(ctx context.Context) error {
	// ---- inventory --------------------------------------------------------
	pvs, err := o.pvLister.List(labels.Everything())
	if err != nil {
		return fmt.Errorf("list PVs: %w", err)
	}
	pvcs, err := o.pvcLister.List(labels.Everything())
	if err != nil {
		return fmt.Errorf("list PVCs: %w", err)
	}
	vas, err := o.vaLister.List(labels.Everything())
	if err != nil {
		return fmt.Errorf("list VolumeAttachments: %w", err)
	}

	inv := state.VolumeInventory{
		TotalPVs:          len(pvs),
		TotalPVCs:         len(pvcs),
		VolumeAttachments: len(vas),
	}
	var oldest *corev1.PersistentVolumeClaim
	now := time.Now().UTC()
	for _, p := range pvcs {
		switch p.Status.Phase {
		case corev1.ClaimBound:
			inv.PVCsBound++
		case corev1.ClaimPending:
			inv.PVCsPending++
			if oldest == nil || p.CreationTimestamp.Before(&oldest.CreationTimestamp) {
				oldest = p
			}
		case corev1.ClaimLost:
			inv.PVCsLost++
		}
	}
	for _, va := range vas {
		if va.Status.Attached {
			inv.AttachmentsAttached++
		}
	}
	if oldest != nil {
		inv.OldestPendingPVC = fmt.Sprintf("%s/%s", oldest.Namespace, oldest.Name)
		inv.OldestPendingAge = now.Sub(oldest.CreationTimestamp.UTC()).Round(time.Second).String()
	}
	o.store.SetInventory(inv)

	// ---- driver pod health ------------------------------------------------
	pods, err := o.podLister.List(labels.Everything())
	if err != nil {
		return fmt.Errorf("list driver pods: %w", err)
	}

	driverInfo := state.CSIDriverInfo{
		Name:      o.csiDriverName,
		Namespace: o.driverNamespace,
	}

	ctrlHealth := state.HealthUnknown
	syncerHealth := state.HealthUnknown
	nodeHealthy := 0
	nodeTotal := 0

	for _, p := range pods {
		role := classifyDriverPod(p)
		ready := isPodReady(p)
		switch role {
		case "controller":
			driverInfo.ControllerReplicas++
			driverInfo.ControllerImage = mainImage(p, "vsphere-csi-controller", driverInfo.ControllerImage)
			driverInfo.SyncerImage = mainImage(p, "vsphere-syncer", driverInfo.SyncerImage)
			driverInfo.Version = inferVersion(driverInfo.ControllerImage, driverInfo.Version)
			ctrlHealth = mergeHealth(ctrlHealth, podHealth(p, "vsphere-csi-controller", ready))
			syncerHealth = mergeHealth(syncerHealth, podHealth(p, "vsphere-syncer", ready))
		case "node":
			driverInfo.NodeReplicas++
			driverInfo.NodeImage = mainImage(p, "vsphere-csi-node", driverInfo.NodeImage)
			nodeTotal++
			if ready {
				nodeHealthy++
			}
		}
	}

	o.store.SetDriver(driverInfo)

	current := o.store.Health()
	current.Controller = ctrlHealth
	current.Syncer = syncerHealth
	current.Node = aggregateNodeHealth(nodeHealthy, nodeTotal)
	current.NodePodsHealthy = nodeHealthy
	current.NodePodsTotal = nodeTotal
	o.store.SetHealth(current)

	return nil
}

// classifyDriverPod returns "controller", "node", or "" depending on pod labels.
func classifyDriverPod(p *corev1.Pod) string {
	if p == nil {
		return ""
	}
	switch p.Labels["app"] {
	case "vsphere-csi-controller":
		return "controller"
	case "vsphere-csi-node", "vsphere-csi-node-windows":
		return "node"
	}
	return ""
}

func isPodReady(p *corev1.Pod) bool {
	if p == nil {
		return false
	}
	if p.Status.Phase != corev1.PodRunning {
		return false
	}
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// podHealth derives a Health value from a specific container within the pod,
// based on restart count and ready state.
func podHealth(p *corev1.Pod, containerName string, podReady bool) state.Health {
	if p == nil {
		return state.HealthUnknown
	}
	for _, c := range p.Status.ContainerStatuses {
		if c.Name != containerName {
			continue
		}
		if c.State.Waiting != nil && strings.Contains(c.State.Waiting.Reason, "BackOff") {
			return state.HealthDown
		}
		if !c.Ready {
			return state.HealthDegraded
		}
		if c.RestartCount > 5 {
			return state.HealthDegraded
		}
	}
	if podReady {
		return state.HealthHealthy
	}
	return state.HealthDegraded
}

// aggregateNodeHealth turns a healthy/total count into a coarse status.
func aggregateNodeHealth(healthy, total int) state.Health {
	if total == 0 {
		return state.HealthUnknown
	}
	if healthy == total {
		return state.HealthHealthy
	}
	if healthy == 0 {
		return state.HealthDown
	}
	if healthy >= (total*2)/3 {
		return state.HealthDegraded
	}
	return state.HealthDown
}

// mergeHealth keeps the worse of two Health values across pod replicas.
func mergeHealth(a, b state.Health) state.Health {
	rank := func(h state.Health) int {
		switch h {
		case state.HealthDown:
			return 4
		case state.HealthDegraded:
			return 3
		case state.HealthHealthy:
			return 2
		default:
			return 1
		}
	}
	if rank(a) >= rank(b) {
		return a
	}
	return b
}

// mainImage returns the image of the named container, preferring the first
// non-empty value seen across pod replicas.
func mainImage(p *corev1.Pod, name, existing string) string {
	if existing != "" {
		return existing
	}
	for _, c := range p.Spec.Containers {
		if c.Name == name {
			return c.Image
		}
	}
	return existing
}

// inferVersion extracts a vN.N.N tag from an image reference if present.
func inferVersion(image, existing string) string {
	if existing != "" {
		return existing
	}
	if image == "" {
		return ""
	}
	if i := strings.LastIndex(image, ":"); i >= 0 && i < len(image)-1 {
		return image[i+1:]
	}
	return ""
}

// silence unused linter on storagev1 import for future Node use.
var _ = storagev1.VolumeAttachment{}
