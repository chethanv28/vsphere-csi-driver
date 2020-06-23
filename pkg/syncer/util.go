package syncer

import (
	"context"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"

	cnstypes "github.com/vmware/govmomi/cns/types"
	volumes "sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/volume"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/logger"
	csitypes "sigs.k8s.io/vsphere-csi-driver/pkg/csi/types"
)

// getPVsInBoundAvailableOrReleased return PVs in Bound, Available or Released state
func getPVsInBoundAvailableOrReleased(ctx context.Context, metadataSyncer *metadataSyncInformer) ([]*v1.PersistentVolume, error) {
	log := logger.GetLogger(ctx)
	var pvsInDesiredState []*v1.PersistentVolume
	log.Debugf("FullSync: Getting all PVs in Bound, Available or Released state")
	// Get all PVs from kubernetes
	allPVs, err := metadataSyncer.pvLister.List(labels.Everything())
	if err != nil {
		return nil, err
	}
	for _, pv := range allPVs {
		annotations := make(map[string]string)
		annotations = pv.GetAnnotations()
		if (pv.Spec.CSI != nil && pv.Spec.CSI.Driver == csitypes.Name) || (annotations["pv.kubernetes.io/provisioned-by"] == vSphereCSIBlockDriverName && pv.Spec.VsphereVolume != nil) || (annotations["pv.kubernetes.io/provisioned-by"] == inTreePluginName && annotations["pv.kubernetes.io/migrated-to"] == vSphereCSIBlockDriverName) {
			log.Debugf("FullSync: pv %v is in state %v", pv, pv.Status.Phase)
			if pv.Status.Phase == v1.VolumeBound || pv.Status.Phase == v1.VolumeAvailable || pv.Status.Phase == v1.VolumeReleased {
				pvsInDesiredState = append(pvsInDesiredState, pv)
			}
		}
	}
	return pvsInDesiredState, nil
}

// IsValidVolume determines if the given volume mounted by a POD is a valid vsphere volume. Returns the pv and pvc object if true.
func IsValidVolume(ctx context.Context, volume v1.Volume, pod *v1.Pod, metadataSyncer *metadataSyncInformer) (bool, *v1.PersistentVolume, *v1.PersistentVolumeClaim) {
	log := logger.GetLogger(ctx)
	pvcName := volume.PersistentVolumeClaim.ClaimName
	// Get pvc attached to pod
	pvc, err := metadataSyncer.pvcLister.PersistentVolumeClaims(pod.Namespace).Get(pvcName)
	if err != nil {
		log.Errorf("Error getting Persistent Volume Claim for volume %s with err: %v", volume.Name, err)
		return false, nil, nil
	}

	// Get pv object attached to pvc
	pv, err := metadataSyncer.pvLister.Get(pvc.Spec.VolumeName)
	if err != nil {
		log.Errorf("Error getting Persistent Volume for PVC %s in volume %s with err: %v", pvc.Name, volume.Name, err)
		return false, nil, nil
	}

	// Verify if pv is vsphere csi volume
	if pv.Spec.CSI == nil || pv.Spec.CSI.Driver != csitypes.Name {
		return false, nil, nil
	}
	return true, pv, pvc
}

// GetVolumeID determines if the given volume is either provisioned by in tree storage plugin or a volume provisioned by CSI driver using in-tree storage class.
// For such volumes the method returns volume handle if successfully found by the volumeMigrationService, else returns volume handle from CSI spec for CSI volumes
func GetVolumeID(ctx context.Context, pv *v1.PersistentVolume) (string, error) {
	log := logger.GetLogger(ctx)
	annotations := make(map[string]string)
	var err error
	var volumeHandle string
	annotations = pv.GetAnnotations()
	// Check if the pv is provisioned by CSI driver
	if annotations["pv.kubernetes.io/provisioned-by"] == vSphereCSIBlockDriverName {
		if pv.Spec.VsphereVolume == nil {
			volumeHandle = pv.Spec.CSI.VolumeHandle
		}
	}
	// Check if the volume is provisioned by in-tree plugin and has migrated-to CSI annotation OR if the volume is provisioned by CSI driver using in-tree storage class
	if (annotations["pv.kubernetes.io/provisioned-by"] == vSphereCSIBlockDriverName && pv.Spec.VsphereVolume != nil) || (annotations["pv.kubernetes.io/provisioned-by"] == inTreePluginName && annotations["pv.kubernetes.io/migrated-to"] == vSphereCSIBlockDriverName) {
		volumeHandle, err = volumeMigrationService.GetVolumeID(ctx, pv.Spec.VsphereVolume.VolumePath)
		if err != nil {
			log.Errorf("failed to get VolumeID from volumeMigrationService for volumePath: %q", pv.Spec.VsphereVolume.VolumePath)
			return "", err
		}
	}
	return volumeHandle, nil
}

// getQueryResults returns list of CnsQueryResult retrieved using
// queryFilter with offset and limit to query volumes using pagination
// if volumeIds is empty, then all volumes from CNS will be retrieved by pagination
func getQueryResults(ctx context.Context, volumeIds []cnstypes.CnsVolumeId, clusterID string, volumeManager volumes.Manager) ([]*cnstypes.CnsQueryResult, error) {
	log := logger.GetLogger(ctx)
	queryFilter := cnstypes.CnsQueryFilter{
		VolumeIds: volumeIds,
		Cursor: &cnstypes.CnsCursor{
			Offset: 0,
			Limit:  queryVolumeLimit,
		},
	}
	if clusterID != "" {
		queryFilter.ContainerClusterIds = []string{clusterID}
	}
	var allQueryResults []*cnstypes.CnsQueryResult
	for {
		log.Debugf("Query volumes with offset: %v and limit: %v", queryFilter.Cursor.Offset, queryFilter.Cursor.Limit)
		queryResult, err := volumeManager.QueryVolume(ctx, queryFilter)
		if err != nil {
			log.Errorf("failed to QueryVolume using filter: %+v", queryFilter)
			return nil, err
		}
		if queryResult == nil {
			log.Info("Observed empty queryResult")
			break
		}
		allQueryResults = append(allQueryResults, queryResult)
		log.Infof("%v more volumes to be queried", queryResult.Cursor.TotalRecords-queryResult.Cursor.Offset)
		if queryResult.Cursor.Offset == queryResult.Cursor.TotalRecords {
			log.Info("Metadata retrieved for all requested volumes")
			break
		}
		queryFilter.Cursor = &queryResult.Cursor
	}
	return allQueryResults, nil
}
