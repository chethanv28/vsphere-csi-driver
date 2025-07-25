/*
Copyright 2019 The Kubernetes Authors.

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

package e2e

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/find"
	"github.com/vmware/govmomi/object"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	_ "k8s.io/client-go/plugin/pkg/client/auth/oidc"
	restclient "k8s.io/client-go/rest"
	"k8s.io/kubernetes/test/e2e/framework"
	fdep "k8s.io/kubernetes/test/e2e/framework/deployment"
	e2ekubectl "k8s.io/kubernetes/test/e2e/framework/kubectl"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2eoutput "k8s.io/kubernetes/test/e2e/framework/pod/output"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	admissionapi "k8s.io/pod-security-admission/api"

	cnsregistervolumev1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsregistervolume/v1alpha1"
)

var _ = ginkgo.Describe("Volume Expansion Test", func() {

	f := framework.NewDefaultFramework("volume-expansion")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	framework.TestContext.DeleteNamespace = true
	var (
		client                     clientset.Interface
		namespace                  string
		storagePolicyName          string
		profileID                  string
		pandoraSyncWaitTime        int
		defaultDatastore           *object.Datastore
		isVsanHealthServiceStopped bool
		isSPSServiceStopped        bool
		fsType                     string
	)

	ginkgo.BeforeEach(func() {
		client = f.ClientSet
		namespace = getNamespaceToRunTests(f)
		bootstrap()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var err error

		isVsanHealthServiceStopped = false
		isSPSServiceStopped = false

		if !stretchedSVC {
			storagePolicyName = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)
			profileID = e2eVSphere.GetSpbmPolicyID(storagePolicyName)
			defaultDatastore = getDefaultDatastore(ctx)
		}

		storagePolicyName = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)
		profileID = e2eVSphere.GetSpbmPolicyID(storagePolicyName)

		nodeList, err := fnodes.GetReadySchedulableNodes(ctx, f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}

		if os.Getenv(envPandoraSyncWaitTime) != "" {
			pandoraSyncWaitTime, err = strconv.Atoi(os.Getenv(envPandoraSyncWaitTime))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			pandoraSyncWaitTime = defaultPandoraSyncWaitTime
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defaultDatastore = getDefaultDatastore(ctx)

		if guestCluster {
			svcClient, svNamespace := getSvcClientAndNamespace()
			setResourceQuota(svcClient, svNamespace, rqLimit)
		}
		if windowsEnv {
			fsType = ntfsFSType
		} else {
			fsType = ext4FSType
		}
	})

	ginkgo.AfterEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		if isSPSServiceStopped {
			framework.Logf("Bringing sps up before terminating the test")
			startVCServiceWait4VPs(ctx, vcAddress, spsServiceName, &isSPSServiceStopped)
		}

		if isVsanHealthServiceStopped {
			framework.Logf("Bringing vsanhealth up before terminating the test")
			startVCServiceWait4VPs(ctx, vcAddress, vsanhealthServiceName, &isVsanHealthServiceStopped)
		}

		if supervisorCluster {
			dumpSvcNsEventsOnTestFailure(client, namespace)
		}
		if guestCluster {
			svcClient, svNamespace := getSvcClientAndNamespace()
			setResourceQuota(svcClient, svNamespace, rqLimit)
			dumpSvcNsEventsOnTestFailure(svcClient, svNamespace)
		}

	})

	// Test to verify volume expansion is supported if allowVolumeExpansion
	// is true in StorageClass, PVC is created and offline and not attached
	// to a Pod before the expansion.

	// Steps
	// 1. Create StorageClass with allowVolumeExpansion set to true.
	// 2. Create PVC which uses the StorageClass created in step 1.
	// 3. Wait for PV to be provisioned.
	// 4. Wait for PVC's status to become Bound.
	// 5. Modify PVC's size to trigger offline volume expansion.
	// 6. Create pod using PVC on specific node.
	// 7. Wait for Disk to be attached to the node.
	// 8. Wait for file system resize to complete.
	// 9. Delete pod and Wait for Volume Disk to be detached from the Node.
	// 10. Delete PVC, PV and Storage Class.

	ginkgo.It("[csi-block-vanilla] [csi-supervisor] [csi-guest] [csi-block-vanilla-parallelized] [csi-vcp-mig] Verify "+
		"volume expansion with no filesystem before expansion", ginkgo.Label(p0,
		block, vanilla, wcp, tkg, core, vc70), func() {
		invokeTestForVolumeExpansion(f, client, namespace, "", storagePolicyName, profileID)
	})

	// Test to verify volume expansion is supported if allowVolumeExpansion
	// is true in StorageClass, PVC is created, attached to a Pod, detached,
	// expanded, and attached to a Pod again to finish the filesystem resize.

	// Steps
	// 1. Create StorageClass with allowVolumeExpansion set to true.
	// 2. Create PVC which uses the StorageClass created in step 1.
	// 3. Wait for PV to be provisioned.
	// 4. Wait for PVC's status to become Bound.
	// 5. Create pod using PVC on specific node.
	// 6. Wait for Disk to be attached to the node.
	// 7. Detach the volume.
	// 8. Modify PVC's size to trigger offline volume expansion.
	// 9. Create pod again using PVC on specific node.
	// 10. Wait for Disk to be attached to the node.
	// 11. Wait for file system resize to complete.
	// 12. Delete pod and Wait for Volume Disk to be detached from the Node.
	// 13. Delete PVC, PV and Storage Class.

	ginkgo.It("[csi-block-vanilla] [csi-guest] [csi-block-vanilla-parallelized] [csi-vcp-mig] Verify volume expansion "+
		"with initial filesystem before expansion", ginkgo.Label(p0, block, vanilla, tkg, core, vc70), func() {
		invokeTestForVolumeExpansionWithFilesystem(f, client, namespace, fsType, "", storagePolicyName, profileID)
	})

	// Test to verify offline volume expansion workflow with xfs filesystem.

	// Steps
	// 1. Create StorageClass with fstype set to xfs and allowVolumeExpansion set to true.
	// 2. Create PVC which uses the StorageClass created in step 1.
	// 3. Wait for PV to be provisioned.
	// 4. Wait for PVC's status to become Bound.
	// 5. Create pod using PVC on specific node.
	// 6. Wait for Disk to be attached to the node.
	// 7. Detach the volume.
	// 8. Modify PVC's size to trigger offline volume expansion.
	// 9. Create pod again using PVC on specific node.
	// 10. Wait for Disk to be attached to the node.
	// 11. Wait for file system resize to complete.
	// 12. Delete pod and Wait for Volume Disk to be detached from the Node.
	// 13. Delete PVC, PV and Storage Class.

	ginkgo.It("[csi-block-vanilla] [csi-guest] [csi-block-vanilla-parallelized] Verify offline volume expansion workflow "+
		"with xfs filesystem", ginkgo.Label(p0, block, vanilla, tkg, core, vc70), func() {
		invokeTestForVolumeExpansionWithFilesystem(f, client, namespace, xfsFSType, xfsFSType, storagePolicyName, profileID)
	})

	// Test to verify volume expansion is not supported if allowVolumeExpansion
	// is false in StorageClass.

	// Steps
	// 1. Create StorageClass with allowVolumeExpansion set to false.
	// 2. Create PVC which uses the StorageClass created in step 1.
	// 3. Wait for PV to be provisioned.
	// 4. Wait for PVC's status to become Bound.
	// 5. Modify PVC's size to trigger offline volume expansion.
	// 6. Verify if the PVC expansion fails.

	ginkgo.It("[csi-block-vanilla] [csi-guest]  [csi-block-vanilla-parallelized] [csi-vcp-mig] Verify volume "+
		"expansion not allowed", ginkgo.Label(p2, block, vanilla, tkg, core, vc70), func() {
		invokeTestForInvalidVolumeExpansion(f, client, namespace, storagePolicyName, profileID)
	})

	// Test to verify volume expansion is not supported if new size is
	// smaller than the current size.

	// Steps
	// 1. Create StorageClass with allowVolumeExpansion set to true.
	// 2. Create PVC which uses the StorageClass created in step 1.
	// 3. Wait for PV to be provisioned.
	// 4. Wait for PVC's status to become Bound.
	// 5. Modify PVC's size to a smaller size.
	// 6. Verify if the PVC expansion fails.

	ginkgo.It("[csi-block-vanilla] [csi-guest] [csi-supervisor]  [csi-block-vanilla-parallelized] [csi-vcp-mig] Verify "+
		"volume shrinking not allowed", ginkgo.Label(p1, block, vanilla, wcp, tkg, core, vc70), func() {
		invokeTestForInvalidVolumeShrink(f, client, namespace, storagePolicyName, profileID)
	})

	// Test to verify volume expansion is not support for static provisioning.

	// Steps
	// 1. Create FCD and wait for fcd to allow syncing with pandora.
	// 2. Create PV Spec with volumeID set to FCDID created in Step-1, and
	//    PersistentVolumeReclaimPolicy is set to Delete.
	// 3. Create PVC with the storage request set to PV's storage capacity.
	// 4. Wait for PV and PVC to bound.
	// 5. Modify size of PVC to trigger volume expansion.
	// 6. It should fail because volume expansion is not supported.
	// 7. Delete PVC.
	// 8. Verify PV is deleted automatically.

	ginkgo.It("[csi-block-vanilla] [csi-block-vanilla-parallelized] Verify volume expansion is not supported for PVC "+
		"using vSAN-Default-Storage-Policy", ginkgo.Label(p0, block, vanilla, core, vc70), func() {
		invokeTestForInvalidVolumeExpansionStaticProvision(f, client, namespace, storagePolicyName, profileID)
	})

	// Test to verify volume expansion can happen multiple times

	// Steps
	// 1. Create StorageClass with allowVolumeExpansion set to true.
	// 2. Create PVC which uses the StorageClass created in step 1.
	// 3. Wait for PV to be provisioned.
	// 4. Wait for PVC's status to become Bound.
	// 5. In a loop of 10, modify PVC's size by adding 1 Gb at a time
	//    to trigger offline volume expansion.
	// 6. Create pod using PVC on specific node.
	// 7. Wait for Disk to be attached to the node.
	// 8. Wait for file system resize to complete.
	// 9. Delete pod and Wait for Volume Disk to be detached from the Node.
	// 10. Delete PVC, PV and Storage Class.

	ginkgo.It("[csi-block-vanilla] [csi-guest] [csi-supervisor] [csi-block-vanilla-parallelized] [csi-vcp-mig] Verify "+
		"volume expansion can happen multiple times", ginkgo.Label(p1, block, vanilla, wcp, core, vc70), func() {
		invokeTestForExpandVolumeMultipleTimes(f, client, namespace, "", storagePolicyName, profileID)
	})

	// Test to verify volume expansion is not supported for file volume

	// Steps
	// 1. Create StorageClass with allowVolumeExpansion set to true.
	// 2. Create File Volume PVC which uses the StorageClass created in step 1.
	// 3. Wait for PV to be provisioned.
	// 4. Wait for PVC's status to become Bound.
	// 5. Modify PVC's size to a bigger size.
	// 6. Verify if the PVC expansion fails.
	ginkgo.It("[csi-file-vanilla] Verify file volume expansion is not "+
		"supported", ginkgo.Label(p1, file, vanilla, core, vc70), func() {
		invokeTestForUnsupportedFileVolumeExpansion(f, client, namespace, storagePolicyName, profileID)
	})

	/*
		Verify online volume expansion on dynamic volume

		1. Create StorageClass with allowVolumeExpansion set to true.
		2. Create PVC which uses the StorageClass created in step 1.
		3. Wait for PV to be provisioned.
		4. Wait for PVC's status to become Bound and note down the size
		5. Create a Pod using the above created PVC
		6. Modify PVC's size to trigger online volume expansion
		7. verify the PVC status will change to "FilesystemResizePending". Wait till the status is removed
		8. Verify the resized PVC by doing CNS query
		9. Make sure data is intact on the PV mounted on the pod
		10.  Make sure file system has increased

	*/
	ginkgo.It("[csi-block-vanilla] [csi-supervisor] [csi-block-vanilla-parallelized] [csi-vcp-mig] Verify online "+
		"volume expansion on dynamic volume", ginkgo.Label(p0, block, vanilla, wcp, core, vc70), func() {
		ginkgo.By("Invoking Test for Volume Expansion")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var pv *v1.PersistentVolume
		var volHandle string

		ginkgo.By("Create StorageClass with allowVolumeExpansion set to true, Create PVC")
		sharedVSANDatastoreURL := GetAndExpectStringEnvVar(envSharedDatastoreURL)
		volHandle, pvclaim, pv, storageclass = createSCwithVolumeExpansionTrueAndDynamicPVC(
			ctx, f, client, sharedVSANDatastoreURL, storagePolicyName, namespace, ext4FSType)

		defer func() {
			if !supervisorCluster {
				err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			if !vcptocsi {
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			} else {
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create Pod using the above PVC")
		pod, vmUUID := createPODandVerifyVolumeMount(ctx, f, client, namespace, pvclaim, volHandle, "")

		defer func() {
			// Delete Pod.
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
			err := fpod.DeletePodWithWait(ctx, client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			if supervisorCluster {
				ginkgo.By(fmt.Sprintf("Verify volume: %s is detached from PodVM with vmUUID: %s",
					pv.Spec.CSI.VolumeHandle, vmUUID))
				_, err := e2eVSphere.getVMByUUIDWithWait(ctx, vmUUID, supervisorClusterOperationsTimeout)
				gomega.Expect(err).To(gomega.HaveOccurred(),
					fmt.Sprintf("PodVM with vmUUID: %s still exists. So volume: %s is not detached from the PodVM",
						vmUUID, pv.Spec.CSI.VolumeHandle))
			} else {
				isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client,
					volHandle, pod.Spec.NodeName)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
					fmt.Sprintf("Volume %q is not detached from the node %q", volHandle, pod.Spec.NodeName))
			}
		}()

		ginkgo.By("Increase PVC size and verify online volume resize")
		increaseSizeOfPvcAttachedToPod(f, client, namespace, pvclaim, pod)
	})

	/*
		Test to verify online volume expansion workflow with xfs filesystem

		1. Create StorageClass with fstype set to xfs and allowVolumeExpansion set to true.
		2. Create PVC which uses the StorageClass created in step 1.
		3. Wait for PV to be provisioned.
		4. Wait for PVC's status to become Bound and note down the size
		5. Create a Pod using the above created PVC
		6. Modify PVC's size to trigger online volume expansion
		7. verify the PVC status will change to "FilesystemResizePending". Wait till the status is removed
		8. Verify the resized PVC by doing CNS query
		9. Make sure data is intact on the PV mounted on the pod
		10. Make sure file system has increased
	*/
	ginkgo.It("[csi-block-vanilla] [csi-block-vanilla-parallelized] [csi-vcp-mig] Verify online volume expansion "+
		"workflow with xfs filesystem", ginkgo.Label(p1, block, vanilla, core, vc70), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var pv *v1.PersistentVolume
		var volHandle string

		ginkgo.By("Create StorageClass with fstype set to xfs and allowVolumeExpansion set to true, Create PVC")
		sharedVSANDatastoreURL := GetAndExpectStringEnvVar(envSharedDatastoreURL)
		volHandle, pvclaim, pv, storageclass = createSCwithVolumeExpansionTrueAndDynamicPVC(
			ctx, f, client, sharedVSANDatastoreURL, storagePolicyName, namespace, xfsFSType)

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create Pod using the above PVC")
		pod, _ := createPODandVerifyVolumeMount(ctx, f, client, namespace, pvclaim, volHandle, xfsFSType)

		defer func() {
			// Delete Pod.
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
			err := fpod.DeletePodWithWait(ctx, client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By(fmt.Sprintf("Waiting for pv %s to be deleted from CNS", pv.Name))
			isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client,
				volHandle, pod.Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
				fmt.Sprintf("Volume %q is not detached from the node %q", volHandle, pod.Spec.NodeName))
		}()

		ginkgo.By("Increase PVC size and verify online volume resize")
		increaseSizeOfPvcAttachedToPod(f, client, namespace, pvclaim, pod)
	})

	// Verify online volume expansion on static volume.
	//
	// 1. Create FCD and wait for fcd to allow syncing with pandora.
	// 2. Create PV Spec with volumeID set to FCDID created in Step-1,
	//    and PersistentVolumeReclaimPolicy is set to Delete.
	// 3. Create PVC with the storage request set to PV's storage capacity.
	// 4. Wait for PV and PVC to bound and note the PVC size.
	// 5. Create Pod using above created PVC.
	// 6. Modify size of PVC to trigger online volume expansion.
	// 7. Verify the PVC status will change to "FilesystemResizePending".
	//    Wait till the status is removed.
	// 8. Verify the resized PVC by doing CNS query.
	// 9. Make sure data is intact on the PV mounted on the pod.
	// 10. Make sure file system has increased.

	ginkgo.It("[csi-block-vanilla] [csi-block-vanilla-parallelized] Verify online volume "+
		"expansion on static volume", ginkgo.Label(p1, block, vanilla, core, vc70), func() {
		ginkgo.By("Invoking Test for Volume Expansion on statically created PVC ")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		volHandle, pvclaim, pv, storageclass := createStaticPVC(ctx, f, client,
			namespace, defaultDatastore, pandoraSyncWaitTime)

		defer func() {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create POD")
		pod, _ := createPODandVerifyVolumeMount(ctx, f, client, namespace, pvclaim, volHandle, "")

		defer func() {
			// Delete Pod.
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
			err := fpod.DeletePodWithWait(ctx, client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verify volume is detached from the node")
			isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client,
				pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
				fmt.Sprintf("Volume %q is not detached from the node %q", pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
		}()

		increaseSizeOfPvcAttachedToPod(f, client, namespace, pvclaim, pod)
	})

	/*
		Verify online volume expansion does not support shrinking volume

		1. Create StorageClass with allowVolumeExpansion set to true.
		2. Create PVC which uses the StorageClass created in step 1.
		3. Wait for PV to be provisioned.
		4. Wait for PVC's status to become Bound.
		5. Create POD
		6. Modify PVC to be a smaller size.
		7. Verify that the PVC size does not change because volume shrinking is not supported.
	*/
	ginkgo.It("[csi-block-vanilla] [csi-supervisor] [csi-guest] [csi-block-vanilla-parallelized][csi-vcp-mig] Verify "+
		"online volume expansion shrinking volume not "+
		"allowed", ginkgo.Label(p1, block, vanilla, wcp, tkg, core, vc70), func() {
		ginkgo.By("Invoking Test for Volume Expansion")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ginkgo.By("Create StorageClass with allowVolumeExpansion set to true, Create PVC")
		volHandle, pvclaim, pv, storageclass := createSCwithVolumeExpansionTrueAndDynamicPVC(
			ctx, f, client, "", storagePolicyName, namespace, fsType)

		defer func() {
			if !supervisorCluster {
				err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			if !vcptocsi {
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			} else {
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

		}()

		ginkgo.By("Create POD")
		pod, vmUUID := createPODandVerifyVolumeMount(ctx, f, client, namespace, pvclaim, volHandle, "")

		defer func() {
			// Delete POD
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
			err := fpod.DeletePodWithWait(ctx, client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			if supervisorCluster {
				ginkgo.By(fmt.Sprintf("Verify volume: %s is detached from PodVM with vmUUID: %s",
					pv.Spec.CSI.VolumeHandle, vmUUID))
				_, err := e2eVSphere.getVMByUUIDWithWait(ctx, vmUUID, supervisorClusterOperationsTimeout)
				gomega.Expect(err).To(gomega.HaveOccurred(),
					fmt.Sprintf("PodVM with vmUUID: %s still exists. So volume: %s is not detached from the PodVM",
						vmUUID, pv.Spec.CSI.VolumeHandle))
			} else {
				isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client,
					volHandle, pod.Spec.NodeName)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
					fmt.Sprintf("Volume %q is not detached from the node %q", volHandle, pod.Spec.NodeName))
			}
		}()

		// Modify PVC spec to a smaller size
		// Expect this to fail
		ginkgo.By("Verify operation will fail because volume shrinking is not supported")
		currentPvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
		newSize := currentPvcSize.DeepCopy()
		newSize.Sub(resource.MustParse("100Mi"))
		framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
		_, err := expandPVCSize(pvclaim, newSize, client)
		gomega.Expect(err).To(gomega.HaveOccurred())
	})

	/*
			Verify online volume expansion multiple times on the same PVC

			1. Create StorageClass with allowVolumeExpansion set to true.
			2. Create PVC which uses the StorageClass created in step 1.
			3. Wait for PV to be provisioned.
		    4. Wait for PVC's status to become Bound and note the PVC size
			5. Create Pod using the above created PVC
			6. In a loop of 10, modify PVC's size by adding 1 Gb at a time to trigger online volume expansion.
			7. Wait for file system resize to complete.
			8. Verify the PVC Size should increased by 10Gi
			9. Make sure file system has increased
	*/
	ginkgo.It("[csi-block-vanilla] [csi-supervisor] [csi-guest] [csi-block-vanilla-parallelized] [csi-vcp-mig] Verify "+
		"volume expansion multiple times on the same PVC", ginkgo.Label(p0, block, vanilla, wcp, tkg, core, vc70), func() {

		ginkgo.By("Invoking Test for Volume Expansion")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		volHandle, pvclaim, pv, storageclass := createSCwithVolumeExpansionTrueAndDynamicPVC(
			ctx, f, client, "", storagePolicyName, namespace, fsType)
		defer func() {
			if !supervisorCluster {
				err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			if !vcptocsi {
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			} else {
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create POD")
		pod, vmUUID := createPODandVerifyVolumeMount(ctx, f, client, namespace, pvclaim, volHandle, "")

		defer func() {
			// Delete POD
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
			err := fpod.DeletePodWithWait(ctx, client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			if supervisorCluster {
				ginkgo.By(fmt.Sprintf("Verify volume: %s is detached from PodVM with vmUUID: %s",
					pv.Spec.CSI.VolumeHandle, vmUUID))
				_, err := e2eVSphere.getVMByUUIDWithWait(ctx, vmUUID, supervisorClusterOperationsTimeout)
				gomega.Expect(err).To(gomega.HaveOccurred(),
					fmt.Sprintf("PodVM with vmUUID: %s still exists. So volume: %s is not detached from the PodVM",
						vmUUID, pv.Spec.CSI.VolumeHandle))
			} else {
				isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(
					client, volHandle, pod.Spec.NodeName)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
					fmt.Sprintf("Volume %q is not detached from the node %q", volHandle, pod.Spec.NodeName))
			}
		}()

		ginkgo.By("Increase PVC size and verify Volume resize")
		increaseOnlineVolumeMultipleTimes(ctx, f, client, namespace, volHandle, pvclaim, pod)
	})

	/*
		Verify online resize when VSAN health is down
		1. Create StorageClass with allowVolumeExpansion set to true
		2. Create PVC which uses the StorageClass created in step 1
		3. Wait for PVC's status to become Bound and note down the size
		4. Create a Pod using the above created PVC
		5. Bring vsan-health service down
		6. Modify PVC's size to trigger online volume expansion
		7. Verify that PVC has not reached "FilesystemResizePending" state even after waiting for a min
		8. Bring up vsan-health service
		9. Verify the resized PVC by doing CNS query
		10. Make sure data is intact on the PV mounted on the pod
		11. Make sure file system has increased
	*/

	ginkgo.It("[csi-block-vanilla] [csi-supervisor] [csi-guest] [csi-block-vanilla-serialized] [csi-vcp-mig] Verify "+
		"online volume expansion when VSAN-health is down", ginkgo.Label(p1, block, vanilla, wcp, tkg, core, vc70), func() {
		ginkgo.By("Invoking Test for Volume Expansion")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var originalSizeInMb, fsSize int64
		var err error
		var expectedErrMsg string

		volHandle, pvclaim, pv, storageclass := createSCwithVolumeExpansionTrueAndDynamicPVC(
			ctx, f, client, "", storagePolicyName, namespace, fsType)
		defer func() {
			if !supervisorCluster {
				err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			if !vcptocsi {
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			} else {
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

		}()

		ginkgo.By("Create POD")
		pod, vmUUID := createPODandVerifyVolumeMount(ctx, f, client, namespace, pvclaim, volHandle, "")

		// Fetch original FileSystemSize
		ginkgo.By("Verify filesystem size for mount point /mnt/volume1 before expansion")
		originalSizeInMb, err = getFileSystemSizeForOsType(f, client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			// Delete POD
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
			err := fpod.DeletePodWithWait(ctx, client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			if supervisorCluster {
				ginkgo.By(fmt.Sprintf("Verify volume: %s is detached from PodVM with vmUUID: %s",
					pv.Spec.CSI.VolumeHandle, vmUUID))
				_, err := e2eVSphere.getVMByUUIDWithWait(ctx, vmUUID, supervisorClusterOperationsTimeout)
				gomega.Expect(err).To(gomega.HaveOccurred(),
					fmt.Sprintf("PodVM with vmUUID: %s still exists. So volume: %s is not detached from the PodVM",
						vmUUID, pv.Spec.CSI.VolumeHandle))
			} else {
				isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(
					client, volHandle, pod.Spec.NodeName)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
					fmt.Sprintf("Volume %q is not detached from the node %q", volHandle, pod.Spec.NodeName))
			}
		}()

		ginkgo.By("Bring down Vsan-health service")
		isVsanHealthServiceStopped = true
		err = invokeVCenterServiceControl(ctx, stopOperation, vsanhealthServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			if isVsanHealthServiceStopped {
				framework.Logf("Bringing vsanhealth up before terminating the test")
				startVCServiceWait4VPs(ctx, vcAddress, vsanhealthServiceName, &isVsanHealthServiceStopped)
			}
		}()

		ginkgo.By("Expanding current pvc")
		currentPvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
		newSize := currentPvcSize.DeepCopy()
		newSize.Add(resource.MustParse("1Gi"))
		framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
		pvclaim, err = expandPVCSize(pvclaim, newSize, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvclaim).NotTo(gomega.BeNil())

		ginkgo.By("File system resize should not succeed Since Vsan-health is down. Expect an error")
		if guestCluster {
			expectedErrMsg = "not in FileSystemResizePending condition"
		} else {
			expectedErrMsg = "503 Service Unavailable"
		}
		framework.Logf("Expected failure message: %+q", expectedErrMsg)
		err = waitForEvent(ctx, client, namespace, expectedErrMsg, pvclaim.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Bringup vsanhealth service")
		startVCServiceWait4VPs(ctx, vcAddress, vsanhealthServiceName, &isVsanHealthServiceStopped)

		ginkgo.By("Waiting for file system resize to finish")
		pvclaim, err = waitForFSResize(pvclaim, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvcConditions := pvclaim.Status.Conditions
		expectEqual(len(pvcConditions), 0, "pvc should not have conditions")

		ginkgo.By("Verify filesystem size for mount point /mnt/volume1")
		fsSize, err = getFileSystemSizeForOsType(f, client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// Filesystem size may be smaller than the size of the block volume
		// so here we are checking if the new filesystem size is greater than
		// the original volume size as the filesystem is formatted for the
		// first time.
		gomega.Expect(fsSize).Should(gomega.BeNumerically(">", originalSizeInMb),
			fmt.Sprintf("error updating filesystem size for %q. Resulting filesystem size is %d", pvclaim.Name, fsSize))
		ginkgo.By("File system resize finished successfully")
	})

	/*
		Verify online resize when SPS  is down
		1. Create StorageClass with allowVolumeExpansion set to true
		2. Create PVC which uses the StorageClass created in step 1
		3. Wait for PVC's status to become Bound and note down the size
		4. Create a Pod using the above created PVC
		5. Bring SPS service down
		6. Modify PVC's size to trigger online volume expansion
		7. Verify that PVC has not reached "FilesystemResizePending" state even after waiting for a min
		8. Bring up SPSservice
		9. Verify the resized PVC by doing CNS query
		10. Make sure data is intact on the PV mounted on the pod
		11. Make sure file system has increased
	*/
	ginkgo.It("[csi-block-vanilla] [csi-supervisor] [csi-guest] [csi-block-vanilla-serialized] Verify online volume "+
		"expansion when SPS-Service is down", ginkgo.Label(p1, block, vanilla, wcp, tkg, core, vc70), func() {

		ginkgo.By("Invoking Test for Volume Expansion")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var originalSizeInMb, fsSize int64
		var err error

		/*
			Note: As per PR #2935677, even if cns_new_sync is enabled volume expansion
			will not work if sps-service is down.
			Keeping the below disabled line of code for reference. Here, cnsNewSyncFSS = "CNS_NEW_SYNC"
		*/
		// featureEnabled := isFssEnabled(vcAddress, cnsNewSyncFSS)

		volHandle, pvclaim, pv, storageclass := createSCwithVolumeExpansionTrueAndDynamicPVC(
			ctx, f, client, "", storagePolicyName, namespace, fsType)
		defer func() {
			if !supervisorCluster {
				err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create POD")
		pod, vmUUID := createPODandVerifyVolumeMount(ctx, f, client, namespace, pvclaim, volHandle, "")

		// Fetch original FileSystemSize
		ginkgo.By("Verify filesystem size for mount point /mnt/volume1 before expansion")
		originalSizeInMb, err = getFileSystemSizeForOsType(f, client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			// Delete POD
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
			err := fpod.DeletePodWithWait(ctx, client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			if supervisorCluster {
				ginkgo.By(fmt.Sprintf("Verify volume: %s is detached from PodVM with vmUUID: %s",
					pv.Spec.CSI.VolumeHandle, vmUUID))
				_, err := e2eVSphere.getVMByUUIDWithWait(ctx, vmUUID, supervisorClusterOperationsTimeout)
				gomega.Expect(err).To(gomega.HaveOccurred(),
					fmt.Sprintf("PodVM with vmUUID: %s still exists. So volume: %s is not detached from the PodVM",
						vmUUID, pv.Spec.CSI.VolumeHandle))
			} else {
				isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(
					client, pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
					fmt.Sprintf("Volume %q is not detached from the node %q", pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
			}
		}()

		ginkgo.By("Bring down SPS service")
		isSPSServiceStopped = true
		err = invokeVCenterServiceControl(ctx, stopOperation, spsServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = waitVCenterServiceToBeInState(ctx, spsServiceName, vcAddress, svcStoppedMessage)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			if isSPSServiceStopped {
				framework.Logf("Bringing sps up before terminating the test")
				startVCServiceWait4VPs(ctx, vcAddress, spsServiceName, &isSPSServiceStopped)
			}
		}()

		ginkgo.By("Expanding current pvc")
		currentPvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
		newSize := currentPvcSize.DeepCopy()
		newSize.Add(resource.MustParse("1Gi"))
		framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
		pvclaim, err = expandPVCSize(pvclaim, newSize, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvclaim).NotTo(gomega.BeNil())

		ginkgo.By("Waiting for file system resize to finish")
		pvclaim, err = waitForFSResize(pvclaim, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Bringup SPS service")
		startVCServiceWait4VPs(ctx, vcAddress, spsServiceName, &isSPSServiceStopped)

		pvcConditions := pvclaim.Status.Conditions
		expectEqual(len(pvcConditions), 0, "pvc should not have conditions")

		ginkgo.By("Verify filesystem size for mount point /mnt/volume1")
		fsSize, err = getFileSystemSizeForOsType(f, client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// Filesystem size may be smaller than the size of the block volume
		// so here we are checking if the new filesystem size is greater than
		// the original volume size as the filesystem is formatted for the
		// first time.
		gomega.Expect(fsSize).Should(gomega.BeNumerically(">", originalSizeInMb),
			fmt.Sprintf("error updating filesystem size for %q. Resulting filesystem size is %d", pvclaim.Name, fsSize))
		ginkgo.By("File system resize finished successfully")

	})

	/*
		Resize PVC concurrently with different size
		1. Create StorageClass with allowVolumeExpansion set to true.
		2. Create PVC which uses the StorageClass created in step 1.
		3. Wait for PV to be provisioned.
		4. Wait for PVC's status to become Bound and note down the size
		5. Create a Pod using the above created PVC
		6. Trigger online expansion on Same PVC multiple times, with 3Gi , 4Gi and 8Gi
		7. verify the PVC status will change to "FilesystemResizePending". Wait till the status is removed
		8. Verify the resized PVC by doing CNS query, size of the volume should be 8Gi
		9. Make sure data is intact on the PV mounted on the pod
		10. Make sure file system has increased
	*/

	ginkgo.It("[csi-block-vanilla] [csi-supervisor] [csi-block-vanilla-parallelized] [csi-vcp-mig] Verify online volume "+
		"expansion by updating PVC with different sizes concurrently", ginkgo.Label(p1, block,
		vanilla, wcp, core, vc70), func() {

		ginkgo.By("Invoking Test for Volume Expansion")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var originalSizeInMb, fsSize int64
		var err error

		ginkgo.By("Create StorageClass with allowVolumeExpansion set to true, Create PVC")
		volHandle, pvclaim, pv, storageclass := createSCwithVolumeExpansionTrueAndDynamicPVC(
			ctx, f, client, "", storagePolicyName, namespace, ext4FSType)
		defer func() {
			if !supervisorCluster {
				err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			if !vcptocsi {
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			} else {
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create Pod using the above PVC")
		pod, vmUUID := createPODandVerifyVolumeMount(ctx, f, client, namespace, pvclaim, volHandle, "")

		// Fetch original FileSystemSize
		ginkgo.By("Verify filesystem size for mount point /mnt/volume1 before expansion")
		originalSizeInMb, err = getFileSystemSizeForOsType(f, client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			// Delete POD
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
			err := fpod.DeletePodWithWait(ctx, client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			if supervisorCluster {
				ginkgo.By(fmt.Sprintf("Verify volume: %s is detached from PodVM with vmUUID: %s",
					pv.Spec.CSI.VolumeHandle, vmUUID))
				_, err := e2eVSphere.getVMByUUIDWithWait(ctx, vmUUID, supervisorClusterOperationsTimeout)
				gomega.Expect(err).To(gomega.HaveOccurred(),
					fmt.Sprintf("PodVM with vmUUID: %s still exists. So volume: %s is not detached from the PodVM",
						vmUUID, pv.Spec.CSI.VolumeHandle))
			} else {
				isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(
					client, volHandle, pod.Spec.NodeName)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
					fmt.Sprintf("Volume %q is not detached from the node %q", volHandle, pod.Spec.NodeName))
			}
		}()

		ginkgo.By("Expanding current pvc")
		currentPvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
		newSize1 := currentPvcSize.DeepCopy()
		newSize1.Add(resource.MustParse("3Gi"))
		newSize2 := currentPvcSize.DeepCopy()
		newSize2.Add(resource.MustParse("4Gi"))
		newSize3 := currentPvcSize.DeepCopy()
		newSize3.Add(resource.MustParse("8Gi"))

		var wg sync.WaitGroup
		wg.Add(3)
		go resize(client, pvclaim, currentPvcSize, newSize1, &wg)
		go resize(client, pvclaim, currentPvcSize, newSize3, &wg)
		go resize(client, pvclaim, currentPvcSize, newSize2, &wg)
		wg.Wait()

		pvclaim, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvclaim.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvclaim).NotTo(gomega.BeNil())

		ginkgo.By("Waiting for file system resize to finish")
		pvclaim, err = waitForFSResize(pvclaim, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvcConditions := pvclaim.Status.Conditions
		expectEqual(len(pvcConditions), 0, "pvc should not have conditions")

		ginkgo.By("Verify filesystem size for mount point /mnt/volume1")
		fsSize, err = getFileSystemSizeForOsType(f, client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("FileSystemSize after PVC resize %d mb , FileSystemSize Before PVC resize %d mb ",
			fsSize, originalSizeInMb)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		// Filesystem size may be smaller than the size of the block volume
		// so here we are checking if the new filesystem size is greater than
		// the original volume size as the filesystem is formatted for the
		// first time
		gomega.Expect(fsSize).Should(gomega.BeNumerically(">", originalSizeInMb),
			fmt.Sprintf("error updating filesystem size for %q. Resulting filesystem size is %d", pvclaim.Name, fsSize))

		ginkgo.By("File system resize finished successfully")

		pvcsize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
		actualSizeOfPVC := sizeInMb(pvcsize.DeepCopy())
		expectedSize := int64(10240)
		framework.Logf("Actual size after converting to MB : %d", actualSizeOfPVC)
		if actualSizeOfPVC != expectedSize {
			framework.Failf("Received wrong disk size after volume expansion. Expected: %d Actual: %d",
				expectedSize, actualSizeOfPVC)

		}

	})

	/*
		Verify online volume expansion on Shared VVOL datastore
		1. Create StorageClass with allowVolumeExpansion set to true on a shared VVOL datastore.
		2. Create PVC which uses the StorageClass created in step 1.
		3. Wait for PV to be provisioned.
		4. Wait for PVC's status to become Bound and note down the size
		5. Create a Pod using the above created PVC
		6. Modify PVC's size to trigger online volume expansion
		7. verify the PVC status will change to "FilesystemResizePending". Wait till the status is removed
		8. Verify the resized PVC by doing CNS query
		9. Make sure data is intact on the PV mounted on the pod
		10.  Make sure file system has increased
	*/
	ginkgo.It("[csi-block-vanilla] [csi-supervisor] [csi-guest] [csi-block-vanilla-parallelized] Volume expansion "+
		"on shared VVOL datastore", ginkgo.Label(p0, block, vanilla, wcp, tkg, core, vc70), func() {
		ginkgo.By("Invoking Test for Volume Expansion")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var vmUUID string
		var pod *v1.Pod

		if vanillaCluster {
			ginkgo.By("CNS_TEST: Running on Vanilla setup - " +
				"This test covers Online volume expansion on PVC created on VVOL datastore")
		}
		if supervisorCluster {
			ginkgo.By("CNS_TEST: Running on SVC setup - " +
				"This test covers Offline and Online expansion on PVC created on VVOL datastore")
		}
		if guestCluster {
			ginkgo.By("CNS_TEST: Running on GC setup - This test covers Offline" +
				"and Online expansion on PVC created on VVOL datastore")
		}

		sharedVVOLdatastoreURL := os.Getenv(envSharedVVOLDatastoreURL)
		if sharedVVOLdatastoreURL == "" {
			ginkgo.Skip("Skipping the test because SHARED_VVOL_DATASTORE_URL is not set. " +
				"This may be due to testbed is not having shared VVOL datastore.")
		}

		ginkgo.By("Create StorageClass on shared VVOL datastore with allowVolumeExpansion set to true, Create PVC")
		volHandle, pvclaim, pv, storageclass := createSCwithVolumeExpansionTrueAndDynamicPVC(
			ctx, f, client, sharedVVOLdatastoreURL, storagePolicyName, namespace, fsType)
		defer func() {
			if !supervisorCluster {
				err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		if supervisorCluster {
			ginkgo.By("Trigger offline volume expansion on PVC on shared VMFS datastore")
			pvclaim, pod, vmUUID = offlineVolumeExpansionOnSupervisorPVC(client, f, namespace, volHandle, pvclaim)
		}

		if vanillaCluster || guestCluster {
			ginkgo.By("Create POD using the above PVC")
			pod, vmUUID = createPODandVerifyVolumeMount(ctx, f, client, namespace, pvclaim, volHandle, "")
		}

		defer func() {
			// Delete POD
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
			err := fpod.DeletePodWithWait(ctx, client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			if supervisorCluster {
				ginkgo.By(fmt.Sprintf("Verify volume: %s is detached from PodVM with vmUUID: %s",
					pv.Spec.CSI.VolumeHandle, vmUUID))
				_, err := e2eVSphere.getVMByUUIDWithWait(ctx, vmUUID, supervisorClusterOperationsTimeout)
				gomega.Expect(err).To(gomega.HaveOccurred(),
					fmt.Sprintf("PodVM with vmUUID: %s still exists. So volume: %s is not detached from the PodVM",
						vmUUID, pv.Spec.CSI.VolumeHandle))
			} else {
				isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(
					client, pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
					fmt.Sprintf("Volume %q is not detached from the node %q", pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
			}
		}()

		ginkgo.By("Increase PVC size and verify online volume resize")
		increaseSizeOfPvcAttachedToPod(f, client, namespace, pvclaim, pod)
	})

	/*
		Verify online volume expansion on Shared NFS datastore
		1. Create StorageClass with allowVolumeExpansion set to true on a shared NFS datastore.
		2. Create PVC which uses the StorageClass created in step 1.
		3. Wait for PV to be provisioned.
		4. Wait for PVC's status to become Bound and note down the size
		5. Create a Pod using the above created PVC
		6. Modify PVC's size to trigger online volume expansion
		7. verify the PVC status will change to "FilesystemResizePending". Wait till the status is removed
		8. Verify the resized PVC by doing CNS query
		9. Make sure data is intact on the PV mounted on the pod
		10.  Make sure file system has increased
	*/
	ginkgo.It("[csi-block-vanilla] [csi-supervisor] [csi-guest] [csi-block-vanilla-parallelized] [csi-vcp-mig] Volume "+
		"expansion on shared NFS datastore", ginkgo.Label(p0, block, vanilla, wcp, tkg, core, vc70), func() {
		ginkgo.By("Invoking Test for Volume Expansion")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var vmUUID string
		var pod *v1.Pod

		if vanillaCluster {
			ginkgo.By("CNS_TEST: Running on Vanilla setup - " +
				"This test covers Online volume expansion on PVC created on NFS datastore")
		}
		if supervisorCluster {
			ginkgo.By("CNS_TEST: Running on SVC setup - " +
				"This test covers Offline and Online expansion on PVC created on NFS datastore")
		}
		if guestCluster {
			ginkgo.By("CNS_TEST: Running on GC setup - This test covers Offline and" +
				"Online expansion on PVC created on NFS datastore")
		}

		sharedNFSdatastoreURL := os.Getenv(envSharedNFSDatastoreURL)
		if sharedNFSdatastoreURL == "" {
			ginkgo.Skip("Skipping the test because SHARED_NFS_DATASTORE_URL is not set. " +
				"This may be due to testbed is not having shared NFS datastore.")
		}

		ginkgo.By("Create StorageClass on shared NFS datastore with allowVolumeExpansion set to true")
		volHandle, pvclaim, pv, storageclass := createSCwithVolumeExpansionTrueAndDynamicPVC(
			ctx, f, client, sharedNFSdatastoreURL, storagePolicyName, namespace, fsType)
		defer func() {
			if !supervisorCluster {
				err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			if !vcptocsi {
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			} else {
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		if supervisorCluster {
			ginkgo.By("Trigger offline volume expansion on PVC on shared NFS datastore")
			pvclaim, pod, vmUUID = offlineVolumeExpansionOnSupervisorPVC(client, f, namespace, volHandle, pvclaim)
		}

		if vanillaCluster || guestCluster {
			ginkgo.By("Create POD using the above PVC")
			pod, vmUUID = createPODandVerifyVolumeMount(ctx, f, client, namespace, pvclaim, volHandle, "")
		}

		defer func() {
			// Delete POD
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
			err := fpod.DeletePodWithWait(ctx, client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			if supervisorCluster {
				ginkgo.By(fmt.Sprintf("Verify volume: %s is detached from PodVM with vmUUID: %s",
					pv.Spec.CSI.VolumeHandle, vmUUID))
				_, err := e2eVSphere.getVMByUUIDWithWait(ctx, vmUUID, supervisorClusterOperationsTimeout)
				gomega.Expect(err).To(gomega.HaveOccurred(),
					fmt.Sprintf("PodVM with vmUUID: %s still exists. So volume: %s is not detached from the PodVM",
						vmUUID, pv.Spec.CSI.VolumeHandle))
			} else {
				isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(
					client, volHandle, pod.Spec.NodeName)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
					fmt.Sprintf("Volume %q is not detached from the node %q", volHandle, pod.Spec.NodeName))
			}
		}()

		ginkgo.By("Increase PVC size and verify online volume resize")
		increaseSizeOfPvcAttachedToPod(f, client, namespace, pvclaim, pod)

	})

	/*
		Verify online volume expansion on Shared VMFS datastore
		1. Create StorageClass with allowVolumeExpansion set to true on a shared VMFS datastore.
		2. Create PVC which uses the StorageClass created in step 1.
		3. Wait for PV to be provisioned.
		4. Wait for PVC's status to become Bound and note down the size
		5. Create a Pod using the above created PVC
		6. Modify PVC's size to trigger online volume expansion
		7. verify the PVC status will change to "FilesystemResizePending". Wait till the status is removed
		8. Verify the resized PVC by doing CNS query
		9. Make sure data is intact on the PV mounted on the pod
		10.  Make sure file system has increased
	*/
	ginkgo.It("[csi-block-vanilla] [csi-supervisor] [csi-guest] [csi-block-vanilla-parallelized][csi-vcp-mig] Volume "+
		"expansion on shared VMFS datastore", ginkgo.Label(p0, block, vanilla, wcp, tkg, core, vc70), func() {
		ginkgo.By("Invoking Test for Volume Expansion")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var vmUUID string
		var pod *v1.Pod

		if vanillaCluster {
			ginkgo.By("CNS_TEST: Running on Vanilla setup - " +
				"This test covers Online volume expansion on PVC created on NFS datastore")
		}
		if supervisorCluster {
			ginkgo.By("CNS_TEST: Running on SVC setup - " +
				"This test covers Offline and Online expansion on PVC created on NFS datastore")
		}
		if guestCluster {
			ginkgo.By("CNS_TEST: Running on GC setup - This test covers Offline " +
				"and Online expansion on PVC created on NFS datastore")
		}

		sharedVMFSdatastoreURL := os.Getenv(envSharedVMFSDatastoreURL)
		if sharedVMFSdatastoreURL == "" {
			ginkgo.Skip("Skipping the test because SHARED_VMFS_DATASTORE_URL is not set. " +
				"This may be due to testbed is not having shared VNFS datastore.")
		}

		ginkgo.By("Create StorageClass on shared VMFS datastore with allowVolumeExpansion set to true")
		volHandle, pvclaim, pv, storageclass := createSCwithVolumeExpansionTrueAndDynamicPVC(
			ctx, f, client, sharedVMFSdatastoreURL, storagePolicyName, namespace, fsType)
		defer func() {
			if !supervisorCluster {
				err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			if !vcptocsi {
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			} else {
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		if supervisorCluster {
			ginkgo.By("Trigger offline volume expansion on PVC on shared VMFS datastore")
			pvclaim, pod, vmUUID = offlineVolumeExpansionOnSupervisorPVC(client, f, namespace, volHandle, pvclaim)
		}

		if vanillaCluster || guestCluster {
			ginkgo.By("Create POD using the above PVC")
			pod, vmUUID = createPODandVerifyVolumeMount(ctx, f, client, namespace, pvclaim, volHandle, "")
		}

		defer func() {
			// Delete POD
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
			err := fpod.DeletePodWithWait(ctx, client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			if supervisorCluster {
				ginkgo.By(fmt.Sprintf("Verify volume: %s is detached from PodVM with vmUUID: %s",
					pv.Spec.CSI.VolumeHandle, vmUUID))
				_, err := e2eVSphere.getVMByUUIDWithWait(ctx, vmUUID, supervisorClusterOperationsTimeout)
				gomega.Expect(err).To(gomega.HaveOccurred(),
					fmt.Sprintf("PodVM with vmUUID: %s still exists. So volume: %s is not detached from the PodVM",
						vmUUID, pv.Spec.CSI.VolumeHandle))
			} else {
				isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(
					client, volHandle, pod.Spec.NodeName)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
					fmt.Sprintf("Volume %q is not detached from the node %q", volHandle, pod.Spec.NodeName))
			}
		}()

		ginkgo.By("Increase PVC size and verify online volume resize")
		increaseSizeOfPvcAttachedToPod(f, client, namespace, pvclaim, pod)

	})

	/*
		This test verifies offline and online volume expansion on statically created PVC.

		Test Steps:
			1. Create FCD with valid storage policy .
			2. Call CnsRegisterVolume API by specifying VolumeID, AccessMode set to "ReadWriteOnce” and PVC Name
			3. Verify PV and PVC’s should be created and they are bound and note the size
			4. Trigger Offline volume expansion
			5. Verify that PVC size shouldn't have increased
			6. Verify PV size is updated
			7. Create POD
		    8. Verify PVC size is increased and Offline volume expansion comepleted after creating POD
		    9. Resize PVC to trigger online volume expansion on the same PVC
			10. wait for some time for resize to complete and verify that "FilesystemResizePending" is removed from PVC
			11. query CNS volume and make sure new size is updated
			12. Verify data is intact on the PV mounted on the pod
			13. Verify File system has increased
			14. Delete POD, PVC, PV, CNSregisterVolume and SC
	*/
	ginkgo.It("[csi-supervisor] Offline and Online volume resize on statically "+
		"created volume", ginkgo.Label(p0, block, wcp, vc70), func() {
		var err error
		var fsSize int64
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		curtime := time.Now().Unix()
		curtimestring := strconv.FormatInt(curtime, 10)
		pvcName := "cns-pvc-" + curtimestring
		framework.Logf("pvc name :%s", pvcName)

		restConfig, storageclass, profileID := staticProvisioningPreSetUpUtil(ctx, f, client, storagePolicyName)

		defer func() {
			framework.Logf("Delete storage class")
			if !supervisorCluster {
				err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, metav1.DeleteOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Creating FCD (CNS Volume)")
		fcdID, err := e2eVSphere.createFCDwithValidProfileID(ctx,
			"staticfcd"+curtimestring, profileID, diskSizeInMb, defaultDatastore.Reference())
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow newly created FCD:%s to sync with pandora",
			pandoraSyncWaitTime, fcdID))
		time.Sleep(time.Duration(pandoraSyncWaitTime) * time.Second)

		ginkgo.By("Create CNS register volume with above created FCD ")
		cnsRegisterVolume := getCNSRegisterVolumeSpec(ctx, namespace, fcdID, "", pvcName, v1.ReadWriteOnce)
		err = createCNSRegisterVolume(ctx, restConfig, cnsRegisterVolume)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.ExpectNoError(waitForCNSRegisterVolumeToGetCreated(ctx,
			restConfig, namespace, cnsRegisterVolume, poll, supervisorClusterOperationsTimeout))
		cnsRegisterVolumeName := cnsRegisterVolume.GetName()
		framework.Logf("CNS register volume name : %s", cnsRegisterVolumeName)

		ginkgo.By(" verify created PV, PVC and check the bidirectional reference")
		pvclaim, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvcName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pv := getPvFromClaim(client, namespace, pvcName)
		verifyBidirectionalReferenceOfPVandPVC(ctx, client, pvclaim, pv, fcdID)
		volHandle := pv.Spec.CSI.VolumeHandle

		// Modify PVC spec to trigger volume expansion
		// We expand the PVC while no pod is using it to ensure offline expansion
		ginkgo.By("Expanding current pvc")
		currentPvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
		newSize := currentPvcSize.DeepCopy()
		newSize.Add(resource.MustParse("1Gi"))
		framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
		pvclaim, err = expandPVCSize(pvclaim, newSize, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvclaim).NotTo(gomega.BeNil())

		pvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
		if pvcSize.Cmp(newSize) != 0 {
			framework.Failf("error updating pvc size %q", pvclaim.Name)
		}

		ginkgo.By("Waiting for controller volume resize to finish")
		err = waitForPvResizeForGivenPvc(pvclaim, client, totalResizeWaitPeriod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Checking for conditions on pvc")
		pvclaim, err = waitForPVCToReachFileSystemResizePendingCondition(client, namespace, pvclaim.Name, pollTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
		queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		if len(queryResult.Volumes) == 0 {
			err = fmt.Errorf("queryCNSVolumeWithResult returned no volume")
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Verifying disk size requested in volume expansion is honored")
		newSizeInMb := int64(3072)
		if queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsBlockBackingDetails).CapacityInMb != newSizeInMb {
			err = fmt.Errorf("got wrong disk size after volume expansion")

		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Create a Pod to use this PVC, and verify volume has been attached
		ginkgo.By("Creating pod to attach PV to the node")
		pod, err := createPod(ctx, client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false, execCommand)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			ginkgo.By("Deleting the pod")
			err = fpod.DeletePodWithWait(ctx, client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		var vmUUID string
		var exists bool
		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", volHandle, pod.Spec.NodeName))

		annotations := pod.Annotations
		vmUUID, exists = annotations[vmUUIDLabel]
		gomega.Expect(exists).To(gomega.BeTrue(), fmt.Sprintf("Pod doesn't have %s annotation", vmUUIDLabel))

		framework.Logf("VMUUID : %s", vmUUID)
		isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volHandle, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

		ginkgo.By("Verify the volume is accessible and filesystem type is as expected")
		_, err = e2eoutput.LookForStringInPodExec(namespace, pod.Name,
			[]string{"/bin/cat", "/mnt/volume1/fstype"}, "", time.Minute)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting for file system resize to finish")
		pvclaim, err = waitForFSResize(pvclaim, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvcConditions := pvclaim.Status.Conditions
		expectEqual(len(pvcConditions), 0, "pvc should not have conditions")

		ginkgo.By("Verify filesystem size for mount point /mnt/volume1")
		fsSize, err = getFSSizeMb(f, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("File system size after expansion : %d", fsSize)

		// Filesystem size may be smaller than the size of the block volume
		// so here we are checking if the new filesystem size is greater than
		// the original volume size as the filesystem is formatted for the
		// first time after pod creation
		if fsSize < diskSizeInMb {
			framework.Failf("error updating filesystem size for %q. Resulting filesystem size is %d", pvclaim.Name, fsSize)
		}
		ginkgo.By("File system resize finished successfully")

		ginkgo.By("Increase PVC size and verify online volume resize")
		increaseSizeOfPvcAttachedToPod(f, client, namespace, pvclaim, pod)

		ginkgo.By("Deleting the pod")
		err = fpod.DeletePodWithWait(ctx, client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Verify volume: %s is detached from PodVM with vmUUID: %s",
			pv.Spec.CSI.VolumeHandle, vmUUID))
		_, err = e2eVSphere.getVMByUUIDWithWait(ctx, vmUUID, supervisorClusterOperationsTimeout)
		gomega.Expect(err).To(gomega.HaveOccurred(),
			fmt.Sprintf("PodVM with vmUUID: %s still exists. So volume: %s is not detached from the PodVM",
				vmUUID, pv.Spec.CSI.VolumeHandle))

		defer func() {
			testCleanUpUtil(ctx, restConfig, client, cnsRegisterVolume, namespace, pvclaim.Name, pv.Name)
		}()

	})

	/*
		Verify offline and online volume expansion when there is no quota available

			1. Create StorageClass with allowVolumeExpansion set to true.
			2. Create PVC which uses the StorageClass created in step 1.
			3. Wait for PV to be provisioned.
			4. Wait for PVC's status to become Bound and note down the size
			5. Delete reource quota and  modify PVC size to trigger offline volume expansion
			6. Editing PVC will fail saying nsufficient quota
			7. Create a Pod using the above created PVC
			8. Modify PVC's size to trigger online volume expansion - this will fail saying saying nsufficient quota
		    9. Add suffecient quota to namespace
			10. verify the PVC status will change to "FilesystemResizePending". Wait till the status is removed
			11. Verify the resized PVC by doing CNS query
			12. Make sure data is intact on the PV mounted on the pod
			13.  Make sure file system has increased

	*/
	ginkgo.It("[csi-supervisor] Verify offline and online volume expansion when there is no quota "+
		"available", ginkgo.Label(p1, block, wcp, vc70), func() {
		ginkgo.By("Invoking Test for Volume Expansion")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var pv *v1.PersistentVolume
		storagePolicyName2 := GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores2)

		profileID := e2eVSphere.GetSpbmPolicyID(storagePolicyName2)
		framework.Logf("Profile ID : %s", profileID)
		scParameters := make(map[string]string)
		scParameters["storagePolicyID"] = profileID

		ginkgo.By("get  StorageClass to Create PVC")
		storageclass, err := client.StorageV1().StorageClasses().Get(ctx, storagePolicyName2, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		restClientConfig := getRestConfigClient()
		setStoragePolicyQuota(ctx, restClientConfig, storagePolicyName2, namespace, rqLimit)

		pvclaim, err = createPVC(ctx, client, namespace, nil, "", storageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		_, err = fpv.WaitForPVClaimBoundPhase(ctx, client,
			[]*v1.PersistentVolumeClaim{pvclaim}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pv = getPvFromClaim(client, namespace, pvclaim.Name)

		defer func() {
			err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Delete existing resource quota")
		setStoragePolicyQuota(ctx, restClientConfig, storagePolicyName2, namespace, "2Gi")
		defer func() {
			setStoragePolicyQuota(ctx, restClientConfig, storagePolicyName2, namespace, rqLimit)
		}()

		// Trigger offline volume expansion when there is no resource quota available
		ginkgo.By("Expanding current pvc")
		currentPvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
		newSize := currentPvcSize.DeepCopy()
		newSize.Add(resource.MustParse("10Gi"))
		framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
		_, err = expandPVCSize(pvclaim, newSize, client)
		gomega.Expect(err).To(gomega.HaveOccurred())

		ginkgo.By("Create Pod using the above PVC")
		pod, err := createPod(ctx, client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false, execCommand)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Modify PVC spec to trigger volume expansion
		ginkgo.By("Expanding current pvc")
		currentPvcSize = pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
		newSize = currentPvcSize.DeepCopy()
		newSize.Add(resource.MustParse("10Gi"))
		framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
		_, err = expandPVCSize(pvclaim, newSize, client)
		gomega.Expect(err).To(gomega.HaveOccurred())

		ginkgo.By("Create resource quota")
		restClientConfig = getRestConfigClient()
		setStoragePolicyQuota(ctx, restClientConfig, storagePolicyName2, namespace, rqLimit)

		ginkgo.By("Increase PVC size and verify online volume resize")
		increaseSizeOfPvcAttachedToPod(f, client, namespace, pvclaim, pod)

		defer func() {
			// Delete POD
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
			err := fpod.DeletePodWithWait(ctx, client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			annotations := pod.Annotations
			vmUUID, exists := annotations[vmUUIDLabel]
			gomega.Expect(exists).To(gomega.BeTrue(), fmt.Sprintf("Pod doesn't have %s annotation", vmUUIDLabel))

			ginkgo.By(fmt.Sprintf("Verify volume: %s is detached from PodVM with vmUUID: %s",
				pv.Spec.CSI.VolumeHandle, vmUUID))
			_, err = e2eVSphere.getVMByUUIDWithWait(ctx, vmUUID, supervisorClusterOperationsTimeout)
			gomega.Expect(err).To(gomega.HaveOccurred(),
				fmt.Sprintf("PodVM with vmUUID: %s still exists. So volume: %s is not detached from the PodVM",
					vmUUID, pv.Spec.CSI.VolumeHandle))
		}()
	})

	/*
		Verify Offline volume resize when SPS  is down
		1. Create StorageClass with allowVolumeExpansion set to true
		2. Create PVC which uses the StorageClass created in step 1
		3. Wait for PVC's status to become Bound and note down the size
		4. Bring SPS-Service  down
		5. Modify PVC's size to trigger online volume expansion
		6. Verify PVC events shows error saying "failed to expand volume"
		7. Bring up SPS-Service service
		8. Create a Pod using the above created PVC
		9. Verify that PVC has not reached "FilesystemResizePending"
		9. Verify the resized PVC by doing CNS query
		10. Make sure data is intact on the PV mounted on the pod
		11. Make sure file system has increased
	*/
	ginkgo.It("[csi-supervisor] Verify in Offline volume expansion FileSystemResize works "+
		"when SPS-Service is down", ginkgo.Label(p1, block, wcp, vc70), func() {

		ginkgo.By("Invoking Test for Volume Expansion")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var fsSize int64
		var err error

		volHandle, pvclaim, pv, storageclass := createSCwithVolumeExpansionTrueAndDynamicPVC(
			ctx, f, client, "", storagePolicyName, namespace, ext4FSType)
		defer func() {
			if !supervisorCluster {
				err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Bring down SPS service")
		isSPSServiceStopped = true
		err = invokeVCenterServiceControl(ctx, stopOperation, spsServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			if isSPSServiceStopped {
				startVCServiceWait4VPs(ctx, vcAddress, spsServiceName, &isSPSServiceStopped)
			}
		}()

		ginkgo.By("Expanding current pvc")
		currentPvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
		newSize := currentPvcSize.DeepCopy()
		newSize.Add(resource.MustParse("1Gi"))
		framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
		pvclaim, err = expandPVCSize(pvclaim, newSize, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvclaim).NotTo(gomega.BeNil())

		ginkgo.By("File system resize should work when SPS service is down")
		expectedMsg := "FileSystemResizeRequired"
		isFailureFound, err := waitForEventWithReason(client, namespace, pvclaim.Name, expectedMsg)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isFailureFound).To(
			gomega.BeTrue(), "Expected msg %v, to occur but did not occur", expectedMsg)

		pvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
		if pvcSize.Cmp(newSize) != 0 {
			framework.Failf("error updating pvc size %q", pvclaim.Name)
		}

		ginkgo.By("Waiting for controller volume resize to finish")
		err = waitForPvResizeForGivenPvc(pvclaim, client, totalResizeWaitPeriod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Checking for conditions on pvc")
		pvclaim, err = waitForPVCToReachFileSystemResizePendingCondition(client, namespace, pvclaim.Name, pollTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Bringup SPS service")
		startVCServiceWait4VPs(ctx, vcAddress, spsServiceName, &isSPSServiceStopped)

		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
		queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		if len(queryResult.Volumes) == 0 {
			err = fmt.Errorf("queryCNSVolumeWithResult returned no volume")
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verifying disk size requested in volume expansion is honored")
		newSizeInMb := int64(3072)
		if queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsBlockBackingDetails).CapacityInMb != newSizeInMb {
			err = fmt.Errorf("got wrong disk size after volume expansion")
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Create a Pod to use this PVC, and verify volume has been attached
		ginkgo.By("Creating pod to attach PV to the node")
		pod, err := createPod(ctx, client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false, execCommand)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			ginkgo.By("Deleting the pod")
			err = fpod.DeletePodWithWait(ctx, client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		var vmUUID string
		var exists bool
		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", volHandle, pod.Spec.NodeName))
		if supervisorCluster {
			annotations := pod.Annotations
			vmUUID, exists = annotations[vmUUIDLabel]
			gomega.Expect(exists).To(gomega.BeTrue(), fmt.Sprintf("Pod doesn't have %s annotation", vmUUIDLabel))
		} else {
			vmUUID = getNodeUUID(ctx, client, pod.Spec.NodeName)
		}
		framework.Logf("VMUUID : %s", vmUUID)
		isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volHandle, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

		ginkgo.By("Verify the volume is accessible and filesystem type is as expected")
		_, err = e2eoutput.LookForStringInPodExec(namespace, pod.Name,
			[]string{"/bin/cat", "/mnt/volume1/fstype"}, "", time.Minute)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting for file system resize to finish")
		pvclaim, err = waitForFSResize(pvclaim, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvcConditions := pvclaim.Status.Conditions
		expectEqual(len(pvcConditions), 0, "pvc should not have conditions")

		ginkgo.By("Verify filesystem size for mount point /mnt/volume1")
		fsSize, err = getFSSizeMb(f, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("File system size after expansion : %d", fsSize)

		// Filesystem size may be smaller than the size of the block volume
		// so here we are checking if the new filesystem size is greater than
		// the original volume size as the filesystem is formatted for the
		// first time after pod creation
		if fsSize < diskSizeInMb {
			framework.Failf("error updating filesystem size for %q. Resulting filesystem size is %d", pvclaim.Name, fsSize)
		}
		ginkgo.By("File system resize finished successfully")

	})

	/*
		Verify Offline resize when VSAN health is down
		1. Create StorageClass with allowVolumeExpansion set to true
		2. Create PVC which uses the StorageClass created in step 1
		3. Wait for PVC's status to become Bound and note down the size
		4. Bring vsan-health service down
		5. Modify PVC's size to trigger online volume expansion
		6. Verify PVC events shows error saying "503 Service Unavailable"
		7. Bring up vsan-health service
		8. Create a Pod using the above created PVC
		9. Verify that PVC has not reached "FilesystemResizePending"
		9. Verify the resized PVC by doing CNS query
		10. Make sure data is intact on the PV mounted on the pod
		11. Make sure file system has increased
	*/

	ginkgo.It("[csi-supervisor] Verify Offline volume expansion when VSAN-health is "+
		"down", ginkgo.Label(p1, block, wcp, vc70), func() {

		ginkgo.By("Invoking Test for Volume Expansion")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var fsSize int64
		var err error

		volHandle, pvclaim, pv, storageclass := createSCwithVolumeExpansionTrueAndDynamicPVC(
			ctx, f, client, "", storagePolicyName, namespace, ext4FSType)
		defer func() {
			if !supervisorCluster {
				err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Bring down Vsan-health service")
		isVsanHealthServiceStopped = true
		err = invokeVCenterServiceControl(ctx, stopOperation, vsanhealthServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			if isVsanHealthServiceStopped {
				framework.Logf("Bringing vsanhealth up before terminating the test")
				startVCServiceWait4VPs(ctx, vcAddress, vsanhealthServiceName, &isVsanHealthServiceStopped)
			}
		}()

		ginkgo.By("Expanding current pvc")
		currentPvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
		newSize := currentPvcSize.DeepCopy()
		newSize.Add(resource.MustParse("1Gi"))
		framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
		pvclaim, err = expandPVCSize(pvclaim, newSize, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvclaim).NotTo(gomega.BeNil())

		ginkgo.By("File system resize should not succeed Since Vsan-health is down. Expect an error")
		expectedErrMsg := "503 Service Unavailable"
		framework.Logf("Expected failure message: %+q", expectedErrMsg)
		err = waitForEvent(ctx, client, namespace, expectedErrMsg, pvclaim.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Bringup vsanhealth service")
		startVCServiceWait4VPs(ctx, vcAddress, vsanhealthServiceName, &isVsanHealthServiceStopped)

		pvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
		if pvcSize.Cmp(newSize) != 0 {
			framework.Failf("error updating pvc size %q", pvclaim.Name)
		}

		ginkgo.By("Waiting for controller volume resize to finish")
		err = waitForPvResizeForGivenPvc(pvclaim, client, totalResizeWaitPeriod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Checking for conditions on pvc")
		pvclaim, err = waitForPVCToReachFileSystemResizePendingCondition(client, namespace, pvclaim.Name, pollTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
		queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		if len(queryResult.Volumes) == 0 {
			err = fmt.Errorf("queryCNSVolumeWithResult returned no volume")
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Verifying disk size requested in volume expansion is honored")
		newSizeInMb := int64(3072)
		if queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsBlockBackingDetails).CapacityInMb != newSizeInMb {
			err = fmt.Errorf("got wrong disk size after volume expansion")
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Create a Pod to use this PVC, and verify volume has been attached
		ginkgo.By("Creating pod to attach PV to the node")
		pod, err := createPod(ctx, client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false, execCommand)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			ginkgo.By("Deleting the pod")
			err = fpod.DeletePodWithWait(ctx, client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		var vmUUID string
		var exists bool
		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", volHandle, pod.Spec.NodeName))

		annotations := pod.Annotations
		vmUUID, exists = annotations[vmUUIDLabel]
		gomega.Expect(exists).To(gomega.BeTrue(), fmt.Sprintf("Pod doesn't have %s annotation", vmUUIDLabel))

		framework.Logf("VMUUID : %s", vmUUID)
		isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volHandle, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

		ginkgo.By("Verify the volume is accessible and filesystem type is as expected")
		_, err = e2eoutput.LookForStringInPodExec(namespace, pod.Name,
			[]string{"/bin/cat", "/mnt/volume1/fstype"}, "", time.Minute)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting for file system resize to finish")
		pvclaim, err = waitForFSResize(pvclaim, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvcConditions := pvclaim.Status.Conditions
		expectEqual(len(pvcConditions), 0, "pvc should not have conditions")

		ginkgo.By("Verify filesystem size for mount point /mnt/volume1")
		fsSize, err = getFSSizeMb(f, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("File system size after expansion : %d", fsSize)

		// Filesystem size may be smaller than the size of the block volume
		// so here we are checking if the new filesystem size is greater than
		// the original volume size as the filesystem is formatted for the
		// first time after pod creation
		if fsSize < diskSizeInMb {
			framework.Failf("error updating filesystem size for %q. Resulting filesystem size is %d", pvclaim.Name, fsSize)
		}
		ginkgo.By("File system resize finished successfully")

	})

	/*
		Verify online volume expansion on PVC volume when Pod is deleted and re-created

		1. Create StorageClass with allowVolumeExpansion set to true.
		2. Create PVC which uses the StorageClass created in step 1.
		3. Wait for PV to be provisioned.
		4. Wait for PVC's status to become Bound and note down the size
		5. Create a Pod using the above created PVC
		6. Modify PVC's size to trigger online volume expansion
		7. Delete POD
		8. Re-create Pod using same PVC
		9. Verify the resized PVC by doing CNS query
		10. Make sure data is intact on the PV mounted on the pod
		11.  Make sure file system has increased

	*/
	ginkgo.It("[csi-supervisor] [csi-block-vanilla] [csi-guest] [csi-block-vanilla-parallelized][csi-vcp-mig] Verify "+
		"online volume expansion when POD is deleted and re-created", ginkgo.Label(p1,
		block, wcp, vc70), func() {
		ginkgo.By("Invoking Test for Volume Expansion")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var pv *v1.PersistentVolume
		var volHandle string
		var originalSizeInMb int64
		var err error

		ginkgo.By("Create StorageClass with allowVolumeExpansion set to true, Create PVC")
		sharedVSANDatastoreURL := GetAndExpectStringEnvVar(envSharedDatastoreURL)
		volHandle, pvclaim, pv, storageclass = createSCwithVolumeExpansionTrueAndDynamicPVC(
			ctx, f, client, sharedVSANDatastoreURL, storagePolicyName, namespace, fsType)

		defer func() {
			if !supervisorCluster {
				err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			if !vcptocsi {
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			} else {
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create Pod using the above PVC")
		pod, vmUUID := createPODandVerifyVolumeMount(ctx, f, client, namespace, pvclaim, volHandle, "")

		defer func() {
			// Delete POD
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
			err := fpod.DeletePodWithWait(ctx, client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			if supervisorCluster {
				ginkgo.By(fmt.Sprintf("Verify volume: %s is detached from PodVM with vmUUID: %s",
					pv.Spec.CSI.VolumeHandle, vmUUID))
				_, err := e2eVSphere.getVMByUUIDWithWait(ctx, vmUUID, supervisorClusterOperationsTimeout)
				gomega.Expect(err).To(gomega.HaveOccurred(),
					fmt.Sprintf("PodVM with vmUUID: %s still exists. So volume: %s is not detached from the PodVM",
						vmUUID, pv.Spec.CSI.VolumeHandle))
			} else {
				isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(
					client, volHandle, pod.Spec.NodeName)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
					fmt.Sprintf("Volume %q is not detached from the node %q", volHandle, pod.Spec.NodeName))
			}
		}()

		// Fetch original FileSystemSize
		ginkgo.By("Verify filesystem size for mount point /mnt/volume1 before expansion")
		originalSizeInMb, err = getFileSystemSizeForOsType(f, client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// resize PVC
		// Modify PVC spec to trigger volume expansion
		ginkgo.By("Expanding current pvc")
		currentPvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
		newSize := currentPvcSize.DeepCopy()
		newSize.Add(resource.MustParse("1Gi"))
		framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
		pvclaim, err = expandPVCSize(pvclaim, newSize, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvclaim).NotTo(gomega.BeNil())

		ginkgo.By("Deleting Pod after triggering online expansion on PVC")
		err = fpod.DeletePodWithWait(ctx, client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		if supervisorCluster {
			ginkgo.By(fmt.Sprintf("Verify volume: %s is detached from PodVM with vmUUID: %s",
				pv.Spec.CSI.VolumeHandle, vmUUID))
			_, err := e2eVSphere.getVMByUUIDWithWait(ctx, vmUUID, supervisorClusterOperationsTimeout)
			gomega.Expect(err).To(gomega.HaveOccurred(),
				fmt.Sprintf("PodVM with vmUUID: %s still exists. So volume: %s is not detached from the PodVM",
					vmUUID, pv.Spec.CSI.VolumeHandle))
		} else {
			isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(
				client, volHandle, pod.Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
				fmt.Sprintf("Volume %q is not detached from the node %q", volHandle, pod.Spec.NodeName))
		}

		ginkgo.By("re-create Pod using the same PVC")
		pod, vmUUID = createPODandVerifyVolumeMount(ctx, f, client, namespace, pvclaim, volHandle, "")

		ginkgo.By("Waiting for file system resize to finish")
		pvclaim, err = waitForFSResize(pvclaim, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvcConditions := pvclaim.Status.Conditions
		expectEqual(len(pvcConditions), 0, "pvc should not have conditions")

		var fsSize int64
		ginkgo.By("Verify filesystem size for mount point /mnt/volume1")
		fsSize, err = getFileSystemSizeForOsType(f, client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("File system size after expansion : %d", fsSize)
		// Filesystem size may be smaller than the size of the block volume
		// so here we are checking if the new filesystem size is greater than
		// the original volume size as the filesystem is formatted for the
		// first time
		gomega.Expect(fsSize).Should(gomega.BeNumerically(">", originalSizeInMb),
			fmt.Sprintf("error updating filesystem size for %q. Resulting filesystem size is %d", pvclaim.Name, fsSize))
		ginkgo.By("File system resize finished successfully")
	})

	/*
		Verify triggering online volume expansion and delete PVC should not keep any entry in CNSvolume

		1. Create StorageClass with allowVolumeExpansion set to true.
		2. Create PVC which uses the StorageClass created in step 1.
		3. Wait for PV to be provisioned.
		4. Wait for PVC's status to become Bound and note down the size
		5. Create a Pod using the above created PVC
		6. Modify PVC's size to trigger online volume expansion
		7. Delete Pod and PVC
		8. Verify there should not be any PVC entry in CNS
	*/
	ginkgo.It("[csi-supervisor] [csi-block-vanilla] [csi-guest] [csi-block-vanilla-parallelized] [csi-vcp-mig] Verify "+
		"online volume expansion when PVC is deleted", ginkgo.Label(p1,
		vanilla, block, wcp, tkg, core, vc70), func() {
		ginkgo.By("Invoking Test for Volume Expansion")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var pv *v1.PersistentVolume
		var volHandle string
		var err error

		ginkgo.By("Create StorageClass with allowVolumeExpansion set to true, Create PVC")
		sharedVSANDatastoreURL := GetAndExpectStringEnvVar(envSharedDatastoreURL)
		volHandle, pvclaim, pv, storageclass = createSCwithVolumeExpansionTrueAndDynamicPVC(
			ctx, f, client, sharedVSANDatastoreURL, storagePolicyName, namespace, fsType)

		defer func() {
			if !supervisorCluster {
				err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			if pvclaim != nil {
				err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				if !vcptocsi {
					err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				} else {
					err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}
		}()

		ginkgo.By("Create Pod using the above PVC")
		pod, vmUUID := createPODandVerifyVolumeMount(ctx, f, client, namespace, pvclaim, volHandle, "")

		defer func() {
			// Delete POD
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
			err := fpod.DeletePodWithWait(ctx, client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			if supervisorCluster {
				ginkgo.By(fmt.Sprintf("Verify volume: %s is detached from PodVM with vmUUID: %s",
					pv.Spec.CSI.VolumeHandle, vmUUID))
				_, err := e2eVSphere.getVMByUUIDWithWait(ctx, vmUUID, supervisorClusterOperationsTimeout)
				gomega.Expect(err).To(gomega.HaveOccurred(),
					fmt.Sprintf("PodVM with vmUUID: %s still exists. So volume: %s is not detached from the PodVM",
						vmUUID, pv.Spec.CSI.VolumeHandle))
			} else {
				isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(
					client, volHandle, pod.Spec.NodeName)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
					fmt.Sprintf("Volume %q is not detached from the node %q", volHandle, pod.Spec.NodeName))
			}
		}()

		// resize PVC
		// Modify PVC spec to trigger volume expansion
		ginkgo.By("Expanding current pvc")
		currentPvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
		newSize := currentPvcSize.DeepCopy()
		newSize.Add(resource.MustParse("1Gi"))
		framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
		pvclaim, err = expandPVCSize(pvclaim, newSize, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvclaim).NotTo(gomega.BeNil())

		ginkgo.By("Deleting Pod after triggering online expansion on PVC")
		err = fpod.DeletePodWithWait(ctx, client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		if supervisorCluster {
			ginkgo.By(fmt.Sprintf("Verify volume: %s is detached from PodVM with vmUUID: %s",
				pv.Spec.CSI.VolumeHandle, vmUUID))
			_, err := e2eVSphere.getVMByUUIDWithWait(ctx, vmUUID, supervisorClusterOperationsTimeout)
			gomega.Expect(err).To(gomega.HaveOccurred(),
				fmt.Sprintf("PodVM with vmUUID: %s still exists. So volume: %s is not detached from the PodVM",
					vmUUID, pv.Spec.CSI.VolumeHandle))
		} else {
			isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(
				client, volHandle, pod.Spec.NodeName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
				fmt.Sprintf("Volume %q is not detached from the node %q", volHandle, pod.Spec.NodeName))
		}

		err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvclaim = nil
		err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
		queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(len(queryResult.Volumes) == 0)

	})

	/*
		Verify online volume expansion  when CSI Pod is down
		1. Create StorageClass with allowVolumeExpansion set to true .
		2. Create PVC which uses the StorageClass created in step 1.
		3. Wait for PV to be provisioned.
		4. Wait for PVC's status to become Bound and note down the size
		5. Create a Pod using the above created PVC
		6. Bring down CSI controller POD
		7. Modify PVC's size to trigger online volume expansion
		8. verify the PVC status will change to "Resizing", but resize won't succeed until CSI pod is up
		9. Bringup the CSI controller POD
		10. Verify PVC resize completes
		8. Verify the resized PVC by doing CNS query
		9. Make sure data is intact on the PV mounted on the pod
		10.Make sure file system has increased
	*/
	ginkgo.It("[csi-supervisor] Verify online volume expansion when CSI Pod is "+
		"down", ginkgo.Label(p1, block, wcp, vc70), func() {
		ginkgo.By("Invoking Test for Volume Expansion")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var vmUUID string
		var pod *v1.Pod

		ginkgo.By("Create StorageClass on shared VSAN datastore with allowVolumeExpansion set to true")
		sharedVSANDatastoreURL := GetAndExpectStringEnvVar(envSharedDatastoreURL)
		volHandle, pvclaim, pv, storageclass := createSCwithVolumeExpansionTrueAndDynamicPVC(
			ctx, f, client, sharedVSANDatastoreURL, storagePolicyName, namespace, ext4FSType)
		defer func() {
			if !supervisorCluster {
				err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Create Pod using the above PVC")
		pod, vmUUID = createPODandVerifyVolumeMount(ctx, f, client, namespace, pvclaim, volHandle, "")
		defer func() {
			// Delete POD
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
			err := fpod.DeletePodWithWait(ctx, client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			if supervisorCluster {
				ginkgo.By(fmt.Sprintf("Verify volume: %s is detached from PodVM with vmUUID: %s",
					pv.Spec.CSI.VolumeHandle, vmUUID))
				_, err := e2eVSphere.getVMByUUIDWithWait(ctx, vmUUID, supervisorClusterOperationsTimeout)
				gomega.Expect(err).To(gomega.HaveOccurred(),
					fmt.Sprintf("PodVM with vmUUID: %s still exists. So volume: %s is not detached from the PodVM",
						vmUUID, pv.Spec.CSI.VolumeHandle))
			} else {
				isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(
					client, pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
					fmt.Sprintf("Volume %q is not detached from the node %q", pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
			}
		}()

		ginkgo.By("Bringing SVC CSI controller down...")
		svcCsiDeployment := updateDeploymentReplica(client, 0, vSphereCSIControllerPodNamePrefix, csiSystemNamespace)
		defer func() {
			if *svcCsiDeployment.Spec.Replicas == 0 {
				ginkgo.By("Bringing SVC CSI controller up (cleanup)...")
				updateDeploymentReplica(client, 3, vSphereCSIControllerPodNamePrefix, csiSystemNamespace)
			}
		}()

		var originalSizeInMb int64
		// Fetch original FileSystemSize
		ginkgo.By("Verify filesystem size for mount point /mnt/volume1 before expansion")
		originalSizeInMb, err := getFSSizeMb(f, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Expanding current pvc")
		currentPvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
		newSize := currentPvcSize.DeepCopy()
		newSize.Add(resource.MustParse("1Gi"))
		framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
		pvclaim, err = expandPVCSize(pvclaim, newSize, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvclaim).NotTo(gomega.BeNil())

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds for retries...", sleepTimeOut))
		time.Sleep(sleepTimeOut * time.Second)
		pvclaim, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvclaim.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvclaim).NotTo(gomega.BeNil())

		expectedErrMsg := "waiting for an external controller to expand this PVC"
		framework.Logf("Expected failure message: %+q", expectedErrMsg)
		err = waitForEvent(ctx, client, namespace, expectedErrMsg, pvclaim.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Bringing SVC CSI controller up...")
		svcCsiDeployment = updateDeploymentReplica(client, 3, vSphereCSIControllerPodNamePrefix, csiSystemNamespace)

		ginkgo.By("Waiting for controller volume resize to finish PVC1 (online volume expansion)")
		err = waitForPvResizeForGivenPvc(pvclaim, client, totalResizeWaitPeriod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvclaim, err = waitForFSResize(pvclaim, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		var fsSize int64
		ginkgo.By("Verify filesystem size for mount point /mnt/volume1")
		fsSize, err = getFileSystemSizeForOsType(f, client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("File system size after expansion : %d", fsSize)
		// Filesystem size may be smaller than the size of the block volume
		// so here we are checking if the new filesystem size is greater than
		// the original volume size as the filesystem is formatted for the
		// first time
		gomega.Expect(fsSize).Should(gomega.BeNumerically(">", originalSizeInMb),
			fmt.Sprintf("error updating filesystem size for %q. Resulting filesystem size is %d", pvclaim.Name, fsSize))
		ginkgo.By("File system resize finished successfully")
	})

	/*
		Verify offline volume expansion  when CSI Pod is down

		1. Create StorageClass with allowVolumeExpansion set to true .
		2. Create PVC which uses the StorageClass created in step 1.
		3. Wait for PV to be provisioned.
		4. Wait for PVC's status to become Bound and note down the size
		5. Bring down CSI controller POD
		6. Modify PVC's size to trigger online volume expansion
		7. verify the PVC status will change to "Resizing", but resize won't succeed until CSI pod is up
		8. Bringup the CSI controller POD
		9. Create POD
		10. Verify PVC resize completes
		11. Verify the resized PVC by doing CNS query
		12. Make sure data is intact on the PV mounted on the pod
		13. Make sure file system has increased
	*/
	// TODO: Need to add test case for Vanilla
	ginkgo.It("[csi-supervisor] Verify Offline volume expansion when CSI Pod is "+
		"down", ginkgo.Label(p1, block, wcp, vc70), func() {
		ginkgo.By("Invoking Test for Volume Expansion")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var vmUUID string
		var pod *v1.Pod

		ginkgo.By("Create StorageClass on shared VSAN datastore with allowVolumeExpansion set to true")
		sharedVSANDatastoreURL := GetAndExpectStringEnvVar(envSharedDatastoreURL)
		volHandle, pvclaim, pv, storageclass := createSCwithVolumeExpansionTrueAndDynamicPVC(
			ctx, f, client, sharedVSANDatastoreURL, storagePolicyName, namespace, ext4FSType)
		defer func() {
			if !supervisorCluster {
				err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("Bringing SVC CSI controller down...")
		svcCsiDeployment := updateDeploymentReplica(client, 0, vSphereCSIControllerPodNamePrefix, csiSystemNamespace)
		defer func() {
			if *svcCsiDeployment.Spec.Replicas == 0 {
				ginkgo.By("Bringing SVC CSI controller up (cleanup)...")
				updateDeploymentReplica(client, 1, vSphereCSIControllerPodNamePrefix, csiSystemNamespace)
			}
		}()

		ginkgo.By("Expanding current pvc")
		currentPvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
		newSize := currentPvcSize.DeepCopy()
		newSize.Add(resource.MustParse("1Gi"))
		framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
		pvclaim, err := expandPVCSize(pvclaim, newSize, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvclaim).NotTo(gomega.BeNil())

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds for retries...", sleepTimeOut))
		time.Sleep(sleepTimeOut * time.Second)
		pvclaim, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvclaim.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvclaim).NotTo(gomega.BeNil())

		ginkgo.By("Bringing SVC CSI controller up...")
		svcCsiDeployment = updateDeploymentReplica(client, 1, vSphereCSIControllerPodNamePrefix, csiSystemNamespace)

		ginkgo.By("Create Pod using the above PVC")
		pod, vmUUID = createPODandVerifyVolumeMount(ctx, f, client, namespace, pvclaim, volHandle, "")

		defer func() {
			// Delete POD
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
			err := fpod.DeletePodWithWait(ctx, client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By(fmt.Sprintf("Verify volume: %s is detached from PodVM with vmUUID: %s",
				pv.Spec.CSI.VolumeHandle, vmUUID))
			_, err = e2eVSphere.getVMByUUIDWithWait(ctx, vmUUID, supervisorClusterOperationsTimeout)
			gomega.Expect(err).To(gomega.HaveOccurred(),
				fmt.Sprintf("PodVM with vmUUID: %s still exists. So volume: %s is not detached from the PodVM",
					vmUUID, pv.Spec.CSI.VolumeHandle))

		}()

		ginkgo.By("Waiting for file system resize to finish")
		pvclaim, err = waitForFSResize(pvclaim, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvcConditions := pvclaim.Status.Conditions
		expectEqual(len(pvcConditions), 0, "pvc should not have conditions")

		ginkgo.By("Waiting for controller volume resize to finish PVC1 (online volume expansion)")
		err = waitForPvResizeForGivenPvc(pvclaim, client, totalResizeWaitPeriod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		var fsSize int64
		ginkgo.By("Verify filesystem size for mount point /mnt/volume1")
		fsSize, err = getFileSystemSizeForOsType(f, client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("File system size after expansion : %d", fsSize)

		// Filesystem size may be smaller than the size of the block volume
		// so here we are checking if the new filesystem size is greater than
		// the original volume size as the filesystem is formatted for the
		// first time after pod creation
		gomega.Expect(fsSize).Should(gomega.BeNumerically(">", diskSizeInMb),
			fmt.Sprintf("error updating filesystem size for %q. Resulting filesystem size is %d", pvclaim.Name, fsSize))
		ginkgo.By("File system resize finished successfully")
	})

	/*
		Verify triggering offline volume expansion and delete PVC should not keep any entry in CNSvolume

		1. Create StorageClass with thickprovisioning and allowVolumeExpansion set to true.
		2. Create PVC which uses the StorageClass created in step 1.
		3. Wait for PV to be provisioned.
		4. Wait for PVC's status to become Bound and note down the size
		5. Verify there should not be any PVC entry in CNS
	*/
	ginkgo.It("[csi-supervisor] Verify offline volume expansion when PVC is "+
		"deleted", ginkgo.Label(p1, block, wcp, vc70), func() {
		ginkgo.By("Invoking Test for Volume Expansion")

		thickProvPolicy := os.Getenv(envStoragePolicyNameWithThickProvision)
		if thickProvPolicy == "" {
			ginkgo.Skip(envStoragePolicyNameWithThickProvision + " env variable not set")
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var pv *v1.PersistentVolume
		var volHandle string
		var err error

		ginkgo.By("Create StorageClass with allowVolumeExpansion set to true, Create PVC")
		scParameters := make(map[string]string)
		scParameters[scParamFsType] = ext4FSType
		profileID := e2eVSphere.GetSpbmPolicyID(thickProvPolicy)
		scParameters[scParamStoragePolicyID] = profileID
		// create resource quota
		createResourceQuota(client, namespace, rqLimit, thickProvPolicy)
		storageclass, pvclaim, err = createPVCAndStorageClass(ctx, client, namespace,
			nil, scParameters, "", nil, "", true, "", thickProvPolicy)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			if !supervisorCluster {
				err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			if pvclaim != nil {
				err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		// resize PVC
		// Modify PVC spec to trigger volume expansion
		ginkgo.By("Expanding current pvc")
		currentPvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
		newSize := currentPvcSize.DeepCopy()
		newSize.Add(resource.MustParse("1Gi"))
		framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
		pvclaim, err = expandPVCSize(pvclaim, newSize, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvclaim).NotTo(gomega.BeNil())

		err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvclaim = nil
		err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
		queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(len(queryResult.Volumes) == 0)

	})

	/*
		Verify online volume expansion on deployment

		1. Create StorageClass with allowVolumeExpansion set to true.
		2. Create PVC which uses the StorageClass created in step 1.
		3. Wait for PV to be provisioned.
		4. Wait for PVC's status to become Bound and note down the size
		5. Create nginx deployment DEP using above created PVC with 1 replica
		6. wait for all the replicas to come up , verify the deployment POD
		7. Trigger online volume expansion by editing coresponding PVC
		8. Wait for some time for resize to be successful  and verify the PVC size
		9. Verify the resized PVC's by doing CNS query
		10. Make sure file system has increased
		11. Scale down deployment set to 0 replicas and delete all pods, PVC and SC

	*/
	ginkgo.It("[csi-block-vanilla] [csi-supervisor] [csi-guest] [csi-block-vanilla-parallelized] [csi-vcp-mig] Verify "+
		"online volume expansion on deployment", ginkgo.Label(p0, vanilla, block, wcp, tkg, vc70), func() {
		ginkgo.By("Invoking Test for Volume Expansion")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var pv *v1.PersistentVolume
		var volHandle string
		var err error

		ginkgo.By("Create StorageClass with allowVolumeExpansion set to true, Create PVC")
		sharedVSANDatastoreURL := GetAndExpectStringEnvVar(envSharedDatastoreURL)
		volHandle, pvclaim, pv, storageclass = createSCwithVolumeExpansionTrueAndDynamicPVC(
			ctx, f, client, sharedVSANDatastoreURL, storagePolicyName, namespace, fsType)

		defer func() {
			if !supervisorCluster {
				err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			if pvclaim != nil {
				err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				if pvclaim != nil {
					err = fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					if !vcptocsi {
						err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
					} else {
						err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
					}
				}
			}
		}()

		labelsMap := make(map[string]string)
		labelsMap["app"] = "test"
		ginkgo.By("Creating a Deployment using pvc1")

		dep, err := createDeployment(ctx, client, 1, labelsMap, nil, namespace,
			[]*v1.PersistentVolumeClaim{pvclaim}, "", false, busyBoxImageOnGcr)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pods, err := fdep.GetPodsForDeployment(ctx, client, dep)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pod := pods.Items[0]
		err = fpod.WaitForPodNameRunningInNamespace(ctx, client, pod.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		increaseSizeOfPvcAttachedToPod(f, client, namespace, pvclaim, &pod)

		ginkgo.By("Scale down deployment to 0 replica")
		dep, err = client.AppsV1().Deployments(namespace).Get(ctx, dep.Name, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pods, err = fdep.GetPodsForDeployment(ctx, client, dep)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pod = pods.Items[0]
		rep := dep.Spec.Replicas
		*rep = 0
		dep.Spec.Replicas = rep
		dep, err = client.AppsV1().Deployments(namespace).Update(ctx, dep, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = fpod.WaitForPodNotFoundInNamespace(ctx, client, pod.Name, namespace, pollTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			framework.Logf("Delete deployment set")
			err := client.AppsV1().Deployments(namespace).Delete(ctx, dep.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

	})

	/**
	Offline and online volume expansion on stretched SVC
	    1. Create zonal storage policy
		2. Create PVC which uses the above sc.
		3. Wait for PV to be provisioned.
		4. Wait for PVC's status to become Bound and note down the size
		5. Modify PVC's size to trigger offline volume expansion
		6. verify the PVC status will change to "Resizing", but resize won't succeed until POD is created
		9. Create POD
		10. Verify PVC resize completes
		11. Verify the resized PVC by doing CNS query
		12. Modify PVC's size to trigger online volume expansion
		12. Make sure data is intact on the PV mounted on the pod
		13. After resize completes verify and Make sure file system has also increased
		14. clean up the data
	*/
	ginkgo.It("[stretched-svc] offline and online volume "+
		"expansion on stretched svc", ginkgo.Label(p1, block, newTest, vc70), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var pvclaims []*v1.PersistentVolumeClaim
		//var zonalPolicy string
		var categories []string
		var allowedTopologyHAMap map[string][]string

		scParameters := make(map[string]string)

		topologyHaMap := GetAndExpectStringEnvVar(topologyHaMap)
		_, categories = createTopologyMapLevel5(topologyHaMap)
		allowedTopologies := createAllowedTopolgies(topologyHaMap)
		allowedTopologyHAMap = createAllowedTopologiesMap(allowedTopologies)
		framework.Logf("Topology map: %v, categories: %v", allowedTopologyHAMap, categories)

		zonalPolicy := GetAndExpectStringEnvVar(envZonalStoragePolicyName)
		scParameters[svStorageClassName] = zonalPolicy
		storageclass, err := client.StorageV1().StorageClasses().Get(ctx, zonalPolicy, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		pvclaim, err := createPVC(ctx, client, namespace, nil, "", storageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Expect claim to provision volume successfully")
		pvclaims = append(pvclaims, pvclaim)
		pv, err := fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaims, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle := pv[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())

		defer func() {
			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// Modify PVC spec to trigger volume expansion
		// We expand the PVC while no pod is using it to ensure offline expansion
		ginkgo.By("Expanding current pvc")
		currentPvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
		newSize := currentPvcSize.DeepCopy()
		newSize.Add(resource.MustParse("1Gi"))
		framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
		pvclaim, err = expandPVCSize(pvclaim, newSize, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvclaim).NotTo(gomega.BeNil())

		pvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
		if pvcSize.Cmp(newSize) != 0 {
			framework.Failf("error updating pvc size %q", pvclaim.Name)
		}

		ginkgo.By("Waiting for controller volume resize to finish")
		err = waitForPvResizeForGivenPvc(pvclaim, client, totalResizeWaitPeriod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Checking for conditions on pvc")
		pvclaim, err = waitForPVCToReachFileSystemResizePendingCondition(client, namespace, pvclaim.Name, pollTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
		queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		if len(queryResult.Volumes) == 0 {
			err = fmt.Errorf("queryCNSVolumeWithResult returned no volume")
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Verifying disk size requested in volume expansion is honored")
		newSizeInMb := int64(3072)
		if queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsBlockBackingDetails).CapacityInMb != newSizeInMb {
			err = fmt.Errorf("got wrong disk size after volume expansion")
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Create a Pod to use this PVC, and verify volume has been attached
		ginkgo.By("Creating pod to attach PV to the node")
		pod, err := createPod(ctx, client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false, execCommand)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			ginkgo.By("Deleting the pod")
			err = fpod.DeletePodWithWait(ctx, client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		var vmUUID string
		var exists bool
		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", volHandle, pod.Spec.NodeName))

		annotations := pod.Annotations
		vmUUID, exists = annotations[vmUUIDLabel]
		gomega.Expect(exists).To(gomega.BeTrue(), fmt.Sprintf("Pod doesn't have %s annotation", vmUUIDLabel))

		framework.Logf("VMUUID : %s", vmUUID)
		isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volHandle, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

		ginkgo.By("Verify the volume is accessible and filesystem type is as expected")
		_, err = e2eoutput.LookForStringInPodExec(namespace, pod.Name,
			[]string{"/bin/cat", "/mnt/volume1/fstype"}, "", time.Minute)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting for file system resize to finish")
		pvclaim, err = waitForFSResize(pvclaim, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvcConditions := pvclaim.Status.Conditions
		expectEqual(len(pvcConditions), 0, "pvc should not have conditions")

		ginkgo.By("Verify filesystem size for mount point /mnt/volume1")
		fsSize, err := getFSSizeMb(f, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("File system size after expansion : %d, before expansion: %d", fsSize, diskSizeInMb)

		// Filesystem size may be smaller than the size of the block volume
		// so here we are checking if the new filesystem size is greater than
		// the original volume size as the filesystem is formatted for the
		// first time after pod creation
		if fsSize < diskSizeInMb {
			framework.Failf("error updating filesystem size for %q. Resulting filesystem size is %d", pvclaim.Name, fsSize)
		}
		ginkgo.By("File system resize finished successfully")

		ginkgo.By("Increase PVC size and verify online volume resize")
		increaseSizeOfPvcAttachedToPod(f, client, namespace, pvclaim, pod)

	})

})

// increaseOnlineVolumeMultipleTimes this method increases the same volume
// multiple times and verifies PVC and Filesystem size.
func increaseOnlineVolumeMultipleTimes(ctx context.Context, f *framework.Framework,
	client clientset.Interface, namespace string, volHandle string, pvclaim *v1.PersistentVolumeClaim, pod *v1.Pod) {

	var originalSizeInMb, fsSize int64
	var err error
	// Fetch original FileSystemSize
	ginkgo.By("Verify filesystem size for mount point /mnt/volume1 before expansion")
	originalSizeInMb, err = getFileSystemSizeForOsType(f, client, pod)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	// Modify PVC spec to trigger volume expansion
	ginkgo.By("Expanding pvc 10 times")
	currentPvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
	newSize := currentPvcSize.DeepCopy()
	for i := 0; i < 10; i++ {
		newSize.Add(resource.MustParse("1Gi"))
		ginkgo.By(fmt.Sprintf("Expanding pvc to new size: %v", newSize))
		pvclaim, err := expandPVCSize(pvclaim, newSize, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvclaim).NotTo(gomega.BeNil())

		ginkgo.By("Waiting for controller volume resize to finish")
		err = waitForPvResizeForGivenPvc(pvclaim, client, totalResizeWaitPeriod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
		if pvcSize.Cmp(newSize) != 0 {
			framework.Failf("error updating pvc %q to size %v", pvclaim.Name, newSize)
		}
	}

	ginkgo.By("Waiting for controller resize to finish")
	framework.Logf("PVC name : %s ", pvclaim.Name)
	pv := getPvFromClaim(client, namespace, pvclaim.Name)
	pvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
	err = waitForPvResize(pv, client, pvcSize, totalResizeWaitPeriod)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	ginkgo.By("Checking for conditions on pvc")
	framework.Logf("PVC Name %s:", pvclaim.Name)
	pvclaim, err = waitForPVCToReachFileSystemResizePendingCondition(
		client, namespace, pvclaim.Name, totalResizeWaitPeriod)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	pvclaim, err = waitForFSResize(pvclaim, client)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
	queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volHandle)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	if len(queryResult.Volumes) == 0 {
		err = fmt.Errorf("queryCNSVolumeWithResult returned no volume")
	}
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	ginkgo.By("Verifying disk size requested in volume expansion is honored")
	newSizeInMb := int64(12288)
	if queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsBlockBackingDetails).CapacityInMb != newSizeInMb {
		err = fmt.Errorf("received wrong disk size after volume expansion. Expected: %d Actual: %d",
			newSizeInMb, queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsBlockBackingDetails).CapacityInMb)
	}
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	pvclaim, err = waitForFSResize(pvclaim, client)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	pvcConditions := pvclaim.Status.Conditions
	expectEqual(len(pvcConditions), 0, "pvc should not have conditions")

	ginkgo.By("Verify filesystem size for mount point /mnt/volume1")
	fsSize, err = getFileSystemSizeForOsType(f, client, pod)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	// Filesystem size may be smaller than the size of the block volume
	// so here we are checking if the new filesystem size is greater than
	// the original volume size as the filesystem is formatted.
	gomega.Expect(fsSize).Should(gomega.BeNumerically(">", originalSizeInMb),
		fmt.Sprintf("error updating filesystem size for %q. Resulting filesystem size is %d", pvclaim.Name, fsSize))

	framework.Logf("File system resize finished successfully %d mb", fsSize)

}

// createStaticPVC this method creates static PVC
func createStaticPVC(ctx context.Context, f *framework.Framework,
	client clientset.Interface, namespace string, defaultDatastore *object.Datastore,
	pandoraSyncWaitTime int) (string, *v1.PersistentVolumeClaim, *v1.PersistentVolume, *storagev1.StorageClass) {
	curtime := time.Now().Unix()

	var pv *v1.PersistentVolume
	var err error
	var sc *storagev1.StorageClass

	scParameters := make(map[string]string)
	scParameters[scParamFsType] = ext4FSType

	if vcptocsi {
		sc, err = createVcpStorageClass(client, scParameters, nil, "", "", true, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	} else {
		sc, err = createStorageClass(client, nil, nil, "", "", true, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	}

	framework.Logf("Storage Class Name :%s", sc.Name)

	ginkgo.By("Creating FCD Disk")
	curtimeinstring := strconv.FormatInt(curtime, 10)
	fcdID, err := e2eVSphere.createFCD(ctx, "BasicStaticFCD"+curtimeinstring, diskSizeInMb, defaultDatastore.Reference())
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	framework.Logf("FCD ID : %s", fcdID)

	ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow newly created FCD:%s to sync with pandora",
		pandoraSyncWaitTime, fcdID))
	time.Sleep(time.Duration(pandoraSyncWaitTime) * time.Second)

	// Creating label for PV.
	// PVC will use this label as Selector to find PV
	staticPVLabels := make(map[string]string)
	staticPVLabels["fcd-id"] = fcdID

	ginkgo.By("Creating PV")
	pv = getPersistentVolumeSpecWithStorageclass(fcdID, v1.PersistentVolumeReclaimDelete, sc.Name, nil, diskSize)
	if vcptocsi {
		pv.Annotations = map[string]string{pvAnnotationProvisionedBy: e2evSphereCSIDriverName}
	}
	pv, err = client.CoreV1().PersistentVolumes().Create(ctx, pv, metav1.CreateOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	pvName := pv.GetName()

	ginkgo.By("Creating PVC")
	pvc := getPVCSpecWithPVandStorageClass("static-pvc", namespace, nil, pvName, sc.Name, diskSize)
	pvc, err = client.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, pvc, metav1.CreateOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	ginkgo.By("Waiting for claim to be in bound phase")
	err = fpv.WaitForPersistentVolumeClaimPhase(ctx, v1.ClaimBound, client,
		namespace, pvc.Name, framework.Poll, framework.ClaimProvisionTimeout)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	ginkgo.By("Verifying CNS entry is present in cache")
	_, err = e2eVSphere.queryCNSVolumeWithResult(pv.Spec.CSI.VolumeHandle)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	pvc, err = client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	pv = getPvFromClaim(client, namespace, pvc.Name)
	verifyBidirectionalReferenceOfPVandPVC(ctx, client, pvc, pv, fcdID)

	volHandle := pv.Spec.CSI.VolumeHandle

	// Wait for PV and PVC to Bind
	framework.ExpectNoError(fpv.WaitOnPVandPVC(ctx, client, f.Timeouts, namespace, pv, pvc))

	return volHandle, pvc, pv, sc
}

// createSCwithVolumeExpansionTrueAndDynamicPVC creates storageClass with
// allowVolumeExpansion set to true and Creates PVC. Waits till PV, PVC
// are in bound.
func createSCwithVolumeExpansionTrueAndDynamicPVC(ctx context.Context, f *framework.Framework,
	client clientset.Interface, dsurl string, storagePolicyName string, namespace string,
	fstype string) (string, *v1.PersistentVolumeClaim, *v1.PersistentVolume, *storagev1.StorageClass) {
	scParameters := make(map[string]string)
	if vcptocsi {
		scParameters[vcpScParamFstype] = fstype
	} else {
		scParameters[scParamFsType] = fstype
	}

	// Create Storage class and PVC
	ginkgo.By("Creating Storage Class and PVC with allowVolumeExpansion = true")
	var storageclass *storagev1.StorageClass
	var pvclaim *v1.PersistentVolumeClaim
	var err error
	var volHandle string

	if vanillaCluster {
		if dsurl != "" {
			scParameters[scParamDatastoreURL] = dsurl
		}
		if !vcptocsi {
			ginkgo.By("CNS_TEST: Running for vanilla k8s setup")
			storageclass, pvclaim, err = createPVCAndStorageClass(ctx, client, namespace, nil, scParameters,
				"", nil, "", true, "")
		} else {
			storageclass, err = createVcpStorageClass(client, scParameters, nil, "", "", true, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvclaim, err = createPVC(ctx, client, namespace, nil, "", storageclass, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

	} else if supervisorCluster {
		ginkgo.By("CNS_TEST: Running for WCP setup")
		framework.Logf("storagePolicyName: %s", storagePolicyName)
		profileID := e2eVSphere.GetSpbmPolicyID(storagePolicyName)
		scParameters[scParamStoragePolicyID] = profileID

		storageclass, err = client.StorageV1().StorageClasses().Get(ctx, storagePolicyName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		restConfig = getRestConfigClient()
		setStoragePolicyQuota(ctx, restConfig, storagePolicyName, namespace, rqLimit)

		pvclaim, err = createPVC(ctx, client, namespace, nil, "", storageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	} else {
		ginkgo.By("CNS_TEST: Running for GC setup")
		scParameters[svStorageClassName] = storagePolicyName
		storageclass, pvclaim, err = createPVCAndStorageClass(ctx, client, namespace,
			nil, scParameters, "", nil, "", true, "")
	}
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	// Waiting for PVC to be bound
	var pvclaims []*v1.PersistentVolumeClaim
	pvclaims = append(pvclaims, pvclaim)
	ginkgo.By("Waiting for all claims to be in bound state")
	persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaims, framework.ClaimProvisionTimeout)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	pv := persistentvolumes[0]
	if vcptocsi {
		ginkgo.By("Verify annotations on PV/PVCs created after migration")
		waitForMigAnnotationsPvcPvLists(ctx, client, pvclaims, persistentvolumes, false, true)

		ginkgo.By("Verify CnsVSphereVolumeMigration crds and CNS volume metadata for all volumes created " +
			"after migration")
		for _, pvc := range pvclaims {
			vpath := getvSphereVolumePathFromClaim(ctx, client, namespace, pvc.Name)
			newpv := getPvFromClaim(client, namespace, pvc.Name)
			framework.Logf("Processing PVC: %s", pvc.Name)
			crd, err := waitForCnsVSphereVolumeMigrationCrd(ctx, vpath)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = waitAndVerifyCnsVolumeMetadata(ctx, crd.Spec.VolumeID, pvc, newpv, nil)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			volHandle = crd.Spec.VolumeID
		}

	} else {
		volHandle = pv.Spec.CSI.VolumeHandle
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
	}
	if guestCluster {
		volHandle = getVolumeIDFromSupervisorCluster(pv.Spec.CSI.VolumeHandle)
	}

	return volHandle, pvclaims[0], pv, storageclass

}

// createPODandVerifyVolumeMount this method creates Pod and verifies VolumeMount
func createPODandVerifyVolumeMount(ctx context.Context, f *framework.Framework, client clientset.Interface,
	namespace string, pvclaim *v1.PersistentVolumeClaim, volHandle string, expectedContent string) (*v1.Pod, string) {
	// Create a Pod to use this PVC, and verify volume has been attached
	ginkgo.By("Creating pod to attach PV to the node")
	pod, err := createPod(ctx, client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false, execCommand)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	var exists bool
	var vmUUID string
	ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", volHandle, pod.Spec.NodeName))
	if vanillaCluster {
		vmUUID = getNodeUUID(ctx, client, pod.Spec.NodeName)
	} else if guestCluster {
		vmUUID, err = getVMUUIDFromNodeName(pod.Spec.NodeName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	} else {
		annotations := pod.Annotations
		vmUUID, exists = annotations[vmUUIDLabel]
		gomega.Expect(exists).To(gomega.BeTrue(), fmt.Sprintf("Pod doesn't have %s annotation", vmUUIDLabel))
	}
	framework.Logf("VMUUID : %s", vmUUID)
	isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volHandle, vmUUID)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(isDiskAttached).To(gomega.BeTrue(),
		"Volume is not attached to the node volHandle: %s, vmUUID: %s", volHandle, vmUUID)

	ginkgo.By("Verify the volume is accessible and filesystem type is as expected")
	verifyFsTypeOnVsphereVolume(namespace, pod.Name, expectedContent, filePathFsType)

	return pod, vmUUID
}

// increaseSizeOfPvcAttachedToPod this method increases the PVC size, which is attached to POD
func increaseSizeOfPvcAttachedToPod(f *framework.Framework, client clientset.Interface,
	namespace string, pvclaim *v1.PersistentVolumeClaim, pod *v1.Pod) {
	var originalSizeInMb int64
	var err error
	// Fetch original FileSystemSize if not raw block volume
	if *pvclaim.Spec.VolumeMode != v1.PersistentVolumeBlock {
		ginkgo.By("Verify filesystem size for mount point /mnt/volume1 before expansion")
		originalSizeInMb, err = getFileSystemSizeForOsType(f, client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}

	// resize PVC
	// Modify PVC spec to trigger volume expansion
	ginkgo.By("Expanding current pvc")
	currentPvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
	newSize := currentPvcSize.DeepCopy()
	newSize.Add(resource.MustParse("1Gi"))
	framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
	pvclaim, err = expandPVCSize(pvclaim, newSize, client)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(pvclaim).NotTo(gomega.BeNil())

	ginkgo.By("Waiting for file system resize to finish")
	pvclaim, err = waitForFSResize(pvclaim, client)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	pvcConditions := pvclaim.Status.Conditions
	expectEqual(len(pvcConditions), 0, "pvc should not have conditions")

	if *pvclaim.Spec.VolumeMode != v1.PersistentVolumeBlock {
		var fsSize int64
		ginkgo.By("Verify filesystem size for mount point /mnt/volume1")
		fsSize, err = getFileSystemSizeForOsType(f, client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("File system size after expansion : %v, fsSize, after expansion : %v", originalSizeInMb, fsSize)
		// Filesystem size may be smaller than the size of the block volume
		// so here we are checking if the new filesystem size is greater than
		// the original volume size as the filesystem is formatted for the
		// first time
		gomega.Expect(fsSize).Should(gomega.BeNumerically(">", originalSizeInMb),
			fmt.Sprintf("error updating filesystem size for %q. Resulting filesystem size is %d", pvclaim.Name, fsSize))
		ginkgo.By("File system resize finished successfully")
	} else {
		ginkgo.By("Volume resize finished successfully")
	}
}

func invokeTestForVolumeExpansion(f *framework.Framework, client clientset.Interface,
	namespace string, expectedContent string, storagePolicyName string, profileID string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ginkgo.By("Invoking Test for Volume Expansion")
	scParameters := make(map[string]string)
	if windowsEnv {
		scParameters[scParamFsType] = ntfsFSType
	} else {
		scParameters[scParamFsType] = ext4FSType
	}
	// Create Storage class and PVC
	ginkgo.By("Creating Storage Class and PVC with allowVolumeExpansion = true")
	var storageclass *storagev1.StorageClass
	var pvclaim *v1.PersistentVolumeClaim
	var err error

	// Create a StorageClass that sets allowVolumeExpansion to true
	if guestCluster {
		scParameters[svStorageClassName] = storagePolicyName
		storageclass, pvclaim, err = createPVCAndStorageClass(ctx, client,
			namespace, nil, scParameters, "", nil, "", true, "")
	} else if supervisorCluster {
		scParameters[scParamStoragePolicyID] = profileID

		storageclass, pvclaim, err = createPVCAndStorageClass(ctx, client,
			namespace, nil, scParameters, "", nil, "", true, "", storagePolicyName)
	} else if vanillaCluster {
		if !vcptocsi {
			storageclass, pvclaim, err = createPVCAndStorageClass(ctx, client,
				namespace, nil, scParameters, "", nil, "", true, "")
		} else {
			storageclass, err = createVcpStorageClass(client, scParameters, nil, "", "", true, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvclaim, err = createPVC(ctx, client, namespace, nil, "", storageclass, "")

		}
	}
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	defer func() {
		if !supervisorCluster {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	}()
	defer func() {
		err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		if supervisorCluster {
			ginkgo.By("Delete Resource quota")
			deleteResourceQuota(client, namespace)
		}
	}()

	// Waiting for PVC to be bound
	var pvclaims []*v1.PersistentVolumeClaim
	var volHandle, svcPVCName string
	pvclaims = append(pvclaims, pvclaim)
	ginkgo.By("Waiting for all claims to be in bound state")
	persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaims, framework.ClaimProvisionTimeout)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	pv := persistentvolumes[0]
	if vcptocsi {
		ginkgo.By("Verify annotations on PV/PVCs created after migration")
		waitForMigAnnotationsPvcPvLists(ctx, client, pvclaims, persistentvolumes, false, true)

		ginkgo.By("Verify CnsVSphereVolumeMigration crds and CNS volume metadata for all volumes created " +
			"after migration")
		for _, pvc := range pvclaims {
			vpath := getvSphereVolumePathFromClaim(ctx, client, namespace, pvc.Name)
			newpv := getPvFromClaim(client, namespace, pvc.Name)
			framework.Logf("Processing PVC: %s", pvc.Name)
			crd, err := waitForCnsVSphereVolumeMigrationCrd(ctx, vpath)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = waitAndVerifyCnsVolumeMetadata(ctx, crd.Spec.VolumeID, pvc, newpv, nil)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			volHandle = crd.Spec.VolumeID
		}

	} else {
		volHandle = pv.Spec.CSI.VolumeHandle
		svcPVCName = pv.Spec.CSI.VolumeHandle
	}

	if guestCluster {
		volHandle = getVolumeIDFromSupervisorCluster(volHandle)
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
	}

	defer func() {
		err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		if !vcptocsi {
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
		} else {
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
		}

		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}()

	// Modify PVC spec to trigger volume expansion
	// We expand the PVC while no pod is using it to ensure offline expansion
	ginkgo.By("Expanding current pvc")
	currentPvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
	newSize := currentPvcSize.DeepCopy()
	newSize.Add(resource.MustParse("1Gi"))
	framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
	pvclaim, err = expandPVCSize(pvclaim, newSize, client)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(pvclaim).NotTo(gomega.BeNil())

	pvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
	if pvcSize.Cmp(newSize) != 0 {
		framework.Failf("error updating pvc size %q", pvclaim.Name)
	}
	if guestCluster {
		ginkgo.By("Checking for PVC request size change on SVC PVC")
		b, err := verifyPvcRequestedSizeUpdateInSupervisorWithWait(svcPVCName, newSize)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(b).To(gomega.BeTrue())
	}

	ginkgo.By("Waiting for controller volume resize to finish")
	err = waitForPvResizeForGivenPvc(pvclaim, client, totalResizeWaitPeriod)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	if guestCluster {
		ginkgo.By("Checking for resize on SVC PV")
		verifyPVSizeinSupervisor(svcPVCName, newSize)
	}

	ginkgo.By("Checking for conditions on pvc")
	pvclaim, err = waitForPVCToReachFileSystemResizePendingCondition(client, namespace, pvclaim.Name, pollTimeout)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	if guestCluster {
		ginkgo.By("Checking for 'FileSystemResizePending' status condition on SVC PVC")
		_, err = checkSvcPvcHasGivenStatusCondition(pv.Spec.CSI.VolumeHandle,
			true, v1.PersistentVolumeClaimFileSystemResizePending)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}

	ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
	queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volHandle)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	if len(queryResult.Volumes) == 0 {
		err = fmt.Errorf("queryCNSVolumeWithResult returned no volume")
	}
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	ginkgo.By("Verifying disk size requested in volume expansion is honored")
	newSizeInMb := int64(3072)
	if queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsBlockBackingDetails).CapacityInMb != newSizeInMb {
		err = fmt.Errorf("got wrong disk size after volume expansion")
	}
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	// Create a Pod to use this PVC, and verify volume has been attached
	ginkgo.By("Creating pod to attach PV to the node")
	pod, err := createPod(ctx, client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim},
		false, execCommand)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	defer func() {
		ginkgo.By("Deleting the pod")
		err = fpod.DeletePodWithWait(ctx, client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}()

	var vmUUID string
	var exists bool
	ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", volHandle, pod.Spec.NodeName))
	if vanillaCluster {
		vmUUID = getNodeUUID(ctx, client, pod.Spec.NodeName)
	} else if guestCluster {
		vmUUID, err = getVMUUIDFromNodeName(pod.Spec.NodeName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	} else {
		annotations := pod.Annotations
		vmUUID, exists = annotations[vmUUIDLabel]
		gomega.Expect(exists).To(gomega.BeTrue(), fmt.Sprintf("Pod doesn't have %s annotation", vmUUIDLabel))
	}
	framework.Logf("VMUUID : %s", vmUUID)
	isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volHandle, vmUUID)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

	ginkgo.By("Verify the volume is accessible and filesystem type is as expected")
	verifyFsTypeOnVsphereVolume(namespace, pod.Name, expectedContent, filePathFsType)

	ginkgo.By("Waiting for file system resize to finish")
	pvclaim, err = waitForFSResize(pvclaim, client)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	pvcConditions := pvclaim.Status.Conditions
	expectEqual(len(pvcConditions), 0, "pvc should not have conditions")

	var fsSize int64

	ginkgo.By("Verify filesystem size for mount point /mnt/volume1")
	fsSize, err = getFileSystemSizeForOsType(f, client, pod)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	framework.Logf("File system size after expansion : %d", fsSize)

	// Filesystem size may be smaller than the size of the block volume
	// so here we are checking if the new filesystem size is greater than
	// the original volume size as the filesystem is formatted for the
	// first time after pod creation
	if fsSize < diskSizeInMb {
		framework.Failf("error updating filesystem size for %q. Resulting filesystem size is %d",
			pvclaim.Name, fsSize)
	}
	ginkgo.By("File system resize finished successfully")

	if guestCluster {
		ginkgo.By("Checking for PVC resize completion on SVC PVC")
		_, err = waitForFSResizeInSvc(svcPVCName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}

	// Delete POD
	ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
	err = fpod.DeletePodWithWait(ctx, client, pod)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	ginkgo.By("Verify volume is detached from the node")
	if supervisorCluster {
		ginkgo.By(fmt.Sprintf("Verify volume: %s is detached from PodVM with vmUUID: %s",
			pv.Spec.CSI.VolumeHandle, vmUUID))
		_, err := e2eVSphere.getVMByUUIDWithWait(ctx, vmUUID, supervisorClusterOperationsTimeout)
		gomega.Expect(err).To(gomega.HaveOccurred(),
			fmt.Sprintf("PodVM with vmUUID: %s still exists. So volume: %s is not detached from the PodVM",
				vmUUID, pv.Spec.CSI.VolumeHandle))
	} else {
		isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(
			client, volHandle, pod.Spec.NodeName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
			fmt.Sprintf("Volume %q is not detached from the node %q", volHandle, pod.Spec.NodeName))
	}

}

func invokeTestForVolumeExpansionWithFilesystem(f *framework.Framework, client clientset.Interface,
	namespace string, fstype string, expectedContent string, storagePolicyName string, profileID string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ginkgo.By("Invoking Test for Volume Expansion 2")
	scParameters := make(map[string]string)
	scParameters[scParamFsType] = fstype
	// Create Storage class and PVC
	ginkgo.By(fmt.Sprintf("Creating Storage Class with %s filesystem and PVC with allowVolumeExpansion = true",
		fstype))
	var storageclass *storagev1.StorageClass
	var pvclaim *v1.PersistentVolumeClaim
	var err error
	var originalFsSize int64

	// Create a StorageClass that sets allowVolumeExpansion to true
	if guestCluster {
		storagePolicyNameForSharedDatastores := GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)
		scParameters[svStorageClassName] = storagePolicyNameForSharedDatastores
	}
	if vcptocsi {
		storageclass, err = createVcpStorageClass(client, scParameters, nil, "", "",
			true, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvclaim, err = createPVC(ctx, client, namespace, nil, "", storageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	} else {
		storageclass, pvclaim, err = createPVCAndStorageClass(ctx, client, namespace, nil,
			scParameters, "", nil, "", true, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}

	defer func() {
		err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}()
	defer func() {
		err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}()

	// Waiting for PVC to be bound
	var pvclaims []*v1.PersistentVolumeClaim
	var volHandle, svcPVCName string
	pvclaims = append(pvclaims, pvclaim)
	ginkgo.By("Waiting for all claims to be in bound state")
	persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaims, framework.ClaimProvisionTimeout)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	pv := persistentvolumes[0]

	if vcptocsi {
		ginkgo.By("Verify annotations on PV/PVCs created after migration")
		waitForMigAnnotationsPvcPvLists(ctx, client, pvclaims, persistentvolumes, false, true)

		ginkgo.By("Verify CnsVSphereVolumeMigration crds and CNS volume metadata for all volumes created " +
			"after migration")
		for _, pvc := range pvclaims {
			vpath := getvSphereVolumePathFromClaim(ctx, client, namespace, pvc.Name)
			newpv := getPvFromClaim(client, namespace, pvc.Name)
			framework.Logf("Processing PVC: %s", pvc.Name)
			crd, err := waitForCnsVSphereVolumeMigrationCrd(ctx, vpath)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = waitAndVerifyCnsVolumeMetadata(ctx, crd.Spec.VolumeID, pvc, newpv, nil)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			volHandle = crd.Spec.VolumeID
		}

	} else {
		volHandle = pv.Spec.CSI.VolumeHandle
		svcPVCName = pv.Spec.CSI.VolumeHandle
	}

	if guestCluster {
		volHandle = getVolumeIDFromSupervisorCluster(volHandle)
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
	}

	defer func() {
		err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		if !vcptocsi {
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	}()

	// Create a Pod to use this PVC, and verify volume has been attached
	ginkgo.By("Creating pod to attach PV to the node")
	pod, err := createPod(ctx, client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false, execCommand)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	var vmUUID string
	ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", volHandle, pod.Spec.NodeName))
	vmUUID = getNodeUUID(ctx, client, pod.Spec.NodeName)
	if guestCluster {
		vmUUID, err = getVMUUIDFromNodeName(pod.Spec.NodeName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}
	isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volHandle, vmUUID)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

	ginkgo.By("Verify the volume is accessible and filesystem type is as expected")
	verifyFsTypeOnVsphereVolume(namespace, pod.Name, expectedContent, filePathFsType)
	ginkgo.By("Check filesystem size for mount point /mnt/volume1 before expansion")
	originalFsSize, err = getFileSystemSizeForOsType(f, client, pod)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	// Delete POD
	ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
	err = fpod.DeletePodWithWait(ctx, client, pod)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	ginkgo.By("Verify volume is detached from the node")
	isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(
		client, volHandle, pod.Spec.NodeName)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
		fmt.Sprintf("Volume %q is not detached from the node %q", volHandle, pod.Spec.NodeName))

	// Modify PVC spec to trigger volume expansion
	// We expand the PVC while no pod is using it to ensure offline expansion
	ginkgo.By("Expanding current pvc")
	currentPvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
	newSize := currentPvcSize.DeepCopy()
	newSize.Add(resource.MustParse("1Gi"))
	framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
	pvclaim, err = expandPVCSize(pvclaim, newSize, client)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(pvclaim).NotTo(gomega.BeNil())

	pvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
	if pvcSize.Cmp(newSize) != 0 {
		framework.Failf("error updating pvc size %q", pvclaim.Name)
	}
	if guestCluster {
		ginkgo.By("Checking for PVC request size change on SVC PVC")
		b, err := verifyPvcRequestedSizeUpdateInSupervisorWithWait(svcPVCName, newSize)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(b).To(gomega.BeTrue())
	}

	ginkgo.By("Waiting for controller volume resize to finish")
	err = waitForPvResizeForGivenPvc(pvclaim, client, totalResizeWaitPeriod)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	if guestCluster {
		ginkgo.By("Checking for resize on SVC PV")
		verifyPVSizeinSupervisor(svcPVCName, newSize)
	}

	ginkgo.By("Checking for conditions on pvc")
	pvclaim, err = waitForPVCToReachFileSystemResizePendingCondition(client, namespace, pvclaim.Name, pollTimeout)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	if guestCluster {
		ginkgo.By("Checking for 'FileSystemResizePending' status condition on SVC PVC")
		_, err = checkSvcPvcHasGivenStatusCondition(pv.Spec.CSI.VolumeHandle,
			true, v1.PersistentVolumeClaimFileSystemResizePending)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}

	ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
	queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volHandle)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	if len(queryResult.Volumes) == 0 {
		err = fmt.Errorf("queryCNSVolumeWithResult returned no volume")
	}
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	ginkgo.By("Verifying disk size requested in volume expansion is honored")
	newSizeInMb := int64(3072)
	if queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsBlockBackingDetails).CapacityInMb != newSizeInMb {
		err = fmt.Errorf("got wrong disk size after volume expansion")
	}
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	// Create a new Pod to use this PVC, and verify volume has been attached
	ginkgo.By("Creating a new pod to attach PV again to the node")
	pod, err = createPod(ctx, client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false, execCommand)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	ginkgo.By(fmt.Sprintf("Verify volume after expansion: %s is attached to the node: %s",
		volHandle, pod.Spec.NodeName))
	vmUUID = getNodeUUID(ctx, client, pod.Spec.NodeName)
	if guestCluster {
		vmUUID, err = getVMUUIDFromNodeName(pod.Spec.NodeName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}
	isDiskAttached, err = e2eVSphere.isVolumeAttachedToVM(client, volHandle, vmUUID)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

	ginkgo.By("Verify after expansion the volume is accessible and filesystem type is as expected")
	verifyFsTypeOnVsphereVolume(namespace, pod.Name, expectedContent, filePathFsType)

	ginkgo.By("Waiting for file system resize to finish")
	pvclaim, err = waitForFSResize(pvclaim, client)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	pvcConditions := pvclaim.Status.Conditions
	expectEqual(len(pvcConditions), 0, "pvc should not have conditions")

	var fsSize int64
	ginkgo.By("Verify filesystem size for mount point /mnt/volume1 after expansion")
	fsSize, err = getFileSystemSizeForOsType(f, client, pod)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	// Filesystem size may be smaller than the size of the block volume.
	// Here since filesystem was already formatted on the original volume,
	// we can compare the new filesystem size with the original filesystem size.
	if fsSize < originalFsSize {
		framework.Failf("error updating filesystem size for %q. Resulting filesystem size is %d", pvclaim.Name, fsSize)
	}
	ginkgo.By("File system resize finished successfully")

	if guestCluster {
		ginkgo.By("Checking for PVC resize completion on SVC PVC")
		_, err = waitForFSResizeInSvc(svcPVCName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}

	// Delete POD
	ginkgo.By(fmt.Sprintf("Deleting the new pod %s in namespace %s after expansion", pod.Name, namespace))
	err = fpod.DeletePodWithWait(ctx, client, pod)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	ginkgo.By("Verify volume is detached from the node after expansion")
	isDiskDetached, err = e2eVSphere.waitForVolumeDetachedFromNode(client, volHandle, pod.Spec.NodeName)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
		fmt.Sprintf("Volume %q is not detached from the node %q", volHandle, pod.Spec.NodeName))
}

func invokeTestForInvalidVolumeExpansion(f *framework.Framework, client clientset.Interface,
	namespace string, storagePolicyName string, profileID string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	scParameters := make(map[string]string)
	if windowsEnv {
		scParameters[scParamFsType] = ntfsFSType
	} else {
		scParameters[scParamFsType] = ext4FSType
	}

	// Create Storage class and PVC
	ginkgo.By("Creating Storage Class and PVC with allowVolumeExpansion = false")
	var storageclass *storagev1.StorageClass
	var pvclaim *v1.PersistentVolumeClaim
	var err error

	if guestCluster {
		storagePolicyNameForSharedDatastores := GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)
		scParameters[svStorageClassName] = storagePolicyNameForSharedDatastores
	}
	if vcptocsi {
		storageclass, err = createVcpStorageClass(client, scParameters, nil, "", "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pvclaim, err = createPVC(ctx, client, namespace, nil, "", storageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	} else {
		storageclass, pvclaim, err = createPVCAndStorageClass(ctx, client, namespace, nil,
			scParameters, "", nil, "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}

	defer func() {
		err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}()
	defer func() {
		err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}()

	// Waiting for PVC to be bound
	var pvclaims []*v1.PersistentVolumeClaim
	pvclaims = append(pvclaims, pvclaim)
	ginkgo.By("Waiting for all claims to be in bound state")
	persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaims, framework.ClaimProvisionTimeout)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	if vcptocsi {
		ginkgo.By("Verify annotations on PV/PVCs created after migration")
		waitForMigAnnotationsPvcPvLists(ctx, client, pvclaims, persistentvolumes, false, true)

		ginkgo.By("Verify CnsVSphereVolumeMigration crds and CNS volume metadata for all volumes created " +
			"after migration")
		for _, pvc := range pvclaims {
			vpath := getvSphereVolumePathFromClaim(ctx, client, namespace, pvc.Name)
			pv := getPvFromClaim(client, namespace, pvc.Name)
			framework.Logf("Processing PVC: %s", pvc.Name)
			crd, err := waitForCnsVSphereVolumeMigrationCrd(ctx, vpath)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = waitAndVerifyCnsVolumeMetadata(ctx, crd.Spec.VolumeID, pvc, pv, nil)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

	}

	// Modify PVC spec to trigger volume expansion
	// Expect this to fail
	ginkgo.By("Verify expanding pvc will fail because allowVolumeExpansion is false in StorageClass")
	currentPvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
	newSize := currentPvcSize.DeepCopy()
	newSize.Add(resource.MustParse("1Gi"))
	framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
	_, err = expandPVCSize(pvclaim, newSize, client)
	gomega.Expect(err).To(gomega.HaveOccurred())
}

func invokeTestForInvalidVolumeShrink(f *framework.Framework, client clientset.Interface,
	namespace string, storagePolicyName string, profileID string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	scParameters := make(map[string]string)
	if windowsEnv {
		scParameters[scParamFsType] = ntfsFSType
	} else {
		scParameters[scParamFsType] = ext4FSType
	}

	// Create Storage class and PVC
	ginkgo.By("Creating Storage Class and PVC with allowVolumeExpansion = true")
	var storageclass *storagev1.StorageClass
	var pvclaim *v1.PersistentVolumeClaim
	var volHandle string
	var err error

	// Create a StorageClass that sets allowVolumeExpansion to true
	if guestCluster {
		scParameters[svStorageClassName] = storagePolicyName
		storageclass, pvclaim, err = createPVCAndStorageClass(ctx, client,
			namespace, nil, scParameters, "", nil, "", true, "")
	} else if supervisorCluster {
		scParameters[scParamStoragePolicyID] = profileID
		// create resource quota
		createResourceQuota(client, namespace, rqLimit, storagePolicyName)
		storageclass, pvclaim, err = createPVCAndStorageClass(ctx, client,
			namespace, nil, scParameters, "", nil, "", true, "", storagePolicyName)
	} else if vanillaCluster {
		if !vcptocsi {
			storageclass, pvclaim, err = createPVCAndStorageClass(ctx, client,
				namespace, nil, scParameters, "", nil, "", true, "")
		} else {
			storageclass, err = createVcpStorageClass(client, scParameters, nil, "", "", true, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvclaim, err = createPVC(ctx, client, namespace, nil, "", storageclass, "")

		}
	}

	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	defer func() {
		if !supervisorCluster {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	}()
	defer func() {
		err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		if supervisorCluster {
			ginkgo.By("Delete Resource quota")
			deleteResourceQuota(client, namespace)
		}
	}()

	// Waiting for PVC to be bound
	var pvclaims []*v1.PersistentVolumeClaim
	pvclaims = append(pvclaims, pvclaim)
	ginkgo.By("Waiting for all claims to be in bound state")
	persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaims, framework.ClaimProvisionTimeout)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	pv := persistentvolumes[0]
	if vcptocsi {
		ginkgo.By("Verify annotations on PV/PVCs created after migration")
		waitForMigAnnotationsPvcPvLists(ctx, client, pvclaims, persistentvolumes, false, true)

		ginkgo.By("Verify CnsVSphereVolumeMigration crds and CNS volume metadata for all volumes created " +
			"after migration")
		for _, pvc := range pvclaims {
			vpath := getvSphereVolumePathFromClaim(ctx, client, namespace, pvc.Name)
			pv := getPvFromClaim(client, namespace, pvc.Name)
			framework.Logf("Processing PVC: %s", pvc.Name)
			crd, err := waitForCnsVSphereVolumeMigrationCrd(ctx, vpath)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = waitAndVerifyCnsVolumeMetadata(ctx, crd.Spec.VolumeID, pvc, pv, nil)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			volHandle = crd.Spec.VolumeID
		}

	}

	defer func() {
		err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		if !vcptocsi {
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
		} else {
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
		}
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}()

	// Modify PVC spec to a smaller size
	// Expect this to fail
	ginkgo.By("Verify operation will fail because volume shrinking is not supported")
	currentPvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
	newSize := currentPvcSize.DeepCopy()
	newSize.Sub(resource.MustParse("1Gi"))
	framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
	_, err = expandPVCSize(pvclaim, newSize, client)
	gomega.Expect(err).To(gomega.HaveOccurred())
}

func invokeTestForInvalidVolumeExpansionStaticProvision(f *framework.Framework,
	client clientset.Interface, namespace string, storagePolicyName string, profileID string) {
	ginkgo.By("Invoking Test for Invalid Volume Expansion for Static Provisioning")

	var (
		fcdID               string
		pv                  *v1.PersistentVolume
		pvc                 *v1.PersistentVolumeClaim
		defaultDatacenter   *object.Datacenter
		defaultDatastore    *object.Datastore
		deleteFCDRequired   bool
		pandoraSyncWaitTime int
		err                 error
		datastoreURL        string
	)

	scParameters := make(map[string]string)
	if windowsEnv {
		scParameters[scParamFsType] = ntfsFSType
	} else {
		scParameters[scParamFsType] = ext4FSType
	}

	// Set up FCD
	if os.Getenv(envPandoraSyncWaitTime) != "" {
		pandoraSyncWaitTime, err = strconv.Atoi(os.Getenv(envPandoraSyncWaitTime))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	} else {
		pandoraSyncWaitTime = defaultPandoraSyncWaitTime
	}
	deleteFCDRequired = false
	var datacenters []string
	datastoreURL = GetAndExpectStringEnvVar(envSharedDatastoreURL)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	finder := find.NewFinder(e2eVSphere.Client.Client, false)
	cfg, err := getConfig()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	dcList := strings.Split(cfg.Global.Datacenters, ",")
	for _, dc := range dcList {
		dcName := strings.TrimSpace(dc)
		if dcName != "" {
			datacenters = append(datacenters, dcName)
		}
	}

	for _, dc := range datacenters {
		defaultDatacenter, err = finder.Datacenter(ctx, dc)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		finder.SetDatacenter(defaultDatacenter)
		defaultDatastore, err = getDatastoreByURL(ctx, datastoreURL, defaultDatacenter)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}

	ginkgo.By("Creating FCD Disk")
	fcdID, err = e2eVSphere.createFCD(ctx, "BasicStaticFCD", diskSizeInMb, defaultDatastore.Reference())
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	deleteFCDRequired = true

	defer func() {
		if deleteFCDRequired && fcdID != "" && defaultDatastore != nil {
			ginkgo.By(fmt.Sprintf("Deleting FCD: %s", fcdID))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			err := e2eVSphere.deleteFCD(ctx, fcdID, defaultDatastore.Reference())
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	}()

	ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow newly created FCD:%s to sync with pandora",
		pandoraSyncWaitTime, fcdID))
	time.Sleep(time.Duration(pandoraSyncWaitTime) * time.Second)

	// Creating label for PV.
	// PVC will use this label as Selector to find PV
	staticPVLabels := make(map[string]string)
	staticPVLabels["fcd-id"] = fcdID

	ginkgo.By("Creating the PV")
	pv = getPersistentVolumeSpec(fcdID, v1.PersistentVolumeReclaimDelete, staticPVLabels, ext4FSType)
	pv, err = client.CoreV1().PersistentVolumes().Create(ctx, pv, metav1.CreateOptions{})
	if err != nil {
		return
	}

	defer func() {
		ginkgo.By("Verify PV should be deleted automatically")
		framework.ExpectNoError(fpv.WaitForPersistentVolumeDeleted(ctx, client, pv.Name, poll, pollTimeout))
	}()

	err = e2eVSphere.waitForCNSVolumeToBeCreated(pv.Spec.CSI.VolumeHandle)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	ginkgo.By("Creating the PVC")

	pvc = getPersistentVolumeClaimSpec(namespace, staticPVLabels, pv.Name)
	pvc, err = client.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, pvc, metav1.CreateOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	defer func() {
		ginkgo.By("Deleting the PV Claim")
		framework.ExpectNoError(fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace),
			"Failed to delete PVC ", pvc.Name)
	}()

	// Wait for PV and PVC to Bind
	framework.ExpectNoError(fpv.WaitOnPVandPVC(ctx, client, f.Timeouts, namespace, pv, pvc))

	// Set deleteFCDRequired to false.
	// After PV, PVC is in the bind state, Deleting PVC should delete container volume.
	// So no need to delete FCD directly using vSphere API call.
	deleteFCDRequired = false

	ginkgo.By("Verifying CNS entry is present in cache")
	_, err = e2eVSphere.queryCNSVolumeWithResult(pv.Spec.CSI.VolumeHandle)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	// Modify PVC spec to trigger volume expansion
	// Volume expansion will fail because it is not supported
	// on statically provisioned volume
	ginkgo.By("Verify operation will fail because volume expansion on statically provisioned volume is not supported")
	currentPvcSize := pvc.Spec.Resources.Requests[v1.ResourceStorage]
	newSize := currentPvcSize.DeepCopy()
	newSize.Add(resource.MustParse("1Gi"))
	framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)

	_, err = expandPVCSize(pvc, newSize, client)
	gomega.Expect(err).To(gomega.HaveOccurred())
}

func invokeTestForExpandVolumeMultipleTimes(f *framework.Framework, client clientset.Interface,
	namespace string, expectedContent string, storagePolicyName string, profileID string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ginkgo.By("Invoking Test to verify Multiple Volume Expansions on the same volume")
	scParameters := make(map[string]string)
	if windowsEnv {
		scParameters[scParamFsType] = ntfsFSType
	} else {
		scParameters[scParamFsType] = ext4FSType
	}
	// Create Storage class and PVC
	ginkgo.By("Creating Storage Class and PVC with allowVolumeExpansion = true")
	var storageclass *storagev1.StorageClass
	var pvclaim *v1.PersistentVolumeClaim
	var err error

	// Create a StorageClass that sets allowVolumeExpansion to true
	if guestCluster {
		scParameters[svStorageClassName] = storagePolicyName
		storageclass, pvclaim, err = createPVCAndStorageClass(ctx, client,
			namespace, nil, scParameters, "", nil, "", true, "")
	} else if supervisorCluster {
		scParameters[scParamStoragePolicyID] = profileID
		// create resource quota
		createResourceQuota(client, namespace, rqLimit, storagePolicyName)
		storageclass, pvclaim, err = createPVCAndStorageClass(ctx, client,
			namespace, nil, scParameters, "", nil, "", true, "", storagePolicyName)
	} else if vanillaCluster {
		if !vcptocsi {
			storageclass, pvclaim, err = createPVCAndStorageClass(ctx, client,
				namespace, nil, scParameters, "", nil, "", true, "")
		} else {
			storageclass, err = createVcpStorageClass(client, scParameters, nil, "", "", true, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pvclaim, err = createPVC(ctx, client, namespace, nil, "", storageclass, "")

		}
	}
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	defer func() {
		if !supervisorCluster {
			err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	}()
	defer func() {
		err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		if supervisorCluster {
			ginkgo.By("Delete Resource quota")
			deleteResourceQuota(client, namespace)
		}
	}()

	// Waiting for PVC to be bound
	var pvclaims []*v1.PersistentVolumeClaim
	var volHandle, svcPVCName string
	pvclaims = append(pvclaims, pvclaim)
	ginkgo.By("Waiting for all claims to be in bound state")
	persistentvolumes, err := fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaims, framework.ClaimProvisionTimeout)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	pv := persistentvolumes[0]
	if vcptocsi {
		ginkgo.By("Verify annotations on PV/PVCs created after migration")
		waitForMigAnnotationsPvcPvLists(ctx, client, pvclaims, persistentvolumes, false, true)

		ginkgo.By("Verify CnsVSphereVolumeMigration crds and CNS volume metadata for all volumes created " +
			"after migration")
		for _, pvc := range pvclaims {
			vpath := getvSphereVolumePathFromClaim(ctx, client, namespace, pvc.Name)
			newpv := getPvFromClaim(client, namespace, pvc.Name)
			framework.Logf("Processing PVC: %s", pvc.Name)
			crd, err := waitForCnsVSphereVolumeMigrationCrd(ctx, vpath)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			framework.Logf("till here")
			err = waitAndVerifyCnsVolumeMetadata(ctx, crd.Spec.VolumeID, pvc, newpv, nil)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			volHandle = crd.Spec.VolumeID
			framework.Logf("till here again and again")
		}

	} else {
		volHandle = pv.Spec.CSI.VolumeHandle
		svcPVCName = pv.Spec.CSI.VolumeHandle
	}
	if guestCluster {
		volHandle = getVolumeIDFromSupervisorCluster(volHandle)
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
	}

	defer func() {
		err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		if !vcptocsi {
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	}()

	// Modify PVC spec to trigger volume expansion
	// We expand the PVC while no pod is using it to ensure offline expansion
	ginkgo.By("Expanding pvc 10 times")
	currentPvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
	newSize := currentPvcSize.DeepCopy()
	for i := 0; i < 10; i++ {
		newSize.Add(resource.MustParse("1Gi"))
		ginkgo.By(fmt.Sprintf("Expanding pvc to new size: %v", newSize))
		pvclaim, err = expandPVCSize(pvclaim, newSize, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvclaim).NotTo(gomega.BeNil())

		pvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
		if pvcSize.Cmp(newSize) != 0 {
			framework.Failf("error updating pvc %q to size %v", pvclaim.Name, newSize)
		}
		if guestCluster {
			ginkgo.By("Checking for PVC request size change on SVC PVC")
			b, err := verifyPvcRequestedSizeUpdateInSupervisorWithWait(svcPVCName, newSize)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(b).To(gomega.BeTrue())
		}
	}

	ginkgo.By("Waiting for controller resize to finish")
	err = waitForPvResizeForGivenPvc(pvclaim, client, totalResizeWaitPeriod)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	// reconciler takes a few secs in this case to update
	if guestCluster {
		ginkgo.By("Checking for resize on SVC PV")
		err = verifyPVSizeinSupervisorWithWait(ctx, svcPVCName, newSize)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}

	ginkgo.By("Checking for conditions on pvc")
	pvclaim, err = waitForPVCToReachFileSystemResizePendingCondition(client, namespace, pvclaim.Name, pollTimeout)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	if guestCluster {
		ginkgo.By("Checking for 'FileSystemResizePending' status condition on SVC PVC")
		_, err = checkSvcPvcHasGivenStatusCondition(pv.Spec.CSI.VolumeHandle,
			true, v1.PersistentVolumeClaimFileSystemResizePending)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}

	ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
	queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volHandle)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	if len(queryResult.Volumes) == 0 {
		err = fmt.Errorf("queryCNSVolumeWithResult returned no volume")
	}
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	ginkgo.By("Verifying disk size requested in volume expansion is honored")
	newSizeInMb := int64(12288)
	if queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsBlockBackingDetails).CapacityInMb != newSizeInMb {
		err = fmt.Errorf("received wrong disk size after volume expansion. Expected: %d Actual: %d",
			newSizeInMb, queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsBlockBackingDetails).CapacityInMb)
	}
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	// Create a Pod to use this PVC, and verify volume has been attached
	ginkgo.By("Creating pod to attach PV to the node")
	pod, err := createPod(ctx, client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false, execCommand)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	defer func() {
		ginkgo.By("Deleting the pod")
		err = fpod.DeletePodWithWait(ctx, client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}()

	var vmUUID string
	var exists bool
	ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", volHandle, pod.Spec.NodeName))
	if vanillaCluster {
		vmUUID = getNodeUUID(ctx, client, pod.Spec.NodeName)
	} else if guestCluster {
		vmUUID, err = getVMUUIDFromNodeName(pod.Spec.NodeName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	} else {
		annotations := pod.Annotations
		vmUUID, exists = annotations[vmUUIDLabel]
		gomega.Expect(exists).To(gomega.BeTrue(), fmt.Sprintf("Pod doesn't have %s annotation", vmUUIDLabel))
	}
	framework.Logf("VMUUID : %s", vmUUID)

	isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volHandle, vmUUID)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

	ginkgo.By("Verify the volume is accessible and filesystem type is as expected")
	verifyFsTypeOnVsphereVolume(namespace, pod.Name, expectedContent, filePathFsType)

	ginkgo.By("Waiting for file system resize to finish")
	pvclaim, err = waitForFSResize(pvclaim, client)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	pvcConditions := pvclaim.Status.Conditions
	expectEqual(len(pvcConditions), 0, "pvc should not have conditions")

	var fsSize int64

	ginkgo.By("Verify filesystem size for mount point /mnt/volume1")
	fsSize, err = getFileSystemSizeForOsType(f, client, pod)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	framework.Logf("File system size after expansion : %d", fsSize)

	// Filesystem size may be smaller than the size of the block volume
	// so here we are checking if the new filesystem size is greater than
	// the original volume size as the filesystem is formatted for the
	// first time after pod creation
	gomega.Expect(fsSize).Should(gomega.BeNumerically(">", diskSizeInMb),
		fmt.Sprintf("error updating filesystem size for %q. Resulting filesystem size is %d", pvclaim.Name, fsSize))

	if guestCluster {
		ginkgo.By("Checking for PVC resize completion on SVC PVC")
		_, err = waitForFSResizeInSvc(svcPVCName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}

	ginkgo.By(fmt.Sprintf("File system resize finished successfully to %d", fsSize))

	// Delete POD
	ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
	err = fpod.DeletePodWithWait(ctx, client, pod)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	ginkgo.By("Verify volume is detached from the node")
	if supervisorCluster {
		ginkgo.By(fmt.Sprintf("Verify volume: %s is detached from PodVM with vmUUID: %s",
			pv.Spec.CSI.VolumeHandle, vmUUID))
		_, err := e2eVSphere.getVMByUUIDWithWait(ctx, vmUUID, supervisorClusterOperationsTimeout)
		gomega.Expect(err).To(gomega.HaveOccurred(),
			fmt.Sprintf("PodVM with vmUUID: %s still exists. So volume: %s is not detached from the PodVM",
				vmUUID, pv.Spec.CSI.VolumeHandle))
	} else {
		isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(
			client, volHandle, pod.Spec.NodeName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
			fmt.Sprintf("Volume %q is not detached from the node %q", volHandle, pod.Spec.NodeName))
	}

}

func invokeTestForUnsupportedFileVolumeExpansion(f *framework.Framework,
	client clientset.Interface, namespace string, storagePolicyName string, profileID string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ginkgo.By("Invoking Test for Unsupported File Volume Expansion")
	scParameters := make(map[string]string)
	scParameters[scParamFsType] = nfs4FSType
	// Create Storage class and PVC
	ginkgo.By("Creating Storage Class and PVC with allowVolumeExpansion is true and filesystem type is nfs4FSType")
	var storageclass *storagev1.StorageClass
	var pvclaim *v1.PersistentVolumeClaim
	var err error

	// Create a StorageClass that sets allowVolumeExpansion to true
	storageclass, pvclaim, err = createPVCAndStorageClass(ctx, client,
		namespace, nil, scParameters, "", nil, "", true, v1.ReadWriteMany)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	defer func() {
		err := client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}()
	defer func() {
		err := fpv.DeletePersistentVolumeClaim(ctx, client, pvclaim.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}()

	// Waiting for PVC to be bound
	var pvclaims []*v1.PersistentVolumeClaim
	pvclaims = append(pvclaims, pvclaim)
	ginkgo.By("Waiting for all claims to be in bound state")
	// persistentvolumes
	_, err = fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaims, framework.ClaimProvisionTimeout)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	// Modify PVC spec to trigger volume expansion
	// Expect to fail as file volume expansion is not supported
	ginkgo.By("Verify expanding file volume pvc is not supported")
	currentPvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
	newSize := currentPvcSize.DeepCopy()
	newSize.Add(resource.MustParse("1Gi"))
	framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)

	newPVC, err := expandPVCSize(pvclaim, newSize, client)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	pvclaim = newPVC
	gomega.Expect(pvclaim).NotTo(gomega.BeNil())

	pvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
	if pvcSize.Cmp(newSize) != 0 {
		framework.Failf("error updating pvc size %q", pvclaim.Name)
	}

	ginkgo.By("Verify if controller resize failed")
	err = waitForPvResizeForGivenPvc(pvclaim, client, totalResizeWaitPeriod)
	gomega.Expect(err).To(gomega.HaveOccurred())
}

// expandPVCSize expands PVC size
func expandPVCSize(origPVC *v1.PersistentVolumeClaim, size resource.Quantity,
	c clientset.Interface) (*v1.PersistentVolumeClaim, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pvcName := origPVC.Name
	updatedPVC := origPVC.DeepCopy()

	waitErr := wait.PollUntilContextTimeout(ctx, resizePollInterval, 30*time.Second, true,
		func(ctx context.Context) (bool, error) {
			var err error
			updatedPVC, err = c.CoreV1().PersistentVolumeClaims(origPVC.Namespace).Get(ctx, pvcName, metav1.GetOptions{})
			if err != nil {
				return false, fmt.Errorf("error fetching pvc %q for resizing with %v", pvcName, err)
			}

			updatedPVC.Spec.Resources.Requests[v1.ResourceStorage] = size
			updatedPVC, err = c.CoreV1().PersistentVolumeClaims(origPVC.Namespace).Update(
				ctx, updatedPVC, metav1.UpdateOptions{})
			if err == nil {
				return true, nil
			}
			framework.Logf("Error updating pvc %s with %v", pvcName, err)
			return false, nil
		})
	return updatedPVC, waitErr
}

// waitForPvResizeForGivenPvc waits for the controller resize to be finished
func waitForPvResizeForGivenPvc(pvc *v1.PersistentVolumeClaim, c clientset.Interface, duration time.Duration) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pvName := pvc.Spec.VolumeName
	pvcSize := pvc.Spec.Resources.Requests[v1.ResourceStorage]
	pv, err := c.CoreV1().PersistentVolumes().Get(ctx, pvName, metav1.GetOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	return waitForPvResize(pv, c, pvcSize, duration)
}

// waitForPvResize waits for the controller resize to be finished
func waitForPvResize(pv *v1.PersistentVolume, c clientset.Interface,
	size resource.Quantity, duration time.Duration) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	return wait.PollUntilContextTimeout(ctx, resizePollInterval, duration, true,
		func(ctx context.Context) (bool, error) {
			pv, err := c.CoreV1().PersistentVolumes().Get(ctx, pv.Name, metav1.GetOptions{})

			if err != nil {
				return false, fmt.Errorf("error fetching pv %q for resizing %v", pv.Name, err)
			}

			pvSize := pv.Spec.Capacity[v1.ResourceStorage]

			// If pv size is greater or equal to requested size that means controller resize is finished.
			if pvSize.Cmp(size) >= 0 {
				return true, nil
			}
			return false, nil
		})
}

// waitForFSResize waits for the filesystem in the pv to be resized
func waitForFSResize(pvc *v1.PersistentVolumeClaim, c clientset.Interface) (*v1.PersistentVolumeClaim, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var updatedPVC *v1.PersistentVolumeClaim
	waitErr := wait.PollUntilContextTimeout(ctx, resizePollInterval, totalResizeWaitPeriod, true,
		func(ctx context.Context) (bool, error) {
			var err error
			updatedPVC, err = c.CoreV1().PersistentVolumeClaims(pvc.Namespace).Get(ctx, pvc.Name, metav1.GetOptions{})

			if err != nil {
				return false, fmt.Errorf("error fetching pvc %q for checking for resize status : %v", pvc.Name, err)
			}

			pvcSize := updatedPVC.Spec.Resources.Requests[v1.ResourceStorage]
			pvcStatusSize := updatedPVC.Status.Capacity[v1.ResourceStorage]

			// If pvc's status field size is greater than or equal to pvc's size then done
			if pvcStatusSize.Cmp(pvcSize) >= 0 {
				return true, nil
			}
			return false, nil
		})
	return updatedPVC, waitErr
}

// getFSSizeMb returns filesystem size in Mb
func getFSSizeMb(f *framework.Framework, pod *v1.Pod) (int64, error) {
	var output string
	var err error
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if supervisorCluster {
		namespace := getNamespaceToRunTests(f)
		var cmd []string
		if wcpVsanDirectCluster {
			cmd = []string{"exec", pod.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c", "df -Tkm | grep /data0"}
		} else {
			cmd = []string{"exec", pod.Name, "--namespace=" + namespace, "--", "/bin/sh", "-c", "df -Tkm | grep /mnt/volume1"}
		}
		output = e2ekubectl.RunKubectlOrDie(namespace, cmd...)
		gomega.Expect(strings.Contains(output, ext4FSType)).NotTo(gomega.BeFalse())
	} else {
		output, _, err = fpod.ExecShellInPodWithFullOutput(ctx, f, pod.Name, "df -T -m | grep /mnt/volume1")
		if err != nil {
			return -1, fmt.Errorf("unable to find mount path via `df -T`: %v", err)
		}
	}

	arrMountOut := strings.Fields(string(output))
	if len(arrMountOut) <= 0 {
		return -1, fmt.Errorf("error when parsing output of `df -T`. output: %s", string(output))
	}
	var devicePath, strSize string
	devicePath = arrMountOut[0]
	if devicePath == "" {
		return -1, fmt.Errorf("error when parsing output of `df -T` to find out devicePath of /mnt/volume1. output: %s",
			string(output))
	}
	strSize = arrMountOut[2]
	if strSize == "" {
		return -1, fmt.Errorf("error when parsing output of `df -T` to find out size of /mnt/volume1: output: %s",
			string(output))
	}

	intSizeInMb, err := strconv.ParseInt(strSize, 10, 64)
	if err != nil {
		return -1, fmt.Errorf("failed to parse size %s into int size", strSize)
	}

	return intSizeInMb, nil
}

// expectEqual expects the specified two are the same, otherwise an exception raises
func expectEqual(actual interface{}, extra interface{}, explain ...interface{}) {
	gomega.ExpectWithOffset(1, actual).To(gomega.Equal(extra), explain...)
}

// sizeInMb this method converts Bytes to MB
func sizeInMb(size resource.Quantity) int64 {
	actualSize, _ := size.AsInt64()
	actualSize = actualSize / (1024 * 1024)
	return actualSize
}

func testCleanUpUtil(ctx context.Context, restClientConfig *restclient.Config, c clientset.Interface,
	cnsRegistervolume *cnsregistervolumev1alpha1.CnsRegisterVolume, namespace string, pvcName string, pvName string) {
	if guestCluster {
		c, _ = getSvcClientAndNamespace()
	}
	ginkgo.By("Deleting the PV Claim")
	framework.ExpectNoError(fpv.DeletePersistentVolumeClaim(ctx, c, pvcName, namespace), "Failed to delete PVC", pvcName)

	ginkgo.By("Verify PV should be deleted automatically")
	framework.ExpectNoError(fpv.WaitForPersistentVolumeDeleted(ctx, c, pvName, poll, supervisorClusterOperationsTimeout))

	if cnsRegistervolume != nil {
		ginkgo.By("Verify CRD should be deleted automatically")
		framework.ExpectNoError(waitForCNSRegisterVolumeToGetDeleted(ctx, restClientConfig, namespace,
			cnsRegistervolume, poll, supervisorClusterOperationsTimeout))
	}

	ginkgo.By("Delete Resource quota")
	deleteResourceQuota(c, namespace)
}

func offlineVolumeExpansionOnSupervisorPVC(client clientset.Interface, f *framework.Framework, namespace string,
	volHandle string, pvclaim *v1.PersistentVolumeClaim) (*v1.PersistentVolumeClaim, *v1.Pod, string) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ginkgo.By("Expanding current pvc")
	currentPvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
	newSize := currentPvcSize.DeepCopy()
	newSize.Add(resource.MustParse("1Gi"))
	framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
	pvclaim, err := expandPVCSize(pvclaim, newSize, client)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(pvclaim).NotTo(gomega.BeNil())

	pvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
	if pvcSize.Cmp(newSize) != 0 {
		framework.Failf("error updating pvc size %q", pvclaim.Name)
	}

	ginkgo.By("Waiting for controller volume resize to finish")
	err = waitForPvResizeForGivenPvc(pvclaim, client, totalResizeWaitPeriod)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	ginkgo.By("Checking for conditions on pvc")
	pvclaim, err = waitForPVCToReachFileSystemResizePendingCondition(client, namespace, pvclaim.Name, pollTimeout)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
	queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volHandle)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	if len(queryResult.Volumes) == 0 {
		err = fmt.Errorf("queryCNSVolumeWithResult returned no volume")
	}
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	ginkgo.By("Verifying disk size requested in volume expansion is honored")
	newSizeInMb := int64(3072)
	if queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsBlockBackingDetails).CapacityInMb != newSizeInMb {
		err = fmt.Errorf("got wrong disk size after volume expansion")
	}
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	ginkgo.By("Create Pod using the above PVC")
	pod, vmUUID := createPODandVerifyVolumeMount(ctx, f, client, namespace, pvclaim, volHandle, "")

	ginkgo.By("Waiting for file system resize to finish")
	pvclaim, err = waitForFSResize(pvclaim, client)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	pvcConditions := pvclaim.Status.Conditions
	expectEqual(len(pvcConditions), 0, "pvc should not have conditions")

	ginkgo.By("Verify filesystem size for mount point /mnt/volume1")
	fsSize, err := getFSSizeMb(f, pod)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	framework.Logf("File system size after expansion : %d", fsSize)

	// Filesystem size may be smaller than the size of the block volume
	// so here we are checking if the new filesystem size is greater than
	// the original volume size as the filesystem is formatted for the
	// first time after pod creation.
	gomega.Expect(fsSize).Should(gomega.BeNumerically(">", diskSizeInMb),
		fmt.Sprintf("error updating filesystem size for %q. Resulting filesystem size is %d", pvclaim.Name, fsSize))
	ginkgo.By("File system resize finished successfully")
	framework.Logf("Offline volume expansion on PVC is successful")
	return pvclaim, pod, vmUUID
}

// getFileSystemSizeForOsType returns the file system size of the volume in respective OS type
func getFileSystemSizeForOsType(f *framework.Framework, client clientset.Interface, pod *v1.Pod) (int64, error) {
	var fsSize int64
	var err error
	if windowsEnv {
		fsSize, err = getWindowsFileSystemSize(client, pod)
	} else {
		fsSize, err = getFSSizeMb(f, pod)
	}
	return fsSize, err
}
