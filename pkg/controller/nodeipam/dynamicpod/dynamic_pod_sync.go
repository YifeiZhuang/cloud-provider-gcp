package dynamicpod

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"regexp"
	"strings"
	"time"

	jsonpatch "github.com/evanphx/json-patch"

	computepb "google.golang.org/api/compute/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	coreinformers "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	cloudprovider "k8s.io/cloud-provider"
	nodeutil "k8s.io/cloud-provider-gcp/pkg/util"
	"k8s.io/cloud-provider-gcp/providers/gce"
	"k8s.io/klog/v2"
)

const (
	// Annotation keys
	DesiredCIDRsAnnotation = "ipam.gke.io/desired-cidrs"
	ActualCIDRsAnnotation  = "ipam.gke.io/actual-cidrs"

	// Controller name
	ControllerName = "gcp-alias-ip-controller"
	AgentName      = "gcp-alias-ip-agent" // For event recorder

	// MaxRetries is the number of times a node will be retried before it is dropped out of the queue.
	MaxRetries = 10
)

// NetworkCIDRs defines the structure for a single network and its associated CIDRs
// as stored in the node annotations.
type NetworkCIDRs struct {
	Network string   `json:"network"`
	CIDRs   []string `json:"cidrs"`
}

// DesiredCIDRs represents the structure of the desired-cidrs annotation value
type DesiredCIDRs []*NetworkCIDRs

// ActualCIDRs represents the structure of the actual-cidrs annotation value
type ActualCIDRs []*NetworkCIDRs

type DynamicPodIPController struct {
	kubeClient kubernetes.Interface
	cloud      *gce.Cloud

	nodesLister corelisters.NodeLister
	nodesSynced cache.InformerSynced
	workqueue   workqueue.RateLimitingInterface
	recorder    record.EventRecorder
}

func NewDynamicPodIPController(
	kubeClient kubernetes.Interface,
	nodeInformer coreinformers.NodeInformer,
	cloud cloudprovider.Interface,
) (*DynamicPodIPController, error) {

	klog.Info("Creating event broadcaster")
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartStructuredLogging(0)
	eventBroadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: kubeClient.CoreV1().Events("")})
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: AgentName})

	gceCloud, ok := cloud.(*gce.Cloud)
	if !ok {
		err := fmt.Errorf("dynamic prod controller does not support %v provider", cloud.ProviderName())
		return nil, err

	}
	controller := &DynamicPodIPController{
		kubeClient:  kubeClient,
		cloud:       gceCloud,
		nodesLister: nodeInformer.Lister(),
		nodesSynced: nodeInformer.Informer().HasSynced,
		workqueue:   workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), ControllerName),
		recorder:    recorder,
	}

	klog.Info("Setting up event handlers for Node informer")
	nodeInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: controller.handleAddNode,
		UpdateFunc: func(old, new interface{}) {
			oldNode := old.(*corev1.Node)
			newNode := new.(*corev1.Node)

			// Process if the DesiredCIDRsAnnotation changed, or
			// if DesiredCIDRsAnnotation exists and ActualCIDRsAnnotation is missing (e.g. first run for a node)
			if annotationChanged(oldNode, newNode, DesiredCIDRsAnnotation) ||
				(newNode.Annotations[DesiredCIDRsAnnotation] != "" && newNode.Annotations[ActualCIDRsAnnotation] == "") {
				klog.Infof("Node UPDATED: %s. Relevant annotation changed or actual missing. Enqueuing.", newNode.Name)
				controller.enqueueNode(new)
			} else {
				klog.Infof("Node UPDATED: %s. No relevant annotation change. Skipping.", newNode.Name)
			}
		},
		DeleteFunc: controller.handleDeleteNode,
	})

	return controller, nil
}

// annotationChanged checks if a specific annotation has changed between two node versions.
func annotationChanged(oldNode, newNode *corev1.Node, annotationKey string) bool {
	oldAnnotation, oldExists := oldNode.Annotations[annotationKey]
	newAnnotation, newExists := newNode.Annotations[annotationKey]

	if oldExists != newExists {
		return true // Annotation was added or removed
	}
	// If both exist (or both don't exist), check if their values differ
	return oldAnnotation != newAnnotation
}

func (c *DynamicPodIPController) Name() string {
	return "dynamic-pod-ip-controller"
}

func (c *DynamicPodIPController) handleAddNode(obj interface{}) {
	node, ok := obj.(*corev1.Node)
	if !ok {
		utilruntime.HandleError(fmt.Errorf("unexpected object type: %v", obj))
		return
	}
	// If the desired annotation exists, enqueue it for processing.
	if _, exists := node.Annotations[DesiredCIDRsAnnotation]; exists {
		klog.Infof("Node ADDED: %s, has '%s'. Enqueuing.", node.Name, DesiredCIDRsAnnotation)
		c.enqueueNode(obj)
	} else {
		klog.Infof("Node ADDED: %s, no '%s' annotation found. Skipping.", node.Name, DesiredCIDRsAnnotation)
	}
}

func (c *DynamicPodIPController) enqueueNode(obj interface{}) {
	var key string
	var err error
	// MetaNamespaceKeyFunc is safe for cluster-scoped resources like Nodes; it returns "name".
	if key, err = cache.MetaNamespaceKeyFunc(obj); err != nil {
		utilruntime.HandleError(err)
		return
	}
	c.workqueue.Add(key)
	klog.Infof("Enqueued node: %s", key)
}

func (c *DynamicPodIPController) handleDeleteNode(obj interface{}) {
	node, ok := obj.(*corev1.Node)
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("error decoding object, invalid type: %T", obj))
			return
		}
		node, ok = tombstone.Obj.(*corev1.Node)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("error decoding object tombstone, invalid type: %T", tombstone.Obj))
			return
		}
		klog.Infof("Node DELETED (from tombstone): %s. Forgetting from workqueue.", node.Name)
	} else {
		klog.Infof("Node DELETED: %s. Forgetting from workqueue.", node.Name)
	}

	// Remove the item from the work queue if it's there, as the node is gone.
	key, err := cache.MetaNamespaceKeyFunc(obj)
	if err == nil {
		c.workqueue.Forget(key)
		klog.Infof("Forgot key from workqueue due to deletion: %s", key)
	} else {
		utilruntime.HandleError(fmt.Errorf("could not get key for deleted object: %w", err))
	}
	// Future: If this controller owned external resources tied to the node (other than alias IPs managed by annotations),
	// this would be the place to clean them up. For alias IPs, their lifecycle is tied to the VM or other GKE mechanisms
	// when not managed by this specific annotation.
}

// Start begins the controller's operation.
func (c *DynamicPodIPController) Start(ctx context.Context, workers int) error {
	defer utilruntime.HandleCrash() // Don't let panics crash the process
	defer c.workqueue.ShutDown()    // Ensure the workqueue is shutdown when the controller exits

	klog.Info("Starting GCP Dynamic Pod IP controller")

	klog.Info("Waiting for informer caches to sync...")
	if ok := cache.WaitForCacheSync(ctx.Done(), c.nodesSynced); !ok {
		return fmt.Errorf("failed to wait for caches to sync")
	}
	klog.Info("Informer caches synced.")

	klog.Infof("Starting %d workers", workers)
	for i := 0; i < workers; i++ {
		// wait.UntilWithContext will call runWorker repeatedly until the context is cancelled.
		go wait.UntilWithContext(ctx, c.runWorker, time.Second)
	}
	klog.Info("Started workers.")

	<-ctx.Done() // Block until the context is cancelled (e.g., SIGINT, SIGTERM)
	klog.Info("Shutting down workers...")
	return nil
}

// runWorker is a long-running function that will continually call the
// processNextWorkItem function in order to read and process a message from the
// workqueue. It's called by Start in a goroutine.
func (c *DynamicPodIPController) runWorker(ctx context.Context) {
	// processNextWorkItem returns true if it wants to continue processing, false if it should stop
	for c.processNextWorkItem(ctx) {
		// If context is cancelled, stop the worker.
		if ctx.Err() != nil {
			klog.Infof("Context cancelled, worker stopping.")
			return
		}
	}
	klog.Infof("Worker stopping because processNextWorkItem returned false.")
}

// processNextWorkItem will read a single work item off the workqueue and
// attempt to process it, by calling the syncHandler.
func (c *DynamicPodIPController) processNextWorkItem(ctx context.Context) bool {
	obj, shutdown := c.workqueue.Get() // This blocks until an item is available or the queue is shut down.

	if shutdown {
		klog.Info("Work queue is shutting down. Exiting processNextWorkItem.")
		return false // Signal to stop the worker loop.
	}

	// We wrap this block in a func so we can defer c.workqueue.Done.
	// This ensures Done is called even if syncHandler panics.
	err := func(obj interface{}) error {
		defer c.workqueue.Done(obj) // Tell the queue that we've finished processing this item.
		var key string
		var ok bool
		if key, ok = obj.(string); !ok {
			// This item is invalid. Forget it and log an error.
			c.workqueue.Forget(obj)
			utilruntime.HandleError(fmt.Errorf("expected string in workqueue but got %#v", obj))
			return nil // Don't return an error, as it's not a sync error.
		}

		// Run the syncHandler, passing it the context and the key of the Node resource.
		if err := c.syncHandler(ctx, key); err != nil {
			// An error occurred during sync. Re-queue the item if it's retryable.
			if c.workqueue.NumRequeues(key) < MaxRetries {
				klog.Errorf("Error syncing node '%s': %s. Re-queuing (attempt %d/%d).", key, err.Error(), c.workqueue.NumRequeues(key)+1, MaxRetries)
				c.workqueue.AddRateLimited(key) // Add back to the queue with rate limiting.
				return fmt.Errorf("error syncing '%s': %w", key, err)
			}
			// Max retries reached. Forget the item and log a warning.
			klog.Warningf("Giving up on node '%s' after %d retries: %s. Forgetting from queue.", key, MaxRetries, err.Error())
			c.workqueue.Forget(key)
			utilruntime.HandleError(fmt.Errorf("error syncing '%s' and exceeded max retries: %w", key, err))
			return nil // Don't return error to stop further processing of this item.
		}

		// Successfully synced the item. Forget it so it's not processed again until another change.
		c.workqueue.Forget(obj)
		klog.Infof("Successfully synced node '%s'", key)
		return nil
	}(obj)

	if err != nil {
		// This error is from the syncHandler or re-queue logic.
		// utilruntime.HandleError will log it.
		utilruntime.HandleError(err)
		// Continue processing other items.
	}

	return true // Continue processing.
}

// syncHandler compares the actual state with the desired, and attempts to
// converge the two. It then updates the Status block of the Node resource
// with the current status of the resource.
// This is the core logic of your controller.
func (c *DynamicPodIPController) syncHandler(ctx context.Context, key string) error {
	nodeName := key // For Nodes, the key is just the name.

	startTime := time.Now()
	klog.Infof("Started syncing node %q (%v)", nodeName, startTime)
	defer func() {
		klog.Infof("Finished syncing node %q (%v)", nodeName, time.Since(startTime))
	}()

	// Get the Node resource with this name from the lister.
	node, err := c.nodesLister.Get(nodeName)
	if err != nil {
		if errors.IsNotFound(err) {
			klog.Infof("Node '%s' in work queue no longer exists. Nothing to do.", nodeName)
			// utilruntime.HandleError(fmt.Errorf("node '%s' in work queue no longer exists", nodeName))
			return nil // Don't requeue, node is gone.
		}
		return fmt.Errorf("failed to get node %s from lister: %w", nodeName, err)
	}

	// DeepCopy the node object before modifying it.
	// This is crucial because objects from the informer's cache are shared and should not be mutated.
	node = node.DeepCopy()

	// 1. Parse DesiredCIDRsAnnotation
	desiredCIDRsAnnotation, desiredAnnotationExists := node.Annotations[DesiredCIDRsAnnotation]
	var desired DesiredCIDRs

	if !desiredAnnotationExists || desiredCIDRsAnnotation == "" {
		klog.Infof("Node %s: '%s' annotation is missing or empty. Ensuring alias IPs are removed (if any were managed by this controller) and '%s' is removed.", nodeName, DesiredCIDRsAnnotation, ActualCIDRsAnnotation)
		// If desired is gone, we should ensure any alias IPs *this controller might have set* are removed.
		// And the actual-cidrs annotation should also be removed.
		// For this controller, "removing" means setting the desired alias IPs on GCP to an empty list.
		desired = nil // Explicitly set to nil/empty
	} else {
		if err := json.Unmarshal([]byte(desiredCIDRsAnnotation), &desired); err != nil {
			c.recorder.Eventf(node, corev1.EventTypeWarning, "InvalidDesiredCIDRs", "Failed to parse '%s' annotation: %v. Content: %s", DesiredCIDRsAnnotation, err, desiredCIDRsAnnotation)
			// This is a non-retryable error for this specific sync, as the annotation is malformed.
			// Don't requeue by returning nil, but log the error. The user needs to fix the annotation.
			return fmt.Errorf("failed to unmarshal %s annotation for node %s: %w. Annotation content: %s", DesiredCIDRsAnnotation, nodeName, err, desiredCIDRsAnnotation)
		}
	}

	// 2. Get GCP Instance details
	if node.Spec.ProviderID == "" {
		msg := fmt.Sprintf("Node %s has no ProviderID, cannot manage alias IPs", nodeName)
		c.recorder.Eventf(node, corev1.EventTypeWarning, "MissingProviderID", msg)
		return fmt.Errorf(msg) // Non-retryable for this sync unless ProviderID appears later.
	}
	instanceName := node.Spec.ProviderID

	klog.Infof("Node %s: Fetching GCP instance: %s", node.Name, node.Spec.ProviderID)
	instance, err := c.cloud.InstanceByProviderID(node.Spec.ProviderID)
	if err != nil {
		nodeutil.RecordNodeStatusChange(c.recorder, node, "DyanmicPodIPSyncFailed")
		return fmt.Errorf("failed to get instance from provider: %v", err)
	}

	// 3. Compare desired CIDRs with actual alias IPs on the instance.
	// We'll assume managing alias IPs on the primary network interface ("nic0").
	// A more complex setup might require identifying the NIC based on subnetwork or other tags.
	primaryNICName := "nic0"

	targetNic := getTargetNic(instance, primaryNICName)
	if targetNic == nil {
		nodeutil.RecordNodeStatusChange(c.recorder, node, "NICNotFound")
		return fmt.Errorf("Node %s (Instance %s): can not found the NIC %s on the instance.", nodeName, instanceName, primaryNICName)
	}

	desiredDynamicPodIPRangesOnGCP := convertDesiredToDynamicPodIPRanges(targetNic.AliasIpRanges[0].SubnetworkRangeName, desired)

	currentDynamicPodIPRangesOnGCP := targetNic.AliasIpRanges // These are already in *computepb.AliasIpRange format
	if areDynamicPodIPRangesEqual(currentDynamicPodIPRangesOnGCP, desiredDynamicPodIPRangesOnGCP) {
		klog.Infof("Node %s (Instance %s): Dynamic Pod IPs are already in sync.", nodeName, instanceName)
		return nil
	}

	// 4. If mismatching, update the instance.
	// "All the request to mutate to one instance will be merged, and we only use the last version to update instance"
	// This is handled because we always read the latest `desiredCIDRsAnnotation` from the `node` object fetched from the lister.
	// The work queue ensures we process changes, and by the time `syncHandler` runs, it has the latest known desired state.
	klog.Infof("Node %s (Instance %s): Dynamic Pod IPs mismatch. Desired: %d ranges, Actual: %d ranges. Updating instance.", nodeName, instanceName, len(desiredDynamicPodIPRangesOnGCP), len(currentDynamicPodIPRangesOnGCP))
	klog.Infof("Node %s: Desired GCP Ranges: %+v", nodeName, desiredDynamicPodIPRangesOnGCP)
	klog.Infof("Node %s: Current GCP Ranges: %+v", nodeName, currentDynamicPodIPRangesOnGCP)

	c.recorder.Eventf(node, corev1.EventTypeNormal, "UpdatingDynamicPodIPs", "Dynamic Pod IPs mismatch for instance %s, attempting update.", instanceName)

	err = c.updateGCPInstanceDynamicPodIPs(ctx, node.Spec.ProviderID, primaryNICName, desiredDynamicPodIPRangesOnGCP)
	if err != nil {
		c.recorder.Eventf(node, corev1.EventTypeWarning, "UpdateInstanceFailed", "Failed to update alias IPs for instance %s: %v", instanceName, err)
		return fmt.Errorf("failed to update alias IPs for instance %s: %w", instanceName, err) // Retryable
	}
	c.recorder.Eventf(node, corev1.EventTypeNormal, "DynamicPodIPsUpdated", "Successfully initiated update for alias IPs on instance %s", instanceName)
	klog.Infof("Node %s (Instance %s): Successfully updated alias IPs on GCP.", nodeName, instanceName)

	// After a successful update, re-fetch the instance to get the true actual state from GCP.
	// This is important because the SetAliasIpRanges operation might be asynchronous or GCP might make subtle changes.
	updatedInstance, fetchErr := c.cloud.InstanceByProviderID(node.Spec.ProviderID)
	if fetchErr != nil {
		// Log the error but proceed with annotating based on what we *desired* to set,
		// as the update operation itself was reported as successful.
		klog.Warningf("Node %s: Failed to re-fetch instance %s after update, actual-cidrs annotation might be based on desired state: %v", nodeName, instanceName, fetchErr)
		return fmt.Errorf("failed to fetch instance %s after update: %w", instanceName, fetchErr)
	}
	updatedNIC := getTargetNic(updatedInstance, primaryNICName)
	if updatedNIC == nil {
		klog.Warningf("Node %s (Instance %s): the NIC %s not found on the instance.", nodeName, instanceName, primaryNICName)
		return fmt.Errorf("Node %s (Instance %s): can not found the NIC %s on the instance.", nodeName, instanceName, primaryNICName)
	}

	// Fallback to desired if NIC is somehow gone post-update (unlikely)
	currentDynamicPodIPRangesOnGCP = updatedNIC.AliasIpRanges
	// 5. After the operation is done (or if no operation was needed), update the ActualCIDRsAnnotation.
	actualCIDRsForAnnotation := convertDynamicPodIPRangesToAnnotationFormat("default", currentDynamicPodIPRangesOnGCP)
	return c.updateActualCIDRsAnnotation(ctx, node, actualCIDRsForAnnotation) // node is already a DeepCopy
}

// parseProviderID extracts zone and instance name from a GCE provider ID.
// Format: gce://<project-id>/<zone>/<instance-name>
func (c *DynamicPodIPController) parseProviderID(providerID string) (zone, instanceName string, err error) {
	if !strings.HasPrefix(providerID, "gce://") {
		return "", "", fmt.Errorf("providerID '%s' is not for GCE", providerID)
	}
	trimmedID := strings.TrimPrefix(providerID, "gce://")
	parts := strings.Split(trimmedID, "/")
	if len(parts) != 3 {
		// GKE Autopilot nodes might have a different format like:
		// gce://<project-id>/<zone>/gke-<cluster-name>-<nodepool-name>-<hash>
		// Or regional clusters: gce://<project-id>/<region>/<instance-name> (zone is not in providerID)
		// For now, we assume zonal cluster format. A more robust solution would handle regional clusters
		// or allow zone to be configured/discovered differently.
		return "", "", fmt.Errorf("providerID '%s' has unexpected format (expected 3 parts after prefix 'project/zone/instance', got %d parts: %v)", providerID, len(parts), parts)
	}
	// parts[0] is projectID (we use c.gcpProjectID from config)
	// parts[1] is zone
	// parts[2] is instanceName
	return parts[1], parts[2], nil
}

// updateGCPInstanceDynamicPodIPs updates the alias IP ranges for a specific network interface on a GCP instance.
func (c *DynamicPodIPController) updateGCPInstanceDynamicPodIPs(ctx context.Context, providerID, networkInterfaceName string, aliasIpRanges []*computepb.AliasIpRange) error {
	klog.Infof("Node (Instance %s): Updating GCP instance NIC %s, with %d alias IP ranges.", providerID, networkInterfaceName, len(aliasIpRanges))
	for i, r := range aliasIpRanges {
		klog.Infof("Node (Instance %s): Desired range %d: IP CIDR: %s, Subnet Range: %s", providerID, i, r.IpCidrRange, r.SubnetworkRangeName)
	}

	err := c.cloud.UpdateAliasToInstanceByProviderID(providerID, networkInterfaceName, aliasIpRanges)
	if err != nil {
		return fmt.Errorf("computeClient.UPdateAliasIpRanges for instance %s failed: %w", providerID, err)
	}

	klog.Infof("Node %s (Instance %s): UpdateAliasIPs operation completed successfully.", providerID, providerID)
	return nil
}

// areDynamicPodIPRangesEqual checks if two slices of AliasIpRange are semantically equal.
// Order of ranges in the slice does not matter.
func areDynamicPodIPRangesEqual(curr, desired []*computepb.AliasIpRange) bool {
	if len(curr) != len(desired) {
		return false
	}

	for i, c := range curr {
		d := desired[i]
		if c.IpCidrRange == d.IpCidrRange {
			continue
		}
		if match, err := regexp.MatchString(`^/\d+`, d.IpCidrRange); err != nil && !match {
			return false
		}
		_, cCIDR, err := net.ParseCIDR(c.IpCidrRange)
		if err != nil {
			klog.Errorf("Invalid CIDR format returned: %s", c.IpCidrRange)
			return false
		}
		ones, _ := cCIDR.Mask.Size()
		if d.IpCidrRange != fmt.Sprintf("/%d", ones) {
			return false
		}
	}
	return true
}

// convertDesiredToDynamicPodIPRanges converts the DesiredCIDRs annotation format to GCP's []*computepb.AliasIpRange.
func convertDesiredToDynamicPodIPRanges(subnetworkRangeName string, desired DesiredCIDRs) []*computepb.AliasIpRange {
	var ranges []*computepb.AliasIpRange
	if desired == nil { // Handle case where desired annotation is removed or empty
		return ranges // Return empty slice
	}
	// Only support default network.
	for _, networkCIDR := range desired {
		for _, cidr := range networkCIDR.CIDRs {
			if cidr != "" { // Ensure CIDR is not an empty string
				ranges = append(ranges, &computepb.AliasIpRange{
					IpCidrRange:         cidr,
					SubnetworkRangeName: subnetworkRangeName,
				})
			}
		}
	}
	return ranges
}

// convertDynamicPodIPRangesToAnnotationFormat converts GCP's []*computepb.AliasIpRange to the ActualCIDRs annotation format.
func convertDynamicPodIPRangesToAnnotationFormat(network string, aliasIPs []*computepb.AliasIpRange) ActualCIDRs {
	if len(aliasIPs) == 0 {
		return nil // Return nil if there are no actual ranges, so the annotation can be removed.
	}

	result := &NetworkCIDRs{Network: network}
	for _, alias := range aliasIPs {
		result.CIDRs = append(result.CIDRs, alias.IpCidrRange)
	}
	return ActualCIDRs{result}
}

// updateActualCIDRsAnnotation updates the "ipam.gke.io/actual-cidrs" annotation on the Node.
// If actualCIDRs is nil or empty, the annotation is removed.
func (c *DynamicPodIPController) updateActualCIDRsAnnotation(ctx context.Context, originalNode *corev1.Node, actualCIDRs ActualCIDRs) error {
	if actualCIDRs == nil || len(actualCIDRs) == 0 {
		return fmt.Errorf("Actual CIDRS shoud not be nil or empty for node %s", originalNode.Name)
	}

	nodeToUpdate := originalNode.DeepCopy() // Work on a copy
	currentActualAnnotationValue := nodeToUpdate.Annotations[ActualCIDRsAnnotation]

	jsonData, err := json.Marshal(actualCIDRs)
	if err != nil {
		c.recorder.Eventf(nodeToUpdate, corev1.EventTypeWarning, "MarshalActualCIDRsFailed", "Failed to marshal actual CIDRs for %s: %v", ActualCIDRsAnnotation, err)
		return fmt.Errorf("failed to marshal %s for node %s: %w", ActualCIDRsAnnotation, nodeToUpdate.Name, err)
	}
	newActualAnnotationValue := string(jsonData)

	// Check if an update is actually needed
	if currentActualAnnotationValue == newActualAnnotationValue {
		klog.Infof("Node %s: '%s' annotation is already up-to-date. Value: %s", nodeToUpdate.Name, ActualCIDRsAnnotation, currentActualAnnotationValue)
		return nil
	}

	if nodeToUpdate.Annotations == nil {
		nodeToUpdate.Annotations = make(map[string]string)
	}
	nodeToUpdate.Annotations[ActualCIDRsAnnotation] = newActualAnnotationValue
	klog.Infof("Node %s: Setting '%s' to: %s", nodeToUpdate.Name, ActualCIDRsAnnotation, newActualAnnotationValue)

	patchBytes, err := createMergePatch(originalNode, nodeToUpdate)
	if err != nil {
		return fmt.Errorf("failed to create merge patch for node %s: %w", originalNode.Name, err)
	}

	klog.Infof("Node %s: Patching with: %s", nodeToUpdate.Name, string(patchBytes))
	_, err = c.kubeClient.CoreV1().Nodes().Patch(ctx, nodeToUpdate.Name, types.MergePatchType, patchBytes, metav1.PatchOptions{})
	if err != nil {
		c.recorder.Eventf(nodeToUpdate, corev1.EventTypeWarning, "UpdateAnnotationFailed", "Failed to patch '%s' annotation: %v", ActualCIDRsAnnotation, err)
		return fmt.Errorf("failed to patch '%s' annotation for node %s: %w", ActualCIDRsAnnotation, nodeToUpdate.Name, err)
	}

	klog.Infof("Node %s: Successfully updated/removed '%s' annotation.", nodeToUpdate.Name, ActualCIDRsAnnotation)
	c.recorder.Eventf(nodeToUpdate, corev1.EventTypeNormal, "AnnotationUpdated", "Successfully updated/removed %s annotation", ActualCIDRsAnnotation)
	return nil
}

func getTargetNic(instance *computepb.Instance, nicName string) *computepb.NetworkInterface {
	for _, nic := range instance.NetworkInterfaces {
		if nic.Name == nicName {
			return nic
		}
	}
	return nil
}

// createMergePatch creates a strategic merge patch between an original and a modified node object.
func createMergePatch(original, modified *corev1.Node) ([]byte, error) {
	originalJSON, err := json.Marshal(original)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal original node: %w", err)
	}

	modifiedJSON, err := json.Marshal(modified)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal modified node: %w", err)
	}

	// CreateMergePatch generates a patch that transforms originalJSON into modifiedJSON.
	patchBytes, err := jsonpatch.CreateMergePatch(originalJSON, modifiedJSON)
	if err != nil {
		return nil, fmt.Errorf("failed to create merge patch: %w", err)
	}
	return patchBytes, nil
}