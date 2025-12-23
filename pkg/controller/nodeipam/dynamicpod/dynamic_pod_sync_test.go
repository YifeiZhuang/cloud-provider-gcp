package dynamicpod

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"
	"testing"
	// "time" // Not strictly needed for these sync tests yet

	"github.com/GoogleCloudPlatform/k8s-cloud-provider/pkg/cloud"
	"github.com/stretchr/testify/assert"
	computebeta "google.golang.org/api/compute/v0.beta"
	compute "google.golang.org/api/compute/v1"

	"github.com/GoogleCloudPlatform/k8s-cloud-provider/pkg/cloud/meta"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	// "k8s.io/apimachinery/pkg/types" // Not used directly in test yet
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	listersv1 "k8s.io/client-go/listers/core/v1"
	// "k8s.io/client-go/testing" // For more advanced action checking if needed
	"k8s.io/client-go/tools/record"
	"k8s.io/cloud-provider-gcp/providers/gce"
)

const (
	testProjectID      = "test-project"
	testZone           = "us-central1-a"
	testInstanceName   = "test-instance"
	testNodeName       = "test-node"
	testProviderID     = "gce://" + testProjectID + "/" + testZone + "/" + testInstanceName
	testPrimaryNICName = "nic0"
	exampleSubnetURL   = "https://www.googleapis.com/compute/v1/projects/regions/us-centra1/subnetworks/default"
)

func TestDynamicPodIPControllerSync(t *testing.T) {
	desiredCIDRs1JSON := `[{"subnetwork1":["/24"]}]`
	actualCIDRs1JSON := `[{"subnetwork1":["15.0.1.0/24"]}]`
	gcpAliasIPs1 := []*compute.AliasIpRange{
		{IpCidrRange: "15.0.1.0/24", SubnetworkRangeName: "subnetwork1"},
	}

	desiredCIDRs2JSON := `[{"subnetwork1":["10.1.0.0/24","/24"]},{"subnetwork2":["192.168.1.0/28"]}]`
	gcpAliasIPs2 := []*compute.AliasIpRange{
		{IpCidrRange: "10.1.0.0/24", SubnetworkRangeName: "subnetwork1"},
		{IpCidrRange: "15.0.1.0/24", SubnetworkRangeName: "subnetwork1"},
		{IpCidrRange: "192.168.1.0/28", SubnetworkRangeName: "subnetwork2"},
	}
	tempActualCIDRs2 := convertDynamicPodIPRangesToAnnotationFormat(gcpAliasIPs2)
	actualCIDRs2Bytes, _ := json.Marshal(tempActualCIDRs2)
	actualCIDRs2JSON := string(actualCIDRs2Bytes)
	fmt.Printf(actualCIDRs2JSON)

	emptyGcpAliasIPs := []*compute.AliasIpRange{}

	testCases := []struct {
		name                          string
		initialNode                   *corev1.Node
		initialGCEInstance            *compute.Instance
		gceInstanceExistsInFakeCloud  bool // Controls if initialGCEInstance is added to fake cloud
		setupFakeGCEError             func(t *testing.T, fCloud *gce.Cloud)
		expectedActualCIDRsAnnotation string
		expectedGCEAliasIPsOnSync     []*compute.AliasIpRange
		expectedEvents                []string
		expectSyncError               bool
		skipGCEVerification           bool
	}{
		{
			name:                          "Node with desired-cidrs, no actual-cidrs, GCE needs update",
			initialNode:                   makeNode(testNodeName, testProviderID, desiredCIDRs1JSON, ""),
			initialGCEInstance:            makeFakeGCEInstanceObject(testInstanceName, testZone, emptyGcpAliasIPs),
			gceInstanceExistsInFakeCloud:  true,
			expectedActualCIDRsAnnotation: actualCIDRs1JSON,
			expectedGCEAliasIPsOnSync:     gcpAliasIPs1,
			expectedEvents:                []string{"UpdatingDynamicPodIPs", "DynamicPodIPsUpdated", "AnnotationUpdated"},
		},
		{
			name:                          "Node with desired-cidrs, matching actual-cidrs, GCE in sync",
			initialNode:                   makeNode(testNodeName, testProviderID, desiredCIDRs1JSON, actualCIDRs1JSON),
			initialGCEInstance:            makeFakeGCEInstanceObject(testInstanceName, testZone, gcpAliasIPs1),
			gceInstanceExistsInFakeCloud:  true,
			expectedActualCIDRsAnnotation: actualCIDRs1JSON,
			expectedGCEAliasIPsOnSync:     gcpAliasIPs1,
			// AnnotationUpdated might still fire if patch logic always tries, even if content is same.
			// If no change, patch should be empty, so no "AnnotationUpdated" event.
			// Let's assume no event if truly no change.
			expectedEvents: []string{},
		},
		{
			name:                          "Node with desired-cidrs, GCE out of sync",
			initialNode:                   makeNode(testNodeName, testProviderID, desiredCIDRs2JSON, desiredCIDRs1JSON), // Actual is old
			initialGCEInstance:            makeFakeGCEInstanceObject(testInstanceName, testZone, gcpAliasIPs1),          // GCE has old IPs
			gceInstanceExistsInFakeCloud:  true,
			expectedActualCIDRsAnnotation: actualCIDRs2JSON,
			expectedGCEAliasIPsOnSync:     gcpAliasIPs2,
			expectedEvents:                []string{"UpdatingDynamicPodIPs", "DynamicPodIPsUpdated", "AnnotationUpdated"},
		},
		{
			name:                          "Remove the excess alias IPs from GCE based on new desired-cidrs",
			initialNode:                   makeNode(testNodeName, testProviderID, desiredCIDRs1JSON, actualCIDRs2JSON), // Actual is old
			initialGCEInstance:            makeFakeGCEInstanceObject(testInstanceName, testZone, gcpAliasIPs2),         // GCE has old IPs
			gceInstanceExistsInFakeCloud:  true,
			expectedActualCIDRsAnnotation: actualCIDRs1JSON,
			expectedGCEAliasIPsOnSync:     gcpAliasIPs1,
			expectedEvents:                []string{"UpdatingDynamicPodIPs", "DynamicPodIPsUpdated", "AnnotationUpdated"},
		},
		{
			name:                          "Desired-cidrs annotation removed, GCE needs update to remove alias IPs",
			initialNode:                   makeNode(testNodeName, testProviderID, "", desiredCIDRs1JSON), // Desired is empty
			initialGCEInstance:            makeFakeGCEInstanceObject(testInstanceName, testZone, gcpAliasIPs1),
			gceInstanceExistsInFakeCloud:  true,
			expectedActualCIDRsAnnotation: "", // Actual should be removed
			expectedGCEAliasIPsOnSync:     emptyGcpAliasIPs,
			expectedEvents:                []string{"UpdatingDynamicPodIPs", "DynamicPodIPsUpdated", "AnnotationUpdated"},
		},
		{
			name:                          "Malformed desired-cidrs annotation",
			initialNode:                   makeNode(testNodeName, testProviderID, `[{"bad": "json"`, ""),
			initialGCEInstance:            makeFakeGCEInstanceObject(testInstanceName, testZone, emptyGcpAliasIPs),
			gceInstanceExistsInFakeCloud:  true,
			expectedActualCIDRsAnnotation: "", // No change to actual
			expectedGCEAliasIPsOnSync:     emptyGcpAliasIPs,
			expectedEvents:                []string{"InvalidDesiredCIDRs"},
			expectSyncError:               true,
		},
		{
			name:                          "Node with no ProviderID",
			initialNode:                   makeNode(testNodeName, "", desiredCIDRs1JSON, ""), // No ProviderID
			initialGCEInstance:            nil,                                               // GCE state irrelevant
			gceInstanceExistsInFakeCloud:  false,
			expectedActualCIDRsAnnotation: "",
			expectedGCEAliasIPsOnSync:     nil,
			expectedEvents:                []string{"MissingProviderID"},
			expectSyncError:               true,
			skipGCEVerification:           true,
		},
		{
			name:                          "GCE instance not found by ProviderID",
			initialNode:                   makeNode(testNodeName, testProviderID, desiredCIDRs1JSON, ""),
			initialGCEInstance:            nil, // No instance added to fake cloud
			gceInstanceExistsInFakeCloud:  false,
			expectedActualCIDRsAnnotation: "",
			expectedGCEAliasIPsOnSync:     nil,
			expectedEvents:                []string{"CIDRNotAvailable"}, // Event from nodeutil.RecordNodeStatusChange
			expectSyncError:               true,
			skipGCEVerification:           true,
		},
		{
			name:                          "Primary NIC not found on GCE instance",
			initialNode:                   makeNode(testNodeName, testProviderID, desiredCIDRs1JSON, ""),
			initialGCEInstance:            makeFakeGCEInstanceObjectWithNICs(testInstanceName, testZone, []*compute.NetworkInterface{{Name: "other-nic"}}),
			gceInstanceExistsInFakeCloud:  true,
			expectedActualCIDRsAnnotation: "",
			expectedGCEAliasIPsOnSync:     nil, // Or verify "other-nic" is untouched
			expectedEvents:                []string{"NICNotFound"},
			expectSyncError:               true,
			skipGCEVerification:           true, // GCE state for primary NIC is not applicable
		},
		//		{
		//			name:                         "GCE UpdateAliasIPs (SetAliasIpRanges) fails",
		//			initialNode:                  makeNode(testNodeName, testProviderID, desiredCIDRs1JSON, ""),
		//			initialGCEInstance:           makeFakeGCEInstanceObject(testInstanceName, testZone, emptyGcpAliasIPs),
		//			gceInstanceExistsInFakeCloud: true,
		//			setupFakeGCEError: func(t *testing.T, fCloud *gce.Cloud) {
		//				/*
		//					fakeCompute, ok := fCloud.Compute().(*gce.FakeComputeService)
		//					if !ok {
		//						t.Fatalf("Could not get FakeComputeService from fake GCE cloud for error injection")
		//					}
		//					fakeCompute.SetSetAliasIpRangesErr(fmt.Errorf("simulated GCE SetAliasIpRanges error"))
		//				*/
		//			},
		//			expectedActualCIDRsAnnotation: "",               // No update to actual
		//			expectedGCEAliasIPsOnSync:     emptyGcpAliasIPs, // GCE state should remain unchanged
		//			expectedEvents:                []string{"UpdatingDynamicPodIPs", "UpdateInstanceFailed"},
		//			expectSyncError:               true,
		//			// skipGCEVerification: false, // We want to verify GCE state didn't change
		//		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testEnv := newTestEnv(t, tc.initialNode)

			if tc.gceInstanceExistsInFakeCloud && tc.initialGCEInstance != nil {
				inst := tc.initialGCEInstance
				err := testEnv.gceCloud.Compute().Instances().Insert(context.Background(), meta.ZonalKey(inst.Name, testZone), inst)
				if err != nil {
					t.Fatalf("Failed to insert GCE instance into fake cloud: %v", err)
				}
			}

			if tc.setupFakeGCEError != nil {
				tc.setupFakeGCEError(t, testEnv.gceCloud)
			}

			err := testEnv.runSync(tc.initialNode.Name)

			if tc.expectSyncError {
				assert.Error(t, err, "Expected an error from syncHandler")
			} else {
				assert.NoError(t, err, "Expected no error from syncHandler")
			}

			testEnv.verifyNodeAnnotations(tc.initialNode.Name, tc.expectedActualCIDRsAnnotation)
			if !tc.skipGCEVerification {
				testEnv.verifyGCEAliasIPs(tc.initialNode.Spec.ProviderID, tc.expectedGCEAliasIPsOnSync)
			}
			testEnv.verifyEvents(tc.expectedEvents)
		})
	}
}

type testEnv struct {
	t *testing.T

	kubeClient *fake.Clientset
	gceCloud   *gce.Cloud // This will be the fake GCE cloud

	nodeLister   listersv1.NodeLister
	fakeRecorder *record.FakeRecorder

	controller      *DynamicPodIPController
	ipAllocateIndex int
}

func newTestEnv(t *testing.T, initialObjects ...runtime.Object) *testEnv {
	kubeClient := fake.NewSimpleClientset(initialObjects...)

	testClusterValues := gce.DefaultTestClusterValues()
	// testClusterValues.SubnetworkURL = exampleSubnetURL
	gceCloud := gce.NewFakeGCECloud(testClusterValues)
	mockGce, ok := gceCloud.Compute().(*cloud.MockGCE)
	if !ok {
		t.Fatalf("Can not get the mock GCE cloud")
	}

	informerFactory := informers.NewSharedInformerFactory(kubeClient, 0) // No resync
	nodeInformer := informerFactory.Core().V1().Nodes()

	for _, obj := range initialObjects {
		if node, ok := obj.(*corev1.Node); ok {
			err := nodeInformer.Informer().GetStore().Add(node)
			if err != nil {
				t.Fatalf("Failed to add node to informer store: %v", err)
			}
		}
	}

	fakeRecorder := record.NewFakeRecorder(100)

	// The controller expects *gce.Cloud, which our fakeGCECloud is.
	// The NewDynamicPodIPController takes cloudprovider.Interface, but then casts it.
	// For testing, we pass the concrete *gce.Cloud fake directly.
	controller, err := NewDynamicPodIPController(kubeClient, nodeInformer, gceCloud)
	if err != nil {
		t.Fatalf("Failed to create DynamicPodIPController: %v", err)
	}
	controller.recorder = fakeRecorder // Inject fake recorder

	f := &testEnv{
		t:            t,
		kubeClient:   kubeClient,
		gceCloud:     gceCloud,
		nodeLister:   nodeInformer.Lister(),
		fakeRecorder: fakeRecorder,
		controller:   controller,
	}

	mockGce.MockBetaInstances.UpdateNetworkInterfaceHook = func(ctx context.Context, key *meta.Key, nicName string, networkInterface *computebeta.NetworkInterface, instances *cloud.MockBetaInstances, option ...cloud.Option) error {
		inst, err := instances.Get(ctx, key)
		if err != nil {
			return fmt.Errorf("Failed to get instance when updating network interface: %s: %v", key, err)
		}

		for _, alias := range networkInterface.AliasIpRanges {
			if prefixOnly, _ := regexp.MatchString("^/\\d+$", alias.IpCidrRange); prefixOnly {
				prefix, err := strconv.Atoi(alias.IpCidrRange[1:])
				if err != nil {
					log.Fatal("Bad alias IP format: %s, should be /<prefix>", alias.IpCidrRange)
				}
				alias.IpCidrRange = f.allocateIP(prefix)
			}
		}
		for _, nic := range inst.NetworkInterfaces {
			if nic.Name == nicName {
				nic.AliasIpRanges = networkInterface.AliasIpRanges
			}
			instances.Objects[*key] = &cloud.MockInstancesObj{inst}
			return nil
		}
		return fmt.Errorf("The network interface %s not found in instance %s", nicName, key)
	}
	return f
}

func (f *testEnv) runSync(nodeName string) error {
	return f.controller.syncHandler(context.Background(), nodeName)
}

func (s *testEnv) allocateIP(prefix int) string {
	if prefix < 24 {
		log.Fatal("Only the prefix >= 24 is supported.")
	}
	s.ipAllocateIndex += 1
	return fmt.Sprintf("15.%d.%d.0/%d", s.ipAllocateIndex/256, s.ipAllocateIndex%256, prefix)
}

func (f *testEnv) verifyNodeAnnotations(nodeName string, expectedActualCIDRsAnnotation string) {
	f.t.Helper()
	node, err := f.kubeClient.CoreV1().Nodes().Get(context.Background(), nodeName, metav1.GetOptions{})
	if err != nil {
		f.t.Fatalf("Failed to get node %s: %v", nodeName, err)
	}

	actual, exists := node.Annotations[ActualCIDRsAnnotation]
	if expectedActualCIDRsAnnotation == "" {
		if exists {
			f.t.Errorf("Node %s: expected ActualCIDRsAnnotation to be absent, but got '%s'", nodeName, actual)
		}
	} else {
		if !exists {
			f.t.Errorf("Node %s: expected ActualCIDRsAnnotation to exist with value '%s', but it was absent", nodeName, expectedActualCIDRsAnnotation)
		} else if !jsonStringsEqual(f.t, actual, expectedActualCIDRsAnnotation) {
			f.t.Errorf("Node %s: ActualCIDRsAnnotation mismatch.\nExpected: %s\nActual:   %s", nodeName, expectedActualCIDRsAnnotation, actual)
		}
	}
}

func (f *testEnv) verifyGCEAliasIPs(instanceProviderID string, expectedAliasIPs []*compute.AliasIpRange) {
	f.t.Helper()
	if instanceProviderID == "" { // Cannot verify if providerID is not set
		if len(expectedAliasIPs) > 0 {
			f.t.Errorf("Instance providerID is empty, but expected GCE alias IPs: %+v", expectedAliasIPs)
		}
		return
	}

	instance, err := f.gceCloud.InstanceByProviderID(instanceProviderID)
	if err != nil {
		//if gce.IsNotFound(err) {
		if len(expectedAliasIPs) > 0 { // Instance not found, but we expected IPs
			f.t.Errorf("Instance %s not found in fake GCE, but expected alias IPs: %+v", instanceProviderID, expectedAliasIPs)
		}
		// return // Instance not found, and no IPs expected, this is fine.
		// }
		f.t.Fatalf("Failed to get instance %s from fake GCE: %v", instanceProviderID, err)
	}

	var currentNIC *compute.NetworkInterface
	for _, nic := range instance.NetworkInterfaces {
		if nic.Name == testPrimaryNICName {
			currentNIC = nic
			break
		}
	}

	if currentNIC == nil {
		if len(expectedAliasIPs) > 0 {
			f.t.Errorf("Instance %s: primary NIC '%s' not found, but expected alias IPs: %+v", instanceProviderID, testPrimaryNICName, expectedAliasIPs)
		}
		return // NIC not found, if no IPs expected, this is fine.
	}

	actualAliasIPs := currentNIC.AliasIpRanges
	// Use the controller's own comparison logic if it's robust
	if !areDynamicPodIPRangesEqual(actualAliasIPs, expectedAliasIPs) {
		f.t.Errorf("Instance %s: GCE Alias IP ranges mismatch.\nExpected: %+v\nActual:   %+v", instanceProviderID, expectedAliasIPs, actualAliasIPs)
	}
}

func (f *testEnv) verifyEvents(expectedEventSubstrings []string) {
	f.t.Helper()
	// Ensure channel is closed if no events are expected, or after sync
	// For simplicity in test, close it before reading.
	// In a real concurrent scenario, you'd wait or use select.
	close(f.fakeRecorder.Events)

	var actualEvents []string
	for event := range f.fakeRecorder.Events {
		actualEvents = append(actualEvents, event)
	}

	if len(actualEvents) != len(expectedEventSubstrings) {
		f.t.Errorf("Event count mismatch. Expected %d, Got %d.\nExpected: %v\nActual:   %v",
			len(expectedEventSubstrings), len(actualEvents), expectedEventSubstrings, actualEvents)
		// To aid debugging, print actual events even if count mismatches
		if len(actualEvents) != len(expectedEventSubstrings) && len(actualEvents) > 0 {
			f.t.Logf("Actual events recorded: %v", actualEvents)
		}
		return
	}

	for i, expectedSubstring := range expectedEventSubstrings {
		if !strings.Contains(actualEvents[i], expectedSubstring) {
			f.t.Errorf("Event %d mismatch. Expected substring '%s' in '%s'", i, expectedSubstring, actualEvents[i])
		}
	}
}

// jsonStringsEqual compares two JSON strings for semantic equality.
func jsonStringsEqual(t *testing.T, s1, s2 string) bool {
	t.Helper()
	var o1, o2 interface{}
	if err := json.Unmarshal([]byte(s1), &o1); err != nil {
		t.Errorf("Error unmarshalling s1: %s, err: %v", s1, err)
		return false
	}
	if err := json.Unmarshal([]byte(s2), &o2); err != nil {
		t.Errorf("Error unmarshalling s2: %s, err: %v", s2, err)
		return false
	}
	return assert.ObjectsAreEqualValues(o1, o2)
}

func makeNode(name, providerID string, desiredAnnotationJSON string, actualAnnotationJSON string) *corev1.Node {
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Annotations: make(map[string]string),
		},
		Spec: corev1.NodeSpec{
			ProviderID: providerID,
		},
	}
	if desiredAnnotationJSON != "" {
		node.Annotations[DesiredCIDRsAnnotation] = desiredAnnotationJSON
	}
	if actualAnnotationJSON != "" {
		node.Annotations[ActualCIDRsAnnotation] = actualAnnotationJSON
	}
	return node
}

// makeFakeGCEInstanceObject creates a *compute.Instance suitable for gce.FakeCloud.Instances.Insert()
func makeFakeGCEInstanceObject(name, zone string, aliasIPs []*compute.AliasIpRange) *compute.Instance {
	return &compute.Instance{
		Name: name,
		Zone: zone, // Short zone name
		NetworkInterfaces: []*compute.NetworkInterface{
			{
				Name:          testPrimaryNICName,
				AliasIpRanges: aliasIPs,
			},
		},
		Status: "RUNNING",
		// ProjectId: testProjectID, // Not strictly needed for FakeCloud.Instances.Get by name
	}
}

func makeFakeGCEInstanceObjectWithNICs(name, zone string, nics []*compute.NetworkInterface) *compute.Instance {
	return &compute.Instance{
		Name:              name,
		Zone:              zone,
		NetworkInterfaces: nics,
		Status:            "RUNNING",
	}
}
