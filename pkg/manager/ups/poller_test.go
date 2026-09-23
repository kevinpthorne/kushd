package ups

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"

	"kushd/pkg/apis/kushd/v1alpha1"
)

type mockPollerUPSClient struct {
	status *UPSStatus
}

func (m *mockPollerUPSClient) GetStatus(ctx context.Context) (*UPSStatus, error) {
	return m.status, nil
}

func (m *mockPollerUPSClient) Killpower(ctx context.Context, delaySeconds int, targetOutletGroup string) error {
	return nil
}

func TestHardwarePoller_TriggerOnLowBattery(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	dynClient := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, map[schema.GroupVersionResource]string{
		v1alpha1.ClusterShutdownGVR: "ClusterShutdownList",
	})

	mockUPS := &mockPollerUPSClient{
		status: &UPSStatus{
			State:                   "LB OB",
			BatteryChargePercent:    15,
			EstimatedRuntimeSeconds: 180,
		},
	}

	config := PollerConfig{
		AnchorNode:            "cp-node-1",
		TriggerSourceName:     "UPS-Test",
		BatteryLowPercent:     20,
		RuntimeLowSeconds:     300,
		EnableKillpower:       false,
		KillpowerDelaySeconds: 180,
	}

	poller := NewHardwarePoller(dynClient, mockUPS, config)

	err := poller.pollOnce(ctx)
	if err != nil {
		t.Fatalf("pollOnce failed: %v", err)
	}

	list, err := dynClient.Resource(v1alpha1.ClusterShutdownGVR).List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("failed to list CSDs: %v", err)
	}

	if len(list.Items) != 1 {
		t.Fatalf("expected 1 ClusterShutdown CR created, got %d", len(list.Items))
	}

	data, _ := list.Items[0].MarshalJSON()
	var csd v1alpha1.ClusterShutdown
	_ = json.Unmarshal(data, &csd)

	if csd.Status.Phase != v1alpha1.PhasePending {
		t.Fatalf("expected initial phase Pending, got %s", csd.Status.Phase)
	}
	if csd.Spec.BatteryChargePercent != 15 {
		t.Fatalf("expected battery percent 15, got %d", csd.Spec.BatteryChargePercent)
	}
}

func TestHardwarePoller_MainsRestoredAbort(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()

	// Existing CSD in Pending phase
	csd := &v1alpha1.ClusterShutdown{
		TypeMeta: metav1.TypeMeta{
			APIVersion: fmt.Sprintf("%s/%s", v1alpha1.GroupName, v1alpha1.Version),
			Kind:       "ClusterShutdown",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "ups-event-pending",
		},
		Spec: v1alpha1.ClusterShutdownSpec{
			TriggerSource: "UPS-Test",
			Abort:         false,
		},
		Status: v1alpha1.ClusterShutdownStatus{
			Phase: v1alpha1.PhasePending,
		},
	}
	data, _ := json.Marshal(csd)
	var u unstructured.Unstructured
	_ = json.Unmarshal(data, &u.Object)

	dynClient := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, map[schema.GroupVersionResource]string{
		v1alpha1.ClusterShutdownGVR: "ClusterShutdownList",
	}, &u)

	// UPS is now Online (OL)
	mockUPS := &mockPollerUPSClient{
		status: &UPSStatus{
			State:                   "OL",
			BatteryChargePercent:    95,
			EstimatedRuntimeSeconds: 3000,
		},
	}

	config := PollerConfig{
		AnchorNode: "cp-node-1",
	}

	poller := NewHardwarePoller(dynClient, mockUPS, config)

	err := poller.pollOnce(ctx)
	if err != nil {
		t.Fatalf("pollOnce failed: %v", err)
	}

	item, err := dynClient.Resource(v1alpha1.ClusterShutdownGVR).Get(ctx, "ups-event-pending", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to get item: %v", err)
	}

	spec := item.Object["spec"].(map[string]interface{})
	if spec["abort"] != true {
		t.Fatalf("expected spec.abort to be set to true upon mains restoration")
	}
}
