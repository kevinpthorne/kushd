package reconciler

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"

	"kushd/pkg/apis/kushd/v1alpha1"
	"kushd/pkg/manager/ups"
)

type mockUPSClient struct {
	status           *ups.UPSStatus
	killpowerCalled  bool
	killpowerDelay   int
	killpowerOutlet  string
}

func (m *mockUPSClient) GetStatus(ctx context.Context) (*ups.UPSStatus, error) {
	if m.status != nil {
		return m.status, nil
	}
	return &ups.UPSStatus{State: "OL", BatteryChargePercent: 100, EstimatedRuntimeSeconds: 3600}, nil
}

func (m *mockUPSClient) Killpower(ctx context.Context, delaySeconds int, targetOutletGroup string) error {
	m.killpowerCalled = true
	m.killpowerDelay = delaySeconds
	m.killpowerOutlet = targetOutletGroup
	return nil
}

func newTestScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = v1alpha1.SchemeGroupVersion
	return s
}

func createTestCSD(name string, phase v1alpha1.ClusterShutdownPhase, abort bool) *unstructured.Unstructured {
	now := metav1.Now()
	csd := &v1alpha1.ClusterShutdown{
		TypeMeta: metav1.TypeMeta{
			APIVersion: fmt.Sprintf("%s/%s", v1alpha1.GroupName, v1alpha1.Version),
			Kind:       "ClusterShutdown",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: v1alpha1.ClusterShutdownSpec{
			TriggerSource:           "test-ups",
			BatteryChargePercent:    10,
			EstimatedRuntimeSeconds: 120,
			Abort:                   abort,
			DryRun:                  false,
			PowerManagement: v1alpha1.PowerManagementSpec{
				EnableKillpower:       true,
				KillpowerDelaySeconds: 60, // Deliberately < 180s to test safety clamp
			},
		},
		Status: v1alpha1.ClusterShutdownStatus{
			Phase:      phase,
			StartTime:  &now,
			AnchorNode: "cp-anchor",
		},
	}

	data, _ := json.Marshal(csd)
	var u unstructured.Unstructured
	_ = json.Unmarshal(data, &u.Object)
	return &u
}

func TestReconciler_AbortBeforePointOfNoReturn(t *testing.T) {
	ctx := context.Background()
	scheme := newTestScheme()

	// Node in cluster
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "worker-1",
		},
		Spec: corev1.NodeSpec{
			Unschedulable: true,
		},
	}
	client := fake.NewSimpleClientset(node)

	csdObj := createTestCSD("csd-abort-cordon", v1alpha1.PhaseCordoningCluster, true)
	dynClient := dynamicfake.NewSimpleDynamicClient(scheme, csdObj)

	reconciler := NewClusterReconciler(client, dynClient, &mockUPSClient{}, ReconcilerConfig{
		AnchorNode: "cp-anchor",
	})

	err := reconciler.reconcile(ctx)
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}

	// Verify phase transitioned to Aborted
	updatedCSD, err := dynClient.Resource(v1alpha1.ClusterShutdownGVR).Get(ctx, "csd-abort-cordon", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to fetch CSD: %v", err)
	}
	status := updatedCSD.Object["status"].(map[string]interface{})
	if status["phase"] != string(v1alpha1.PhaseAborted) {
		t.Fatalf("expected phase Aborted, got %v", status["phase"])
	}

	// Verify node uncordoned
	updatedNode, _ := client.CoreV1().Nodes().Get(ctx, "worker-1", metav1.GetOptions{})
	if updatedNode.Spec.Unschedulable {
		t.Fatalf("expected node to be uncordoned on abort")
	}
}

func TestReconciler_PointOfNoReturnAbortRejection(t *testing.T) {
	ctx := context.Background()
	scheme := newTestScheme()

	// Worker node already reporting drained
	worker := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "worker-1",
			Annotations: map[string]string{
				v1alpha1.AnnotationStage:       v1alpha1.StageDrain,
				v1alpha1.AnnotationAgentStatus: v1alpha1.AgentStatusDrained,
			},
		},
	}
	client := fake.NewSimpleClientset(worker)

	// CSD in DrainingWorkers with abort requested
	csdObj := createTestCSD("csd-point-of-no-return", v1alpha1.PhaseDrainingWorkers, true)
	dynClient := dynamicfake.NewSimpleDynamicClient(scheme, csdObj)

	reconciler := NewClusterReconciler(client, dynClient, &mockUPSClient{}, ReconcilerConfig{
		AnchorNode: "cp-anchor",
	})

	err := reconciler.reconcile(ctx)
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}

	updatedCSD, err := dynClient.Resource(v1alpha1.ClusterShutdownGVR).Get(ctx, "csd-point-of-no-return", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to fetch CSD: %v", err)
	}

	status := updatedCSD.Object["status"].(map[string]interface{})
	// Must NOT be Aborted, should proceed to HaltingWorkers because workers are drained!
	if status["phase"] != string(v1alpha1.PhaseHaltingWorkers) {
		t.Fatalf("expected phase HaltingWorkers (abort rejected), got %v", status["phase"])
	}

	// Verify AbortRejected condition was recorded
	conditions, ok := status["conditions"].([]interface{})
	if !ok || len(conditions) == 0 {
		t.Fatalf("expected conditions to contain AbortRejected condition")
	}
	cond0 := conditions[0].(map[string]interface{})
	if cond0["type"] != v1alpha1.ConditionTypeAbortRejected {
		t.Fatalf("expected condition type %s, got %v", v1alpha1.ConditionTypeAbortRejected, cond0["type"])
	}
	if cond0["reason"] != v1alpha1.ConditionReasonPointOfNoReturn {
		t.Fatalf("expected condition reason %s, got %v", v1alpha1.ConditionReasonPointOfNoReturn, cond0["reason"])
	}

	// Verify Kubernetes Warning event was emitted
	events, err := client.CoreV1().Events(metav1.NamespaceDefault).List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("failed to list events: %v", err)
	}
	if len(events.Items) == 0 {
		t.Fatalf("expected warning event to be emitted")
	}
	if events.Items[0].Reason != v1alpha1.ConditionTypeAbortRejected {
		t.Fatalf("expected event reason AbortRejected, got %s", events.Items[0].Reason)
	}
}

func TestReconciler_KillpowerClampingAndFinalizing(t *testing.T) {
	ctx := context.Background()
	scheme := newTestScheme()

	client := fake.NewSimpleClientset()
	mockUPS := &mockUPSClient{}

	csdObj := createTestCSD("csd-finalizing", v1alpha1.PhaseFinalizing, false)
	dynClient := dynamicfake.NewSimpleDynamicClient(scheme, csdObj)

	reconciler := NewClusterReconciler(client, dynClient, mockUPS, ReconcilerConfig{
		AnchorNode: "cp-anchor",
	})

	err := reconciler.reconcile(ctx)
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}

	// Verify killpower called and delay was clamped to >= 180 seconds
	if !mockUPS.killpowerCalled {
		t.Fatalf("expected killpower to be called")
	}
	if mockUPS.killpowerDelay < 180 {
		t.Fatalf("expected killpower delay to be clamped to at least 180s, got %d", mockUPS.killpowerDelay)
	}

	// Verify phase is Completed
	updatedCSD, _ := dynClient.Resource(v1alpha1.ClusterShutdownGVR).Get(ctx, "csd-finalizing", metav1.GetOptions{})
	status := updatedCSD.Object["status"].(map[string]interface{})
	if status["phase"] != string(v1alpha1.PhaseCompleted) {
		t.Fatalf("expected phase Completed, got %v", status["phase"])
	}
}
