package reconciler

import (
	"context"
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"

	"kushd/pkg/apis/kushd/v1alpha1"
)

const managerBoot = "11111111-1111-4111-8111-111111111111"
const workerBoot = "22222222-2222-4222-8222-222222222222"
const previousBoot = "33333333-3333-4333-8333-333333333333"

type recordingManagerPower struct{ calls int }

func (p *recordingManagerPower) HaltHost(context.Context) { p.calls++ }

func safetyNode(name, id string) *corev1.Node {
	n := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, Annotations: map[string]string{}},
		Status: corev1.NodeStatus{NodeInfo: corev1.NodeSystemInfo{BootID: id}}}
	if name == "cp-anchor" {
		n.Labels = map[string]string{v1alpha1.LabelNodeRoleControlPlane: ""}
	}
	return n
}

func safetyScenario(t *testing.T, phase v1alpha1.ClusterShutdownPhase, nodes ...*corev1.Node) (*ClusterReconciler, *fake.Clientset, *dynamicfake.FakeDynamicClient, *recordingManagerPower, *mockUPSClient) {
	t.Helper()
	objects := make([]runtime.Object, 0, len(nodes))
	bindings := make(map[string]interface{}, len(nodes))
	for _, node := range nodes {
		objects = append(objects, node)
		bindings[node.Name] = node.Status.NodeInfo.BootID
	}
	client := fake.NewSimpleClientset(objects...)
	csd := createTestCSD("safety-job", phase, false)
	csd.Object["status"].(map[string]interface{})["nodeBootIDs"] = bindings
	dyn := dynamicfake.NewSimpleDynamicClient(newTestScheme(), csd)
	power := &recordingManagerPower{}
	ups := &mockUPSClient{}
	r := NewClusterReconciler(client, dyn, ups, ReconcilerConfig{AnchorNode: "cp-anchor", HostBootID: managerBoot, PowerController: power})
	return r, client, dyn, power, ups
}

func safetyRequest(t *testing.T, dyn *dynamicfake.FakeDynamicClient) *unstructured.Unstructured {
	t.Helper()
	obj, err := dyn.Resource(v1alpha1.ClusterShutdownGVR).Get(context.Background(), "safety-job", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return obj
}

func saveSafetyStatus(t *testing.T, dyn *dynamicfake.FakeDynamicClient, obj *unstructured.Unstructured) {
	t.Helper()
	if _, err := dyn.Resource(v1alpha1.ClusterShutdownGVR).UpdateStatus(context.Background(), obj, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
}

func TestShutdownPersistsBootBindingBeforeAnyNodeMutation(t *testing.T) {
	ctx := context.Background()
	r, client, dyn, _, _ := safetyScenario(t, v1alpha1.PhasePending, safetyNode("cp-anchor", managerBoot), safetyNode("worker", workerBoot))
	obj := safetyRequest(t, dyn)
	obj.Object["status"] = map[string]interface{}{}
	saveSafetyStatus(t, dyn, obj)
	if err := r.reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	status := safetyRequest(t, dyn).Object["status"].(map[string]interface{})
	if status["phase"] != string(v1alpha1.PhasePending) || status["anchorBootID"] != managerBoot {
		t.Fatalf("boot binding not persisted before progress: %v", status)
	}
	node, _ := client.CoreV1().Nodes().Get(ctx, "worker", metav1.GetOptions{})
	if node.Spec.Unschedulable || node.Annotations[v1alpha1.AnnotationStage] != "" {
		t.Fatal("node changed before durable boot authorization")
	}
	for range 3 {
		if err := r.reconcile(ctx); err != nil {
			t.Fatal(err)
		}
	}
	node, _ = client.CoreV1().Nodes().Get(ctx, "worker", metav1.GetOptions{})
	if node.Annotations[v1alpha1.AnnotationDirectiveBootID] != workerBoot || node.Annotations[v1alpha1.AnnotationShutdownUID] != "safety-job" || node.Annotations[v1alpha1.AnnotationStage] != v1alpha1.StageDrain {
		t.Fatalf("directive is not bound to the captured job and boot: %v", node.Annotations)
	}
}

func TestFailedBootBindingPersistenceCannotCordonNodes(t *testing.T) {
	r, client, dyn, power, ups := safetyScenario(t, v1alpha1.PhasePending, safetyNode("cp-anchor", managerBoot))
	obj := safetyRequest(t, dyn)
	delete(obj.Object["status"].(map[string]interface{}), "anchorBootID")
	saveSafetyStatus(t, dyn, obj)
	dyn.PrependReactor("update", "clustershutdowns", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, fmt.Errorf("status persistence unavailable")
	})
	if err := r.reconcile(context.Background()); err == nil {
		t.Fatal("failed status persistence was ignored")
	}
	node, _ := client.CoreV1().Nodes().Get(context.Background(), "cp-anchor", metav1.GetOptions{})
	if node.Spec.Unschedulable || power.calls != 0 || ups.killpowerCalled {
		t.Fatal("shutdown acted without durable boot binding")
	}
}

func TestOldOrUnboundShutdownCannotResumePowerActions(t *testing.T) {
	for _, mode := range []string{"anchor reboot", "worker reboot", "legacy unbound"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			worker := safetyNode("worker", workerBoot)
			worker.Spec.Unschedulable = true
			worker.Annotations = map[string]string{v1alpha1.AnnotationCordonOwner: "safety-job", v1alpha1.AnnotationShutdownUID: "safety-job", v1alpha1.AnnotationStage: v1alpha1.StageHalt, v1alpha1.AnnotationDirectiveBootID: workerBoot}
			admin := safetyNode("administrator-cordoned", previousBoot)
			admin.Spec.Unschedulable = true
			r, client, dyn, power, ups := safetyScenario(t, v1alpha1.PhaseFinalizing, safetyNode("cp-anchor", managerBoot), worker, admin)
			obj := safetyRequest(t, dyn)
			status := obj.Object["status"].(map[string]interface{})
			switch mode {
			case "anchor reboot":
				status["anchorBootID"] = previousBoot
			case "worker reboot":
				status["nodeBootIDs"].(map[string]interface{})["worker"] = previousBoot
			case "legacy unbound":
				delete(status, "anchorBootID")
				delete(status, "nodeBootIDs")
			}
			saveSafetyStatus(t, dyn, obj)
			if err := r.reconcile(ctx); err != nil {
				t.Fatal(err)
			}
			if power.calls != 0 || ups.killpowerCalled {
				t.Fatal("stale shutdown invoked host or UPS power control")
			}
			if safetyRequest(t, dyn).Object["status"].(map[string]interface{})["phase"] != string(v1alpha1.PhaseAborted) {
				t.Fatal("stale shutdown remained active")
			}
			updated, _ := client.CoreV1().Nodes().Get(ctx, "worker", metav1.GetOptions{})
			if updated.Spec.Unschedulable || updated.Annotations[v1alpha1.AnnotationStage] != "" {
				t.Fatal("owned shutdown state survived retirement")
			}
			untouched, _ := client.CoreV1().Nodes().Get(ctx, admin.Name, metav1.GetOptions{})
			if !untouched.Spec.Unschedulable {
				t.Fatal("recovery removed an administrator's cordon")
			}
		})
	}
}

func TestSameBootManagerRestartCanFinishAuthorizedShutdown(t *testing.T) {
	r, _, dyn, power, ups := safetyScenario(t, v1alpha1.PhaseFinalizing, safetyNode("cp-anchor", managerBoot))
	if err := r.reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if power.calls != 1 || !ups.killpowerCalled {
		t.Fatal("same-boot shutdown no longer reaches its power controllers")
	}
	if safetyRequest(t, dyn).Object["status"].(map[string]interface{})["phase"] != string(v1alpha1.PhaseCompleted) {
		t.Fatal("authorized shutdown did not complete")
	}
}

func TestPreviousBootDrainAcknowledgementCannotAdvanceShutdown(t *testing.T) {
	worker := safetyNode("worker", workerBoot)
	worker.Annotations = map[string]string{
		v1alpha1.AnnotationStage:           v1alpha1.StageDrain,
		v1alpha1.AnnotationDirectiveBootID: workerBoot,
		v1alpha1.AnnotationShutdownUID:     "safety-job",
		v1alpha1.AnnotationAgentStatus:     v1alpha1.AgentStatusDrained,
		v1alpha1.AnnotationAgentBootID:     previousBoot,
	}
	r, _, dyn, power, ups := safetyScenario(t, v1alpha1.PhaseDrainingWorkers, safetyNode("cp-anchor", managerBoot), worker)
	if err := r.reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if safetyRequest(t, dyn).Object["status"].(map[string]interface{})["phase"] != string(v1alpha1.PhaseDrainingWorkers) || power.calls != 0 || ups.killpowerCalled {
		t.Fatal("stale drain acknowledgement advanced shutdown")
	}
}

func TestStartupRecoveryPreservesUnrelatedCordons(t *testing.T) {
	admin := safetyNode("administrator-cordoned", previousBoot)
	admin.Spec.Unschedulable = true
	r, client, dyn, power, ups := safetyScenario(t, v1alpha1.PhaseFinalizing, safetyNode("cp-anchor", managerBoot), admin)
	obj := safetyRequest(t, dyn)
	obj.Object["status"].(map[string]interface{})["anchorBootID"] = previousBoot
	saveSafetyStatus(t, dyn, obj)
	if err := r.RecoverClusterOnStartup(context.Background()); err != nil {
		t.Fatal(err)
	}
	if power.calls != 0 || ups.killpowerCalled {
		t.Fatal("startup recovery invoked power control")
	}
	if safetyRequest(t, dyn).Object["status"].(map[string]interface{})["phase"] != string(v1alpha1.PhaseAborted) {
		t.Fatal("startup retained an old-boot finalizer")
	}
	updated, _ := client.CoreV1().Nodes().Get(context.Background(), admin.Name, metav1.GetOptions{})
	if !updated.Spec.Unschedulable {
		t.Fatal("unrelated cordon was removed on startup")
	}
}

func TestStartupRejectsUnboundQueuedShutdown(t *testing.T) {
	for _, phase := range []v1alpha1.ClusterShutdownPhase{"", v1alpha1.PhasePending} {
		t.Run(fmt.Sprintf("phase=%q", phase), func(t *testing.T) {
			r, _, dyn, power, ups := safetyScenario(t, phase, safetyNode("cp-anchor", managerBoot))
			obj := safetyRequest(t, dyn)
			obj.Object["status"] = map[string]interface{}{"phase": string(phase)}
			saveSafetyStatus(t, dyn, obj)
			if err := r.RecoverClusterOnStartup(context.Background()); err != nil {
				t.Fatal(err)
			}
			status := safetyRequest(t, dyn).Object["status"].(map[string]interface{})
			if status["phase"] != string(v1alpha1.PhaseAborted) || power.calls != 0 || ups.killpowerCalled {
				t.Fatalf("startup replayed an unbound queued shutdown: %v", status)
			}
		})
	}
}
