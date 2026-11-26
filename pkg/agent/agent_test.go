package agent

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"kushd/pkg/apis/kushd/v1alpha1"
)

func TestAgent_Reconciliation(t *testing.T) {
	ctx := context.Background()
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "worker-1",
			Annotations: map[string]string{
				v1alpha1.AnnotationStage:           v1alpha1.StageDrain,
				v1alpha1.AnnotationDirectiveBootID: agentBoot,
				v1alpha1.AnnotationShutdownUID:     "shutdown",
			},
		},
	}
	client := fake.NewSimpleClientset(node)
	kushdAgent := NewAgent(client, AgentConfig{NodeName: "worker-1", HostBootID: agentBoot}, &recordingPowerController{})
	if err := kushdAgent.reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	updatedNode, err := client.CoreV1().Nodes().Get(ctx, "worker-1", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if updatedNode.Annotations[v1alpha1.AnnotationAgentStatus] != v1alpha1.AgentStatusDrained ||
		updatedNode.Annotations[v1alpha1.AnnotationAgentBootID] != agentBoot {
		t.Fatalf("expected boot-scoped drained acknowledgement, got %v", updatedNode.Annotations)
	}
}
