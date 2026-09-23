package agent

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"kushd/pkg/agent/drain"
	"kushd/pkg/agent/power"
	"kushd/pkg/apis/kushd/v1alpha1"
)

func TestAgent_Reconciliation(t *testing.T) {
	ctx := context.Background()
	drain.PollInterval = 10 * time.Millisecond

	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "worker-1",
			Annotations: map[string]string{
				v1alpha1.AnnotationStage: v1alpha1.StageDrain,
			},
		},
	}

	client := fake.NewSimpleClientset(node)

	tempDir := t.TempDir()
	powerOpts := power.ControllerOptions{
		DBusSocketPath:   filepath.Join(tempDir, "dbus.sock"),
		HostRootPath:     tempDir,
		SysrqTriggerPath: filepath.Join(tempDir, "sysrq"),
		SyncTimeout:      10 * time.Millisecond,
	}
	powerController, err := power.NewHostPowerController(powerOpts)
	if err != nil {
		t.Fatalf("failed to create power controller: %v", err)
	}
	defer powerController.Close()

	agentConfig := AgentConfig{
		NodeName:            "worker-1",
		PollInterval:        50 * time.Millisecond,
		PowerControllerOpts: powerOpts,
	}

	kushdAgent := NewAgent(client, agentConfig, powerController)

	// Run single reconcile step
	if err := kushdAgent.reconcile(ctx); err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}

	// Verify node annotation agent-status was updated to "drained"
	updatedNode, err := client.CoreV1().Nodes().Get(ctx, "worker-1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to fetch updated node: %v", err)
	}

	if updatedNode.Annotations[v1alpha1.AnnotationAgentStatus] != v1alpha1.AgentStatusDrained {
		t.Fatalf("expected agent-status to be 'drained', got %q", updatedNode.Annotations[v1alpha1.AnnotationAgentStatus])
	}
}
