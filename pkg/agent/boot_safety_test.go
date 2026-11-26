package agent

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"

	"kushd/pkg/apis/kushd/v1alpha1"
)

const agentBoot = "11111111-1111-4111-8111-111111111111"
const oldAgentBoot = "22222222-2222-4222-8222-222222222222"

type recordingPowerController struct{ calls int }

func (p *recordingPowerController) HaltHost(context.Context) { p.calls++ }

func TestAgentBootBoundDirectives(t *testing.T) {
	for _, tc := range []struct {
		name, stage, directiveBoot, owner, hostBoot string
		wantHalt, wantDrain, wantError              bool
	}{
		{"current boot halt", v1alpha1.StageHalt, agentBoot, "job", agentBoot, true, false, false},
		{"old boot halt", v1alpha1.StageHalt, oldAgentBoot, "job", agentBoot, false, false, false},
		{"missing boot halt", v1alpha1.StageHalt, "", "job", agentBoot, false, false, false},
		{"missing owner halt", v1alpha1.StageHalt, agentBoot, "", agentBoot, false, false, false},
		{"unreadable host boot", v1alpha1.StageHalt, agentBoot, "job", "", false, false, true},
		{"current boot drain", v1alpha1.StageDrain, agentBoot, "job", agentBoot, false, true, false},
		{"old boot drain", v1alpha1.StageDrain, oldAgentBoot, "job", agentBoot, false, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker", Annotations: map[string]string{
				v1alpha1.AnnotationStage:           tc.stage,
				v1alpha1.AnnotationDirectiveBootID: tc.directiveBoot,
				v1alpha1.AnnotationShutdownUID:     tc.owner,
			}}}
			client := fake.NewSimpleClientset(node)
			power := &recordingPowerController{}
			agent := NewAgent(client, AgentConfig{NodeName: "worker", HostBootID: tc.hostBoot}, power)
			err := agent.reconcile(ctx)
			if (err != nil) != tc.wantError {
				t.Fatalf("error = %v, wantError %v", err, tc.wantError)
			}
			if (power.calls > 0) != tc.wantHalt {
				t.Fatalf("halt calls = %d", power.calls)
			}
			updated, err := client.CoreV1().Nodes().Get(ctx, "worker", metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if (updated.Annotations[v1alpha1.AnnotationAgentStatus] == v1alpha1.AgentStatusDrained) != tc.wantDrain {
				t.Fatalf("unexpected drain status: %v", updated.Annotations)
			}
			if !tc.wantError && !tc.wantHalt && !tc.wantDrain && updated.Annotations[v1alpha1.AnnotationStage] != "" {
				t.Fatal("stale directive was not cleared")
			}
		})
	}
}

func TestAgentDoesNotAcknowledgeOrHaltReplacedDirective(t *testing.T) {
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker", Annotations: map[string]string{
		v1alpha1.AnnotationStage:           v1alpha1.StageHalt,
		v1alpha1.AnnotationDirectiveBootID: agentBoot,
		v1alpha1.AnnotationShutdownUID:     "old-job",
	}}}
	client := fake.NewSimpleClientset(node)
	gets := 0
	client.PrependReactor("get", "nodes", func(action ktesting.Action) (bool, runtime.Object, error) {
		gets++
		if gets == 2 {
			changed := node.DeepCopy()
			changed.Annotations[v1alpha1.AnnotationShutdownUID] = "new-job"
			return true, changed, nil
		}
		return false, nil, nil
	})
	power := &recordingPowerController{}
	agent := NewAgent(client, AgentConfig{NodeName: "worker", HostBootID: agentBoot}, power)
	if err := agent.reconcile(context.Background()); err == nil {
		t.Fatal("replaced directive was accepted")
	}
	if power.calls != 0 {
		t.Fatal("host halted after its directive was replaced")
	}
}
