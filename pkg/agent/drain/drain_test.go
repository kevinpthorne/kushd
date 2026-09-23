package drain

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestGetNonDaemonSetPods(t *testing.T) {
	ctx := context.Background()

	pods := []corev1.Pod{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "app-pod",
				Namespace: "default",
			},
			Spec: corev1.PodSpec{
				NodeName: "node-1",
			},
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "daemon-pod",
				Namespace: "kube-system",
				OwnerReferences: []metav1.OwnerReference{
					{
						Kind: "DaemonSet",
						Name: "kushd-agent",
					},
				},
			},
			Spec: corev1.PodSpec{
				NodeName: "node-1",
			},
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "mirror-pod",
				Namespace: "kube-system",
				Annotations: map[string]string{
					corev1.MirrorPodAnnotationKey: "hash",
				},
			},
			Spec: corev1.PodSpec{
				NodeName: "node-1",
			},
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "succeeded-pod",
				Namespace: "default",
			},
			Spec: corev1.PodSpec{
				NodeName: "node-1",
			},
			Status: corev1.PodStatus{
				Phase: corev1.PodSucceeded,
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "other-node-pod",
				Namespace: "default",
			},
			Spec: corev1.PodSpec{
				NodeName: "node-2",
			},
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
			},
		},
	}

	client := fake.NewSimpleClientset(&corev1.PodList{Items: pods})

	nonDSPods, err := GetNonDaemonSetPods(ctx, client, "node-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(nonDSPods) != 1 {
		t.Fatalf("expected 1 active non-daemonset pod on node-1, got %d", len(nonDSPods))
	}

	if nonDSPods[0].Name != "app-pod" {
		t.Fatalf("expected app-pod, got %s", nonDSPods[0].Name)
	}
}

func TestExecuteEscalatingDrain(t *testing.T) {
	ctx := context.Background()
	PollInterval = 10 * time.Millisecond

	appPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "database-pod",
			Namespace: "default",
		},
		Spec: corev1.PodSpec{
			NodeName: "worker-node",
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
		},
	}

	client := fake.NewSimpleClientset(appPod)

	// Execute drain with 100ms timeout
	err := ExecuteEscalatingDrain(ctx, client, "worker-node", 100*time.Millisecond)
	if err != nil {
		t.Fatalf("drain failed: %v", err)
	}

	// Verify pod was deleted in forceful escalation
	remaining, err := GetNonDaemonSetPods(ctx, client, "worker-node")
	if err != nil {
		t.Fatalf("failed to query remaining pods: %v", err)
	}
	if len(remaining) != 0 {
		t.Fatalf("expected 0 remaining pods after drain, got %d", len(remaining))
	}
}
