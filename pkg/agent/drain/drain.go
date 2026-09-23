package drain

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/kubernetes"
)

var (
	// PollInterval defines how often eviction attempts are retried.
	// Can be configured in tests.
	PollInterval = 2 * time.Second
)

// ExecuteEscalatingDrain coordinates local pod evictions for a given node.
// It assigns 75% of the timeout window to polite eviction honoring PDBs,
// and escalates to forceful deletion (gracePeriodSeconds: 0) during the final 25%.
func ExecuteEscalatingDrain(ctx context.Context, client kubernetes.Interface, nodeName string, timeout time.Duration) error {
	politeDuration := time.Duration(float64(timeout) * 0.75)
	politeDeadline := time.Now().Add(politeDuration)
	hardDeadline := time.Now().Add(timeout)

	slog.Info("Starting escalating drain on node",
		"node", nodeName,
		"timeout", timeout,
		"politeDeadline", politeDeadline.Format(time.RFC3339),
		"hardDeadline", hardDeadline.Format(time.RFC3339),
	)

	// Phase 1: Polite Eviction (75% of window)
	// Honors PodDisruptionBudgets and graceful termination periods
	for time.Now().Before(politeDeadline) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		remainingPods, err := GetNonDaemonSetPods(ctx, client, nodeName)
		if err != nil {
			slog.Warn("Failed to list non-daemonset pods during polite drain", "node", nodeName, "error", err)
		} else if len(remainingPods) == 0 {
			slog.Info("All non-daemonset pods successfully evicted during polite phase", "node", nodeName)
			return nil
		} else {
			slog.Info("Attempting polite eviction of pods", "count", len(remainingPods), "node", nodeName)
			for _, pod := range remainingPods {
				eviction := &policyv1.Eviction{
					ObjectMeta: metav1.ObjectMeta{
						Name:      pod.Name,
						Namespace: pod.Namespace,
					},
				}
				err := client.CoreV1().Pods(pod.Namespace).EvictV1(ctx, eviction)
				if err != nil && !apierrors.IsNotFound(err) && !apierrors.IsTooManyRequests(err) {
					slog.Debug("Eviction request returned notice", "pod", pod.Name, "namespace", pod.Namespace, "error", err)
				}
			}
		}

		sleepDuration := PollInterval
		if timeLeft := time.Until(politeDeadline); timeLeft < sleepDuration {
			sleepDuration = timeLeft
		}
		if sleepDuration > 0 {
			time.Sleep(sleepDuration)
		}
	}

	slog.Warn("Polite eviction window elapsed; escalating to forceful deletion", "node", nodeName)

	// Phase 2: Forceful Deletion Escalation (Final 25% of window)
	// Overrides failing PDBs, hung finalizers, and unresponsive storage mounts
	forceContext, cancel := context.WithDeadline(ctx, hardDeadline)
	defer cancel()

	for time.Now().Before(hardDeadline) {
		select {
		case <-forceContext.Done():
			return forceContext.Err()
		default:
		}

		remainingPods, err := GetNonDaemonSetPods(forceContext, client, nodeName)
		if err != nil {
			slog.Warn("Failed to list pods during forceful deletion", "node", nodeName, "error", err)
		} else if len(remainingPods) == 0 {
			slog.Info("All non-daemonset pods successfully cleared after forceful escalation", "node", nodeName)
			return nil
		} else {
			slog.Warn("Force deleting remaining pods with gracePeriodSeconds=0", "count", len(remainingPods), "node", nodeName)
			gracePeriodZero := int64(0)
			for _, pod := range remainingPods {
				err := client.CoreV1().Pods(pod.Namespace).Delete(forceContext, pod.Name, metav1.DeleteOptions{
					GracePeriodSeconds: &gracePeriodZero,
				})
				if err != nil && !apierrors.IsNotFound(err) {
					slog.Error("Failed to force delete pod", "pod", pod.Name, "namespace", pod.Namespace, "error", err)
				}
			}
		}

		sleepDuration := PollInterval
		if timeLeft := time.Until(hardDeadline); timeLeft < sleepDuration {
			sleepDuration = timeLeft
		}
		if sleepDuration > 0 {
			time.Sleep(sleepDuration)
		}
	}

	remainingPods, _ := GetNonDaemonSetPods(ctx, client, nodeName)
	if len(remainingPods) > 0 {
		return fmt.Errorf("drain timed out with %d pods still active on node %s", len(remainingPods), nodeName)
	}

	return nil
}

// GetNonDaemonSetPods retrieves all non-DaemonSet, non-mirror, active pods running on the node.
func GetNonDaemonSetPods(ctx context.Context, client kubernetes.Interface, nodeName string) ([]corev1.Pod, error) {
	podList, err := client.CoreV1().Pods("").List(ctx, metav1.ListOptions{
		FieldSelector: fields.OneTermEqualSelector("spec.nodeName", nodeName).String(),
	})
	if err != nil {
		return nil, err
	}

	var activePods []corev1.Pod
	for _, pod := range podList.Items {
		if pod.Spec.NodeName != "" && pod.Spec.NodeName != nodeName {
			continue
		}

		// Ignore mirror pods (static pods managed directly by kubelet)
		if _, isMirror := pod.Annotations[corev1.MirrorPodAnnotationKey]; isMirror {
			continue
		}

		// Ignore pods owned by DaemonSets
		if isOwnedByDaemonSet(&pod) {
			continue
		}

		// Ignore pods that have completed their lifecycle
		if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		}

		activePods = append(activePods, pod)
	}

	return activePods, nil
}

func isOwnedByDaemonSet(pod *corev1.Pod) bool {
	for _, ref := range pod.OwnerReferences {
		if ref.Kind == "DaemonSet" {
			return true
		}
	}
	return false
}
