package reconciler

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"syscall"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"

	"kushd/pkg/apis/kushd/v1alpha1"
	"kushd/pkg/manager/ups"
)

type ReconcilerConfig struct {
	AnchorNode   string
	PollInterval time.Duration
	DryRun       bool
}

type ClusterReconciler struct {
	client    kubernetes.Interface
	dynClient dynamic.Interface
	upsClient ups.UPSClient
	config    ReconcilerConfig
}

func NewClusterReconciler(
	client kubernetes.Interface,
	dynClient dynamic.Interface,
	upsClient ups.UPSClient,
	config ReconcilerConfig,
) *ClusterReconciler {
	if config.PollInterval <= 0 {
		config.PollInterval = 2 * time.Second
	}
	return &ClusterReconciler{
		client:    client,
		dynClient: dynClient,
		upsClient: upsClient,
		config:    config,
	}
}

// Run starts the reconciliation loop.
func (r *ClusterReconciler) Run(ctx context.Context) error {
	slog.Info("Starting cluster reconciler loop", "anchorNode", r.config.AnchorNode)

	ticker := time.NewTicker(r.config.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("Stopping cluster reconciler loop")
			return ctx.Err()
		case <-ticker.C:
			if err := r.reconcile(ctx); err != nil {
				slog.Error("Reconcile iteration failed", "error", err)
			}
		}
	}
}

func (r *ClusterReconciler) reconcile(ctx context.Context) error {
	list, err := r.dynClient.Resource(v1alpha1.ClusterShutdownGVR).List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list clustershutdowns: %w", err)
	}

	for _, item := range list.Items {
		var csd v1alpha1.ClusterShutdown
		data, err := item.MarshalJSON()
		if err != nil {
			continue
		}
		if err := json.Unmarshal(data, &csd); err != nil {
			continue
		}

		if csd.Status.Phase == v1alpha1.PhaseCompleted ||
			csd.Status.Phase == v1alpha1.PhaseAborted ||
			csd.Status.Phase == v1alpha1.PhaseFailed {
			continue
		}

		if err := r.reconcileCSD(ctx, &csd); err != nil {
			slog.Error("Failed to reconcile ClusterShutdown", "name", csd.Name, "phase", csd.Status.Phase, "error", err)
		}
	}

	return nil
}

func (r *ClusterReconciler) reconcileCSD(ctx context.Context, csd *v1alpha1.ClusterShutdown) error {
	slog.Info("Reconciling ClusterShutdown", "name", csd.Name, "phase", csd.Status.Phase, "abort", csd.Spec.Abort)

	switch csd.Status.Phase {
	case "", v1alpha1.PhasePending:
		if csd.Spec.Abort {
			slog.Info("Shutdown aborted in Pending phase", "name", csd.Name)
			return r.transitionPhase(ctx, csd, v1alpha1.PhaseAborted)
		}
		return r.transitionPhase(ctx, csd, v1alpha1.PhaseCordoningCluster)

	case v1alpha1.PhaseCordoningCluster:
		if csd.Spec.Abort {
			slog.Info("Shutdown aborted in CordoningCluster phase; uncordoning cluster", "name", csd.Name)
			if err := r.setClusterCordon(ctx, false); err != nil {
				slog.Error("Failed to uncordon cluster on abort", "error", err)
			}
			return r.transitionPhase(ctx, csd, v1alpha1.PhaseAborted)
		}

		slog.Info("Cordoning all cluster nodes", "name", csd.Name)
		if !csd.Spec.DryRun && !r.config.DryRun {
			if err := r.setClusterCordon(ctx, true); err != nil {
				return fmt.Errorf("failed to cordon cluster nodes: %w", err)
			}
		}

		// Transition to POINT OF NO RETURN: DrainingWorkers
		return r.transitionPhase(ctx, csd, v1alpha1.PhaseDrainingWorkers)

	case v1alpha1.PhaseDrainingWorkers:
		r.handlePointOfNoReturnAbort(ctx, csd)

		workers, err := r.getWorkerNodes(ctx)
		if err != nil {
			return err
		}

		timeoutSec := csd.Spec.Timeouts.WorkerEvacuationSeconds
		if timeoutSec <= 0 {
			timeoutSec = v1alpha1.DefaultWorkerEvacuationSeconds
		}
		timeoutStr := fmt.Sprintf("%ds", timeoutSec)

		if !csd.Spec.DryRun && !r.config.DryRun {
			// Signal workers to drain
			allDrained := true
			for _, node := range workers {
				stage := node.Annotations[v1alpha1.AnnotationStage]
				status := node.Annotations[v1alpha1.AnnotationAgentStatus]

				if stage != v1alpha1.StageDrain {
					if err := r.annotateNodeStage(ctx, node.Name, v1alpha1.StageDrain, timeoutStr); err != nil {
						slog.Error("Failed to annotate worker node for drain", "node", node.Name, "error", err)
					}
					allDrained = false
				} else if status != v1alpha1.AgentStatusDrained {
					allDrained = false
				}
			}

			if !allDrained {
				slog.Info("Waiting for worker nodes to report drained status...", "count", len(workers))
				return nil
			}
		}

		slog.Info("All worker nodes drained; advancing to HaltingWorkers", "name", csd.Name)
		return r.transitionPhase(ctx, csd, v1alpha1.PhaseHaltingWorkers)

	case v1alpha1.PhaseHaltingWorkers:
		r.handlePointOfNoReturnAbort(ctx, csd)

		workers, err := r.getWorkerNodes(ctx)
		if err != nil {
			return err
		}

		if !csd.Spec.DryRun && !r.config.DryRun {
			for _, node := range workers {
				if node.Annotations[v1alpha1.AnnotationStage] != v1alpha1.StageHalt {
					if err := r.annotateNodeStage(ctx, node.Name, v1alpha1.StageHalt, ""); err != nil {
						slog.Error("Failed to annotate worker node for halt", "node", node.Name, "error", err)
					}
				}
			}
		}

		slog.Info("Worker halt directives dispatched; advancing to DrainingControlPlane", "name", csd.Name)
		return r.transitionPhase(ctx, csd, v1alpha1.PhaseDrainingControlPlane)

	case v1alpha1.PhaseDrainingControlPlane:
		r.handlePointOfNoReturnAbort(ctx, csd)

		followers, err := r.getControlPlaneFollowers(ctx)
		if err != nil {
			return err
		}

		timeoutSec := csd.Spec.Timeouts.ControlPlaneEvacuationSeconds
		if timeoutSec <= 0 {
			timeoutSec = v1alpha1.DefaultControlPlaneEvacuationSecs
		}
		timeoutStr := fmt.Sprintf("%ds", timeoutSec)

		if !csd.Spec.DryRun && !r.config.DryRun {
			// Sequential N-1 Drain: process followers one by one
			for _, node := range followers {
				status := node.Annotations[v1alpha1.AnnotationAgentStatus]
				stage := node.Annotations[v1alpha1.AnnotationStage]

				if status == v1alpha1.AgentStatusDrained {
					continue
				}

				if stage != v1alpha1.StageDrain {
					slog.Info("Draining CP follower node", "node", node.Name)
					if err := r.annotateNodeStage(ctx, node.Name, v1alpha1.StageDrain, timeoutStr); err != nil {
						return err
					}
				}
				// Still waiting on this follower before moving to next
				slog.Info("Waiting for CP follower to complete drain", "node", node.Name)
				return nil
			}
		}

		slog.Info("All CP followers drained; advancing to HaltingControlPlane", "name", csd.Name)
		return r.transitionPhase(ctx, csd, v1alpha1.PhaseHaltingControlPlane)

	case v1alpha1.PhaseHaltingControlPlane:
		r.handlePointOfNoReturnAbort(ctx, csd)

		followers, err := r.getControlPlaneFollowers(ctx)
		if err != nil {
			return err
		}

		if !csd.Spec.DryRun && !r.config.DryRun {
			for _, node := range followers {
				if node.Annotations[v1alpha1.AnnotationStage] != v1alpha1.StageHalt {
					slog.Info("Halting CP follower node", "node", node.Name)
					if err := r.annotateNodeStage(ctx, node.Name, v1alpha1.StageHalt, ""); err != nil {
						slog.Error("Failed to annotate CP follower node for halt", "node", node.Name, "error", err)
					}
				}
			}
		}

		slog.Info("CP follower halt directives dispatched; advancing to Finalizing", "name", csd.Name)
		return r.transitionPhase(ctx, csd, v1alpha1.PhaseFinalizing)

	case v1alpha1.PhaseFinalizing:
		r.handlePointOfNoReturnAbort(ctx, csd)
		slog.Info("Executing final anchor node synchronization and power management", "name", csd.Name)

		if !csd.Spec.DryRun && !r.config.DryRun {
			// Flush filesystem buffers
			syscall.Sync()

			// Check optional killpower
			if csd.Spec.PowerManagement.EnableKillpower {
				delay := csd.Spec.PowerManagement.KillpowerDelaySeconds
				if delay < v1alpha1.DefaultKillpowerDelaySeconds {
					slog.Warn("Configured killpower delay is below safe margin (180s); clamping to 180s", "configured", delay)
					delay = v1alpha1.DefaultKillpowerDelaySeconds
				}

				if r.upsClient != nil {
					slog.Warn("Issuing UPS killpower command...", "delay", delay)
					if err := r.upsClient.Killpower(ctx, delay, csd.Spec.PowerManagement.TargetOutletGroup); err != nil {
						slog.Error("Failed to issue UPS killpower command", "error", err)
					}
				}
			}
		}

		return r.transitionPhase(ctx, csd, v1alpha1.PhaseCompleted)

	case v1alpha1.PhaseCompleted:
		slog.Info("ClusterShutdown completed successfully; halting anchor node", "anchorNode", r.config.AnchorNode)
		if !csd.Spec.DryRun && !r.config.DryRun && r.config.AnchorNode != "" {
			_ = r.annotateNodeStage(ctx, r.config.AnchorNode, v1alpha1.StageHalt, "")
		}
		return nil
	}

	return nil
}

// handlePointOfNoReturnAbort enforces the Point-of-No-Return invariant.
func (r *ClusterReconciler) handlePointOfNoReturnAbort(ctx context.Context, csd *v1alpha1.ClusterShutdown) {
	if !csd.Spec.Abort {
		return
	}

	for _, cond := range csd.Status.Conditions {
		if cond.Type == v1alpha1.ConditionTypeAbortRejected {
			return // Already recorded
		}
	}

	slog.Warn("Point of No Return Exceeded: Manual abort rejected", "phase", csd.Status.Phase)

	// Add AbortRejected condition
	now := metav1.Now()
	csd.Status.Conditions = append(csd.Status.Conditions, v1alpha1.Condition{
		Type:               v1alpha1.ConditionTypeAbortRejected,
		Status:             "True",
		LastTransitionTime: now,
		Reason:             v1alpha1.ConditionReasonPointOfNoReturn,
		Message:            "Manual abort rejected: cluster has passed the point-of-no-return (workers are draining/halting). Full shutdown must proceed.",
	})

	_ = r.updateStatus(ctx, csd)

	// Emit cluster Warning event
	r.emitWarningEvent(ctx, csd, v1alpha1.ConditionTypeAbortRejected,
		"Manual abort rejected: cluster has passed the point-of-no-return. Evacuation pipeline continuing to completion.")
}

func (r *ClusterReconciler) emitWarningEvent(ctx context.Context, csd *v1alpha1.ClusterShutdown, reason, message string) {
	now := metav1.Now()
	event := &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "csd-abort-rejected-",
			Namespace:    metav1.NamespaceDefault,
		},
		InvolvedObject: corev1.ObjectReference{
			Kind:       "ClusterShutdown",
			Name:       csd.Name,
			UID:        csd.UID,
			APIVersion: fmt.Sprintf("%s/%s", v1alpha1.GroupName, v1alpha1.Version),
		},
		Reason:         reason,
		Message:        message,
		Type:           corev1.EventTypeWarning,
		FirstTimestamp: now,
		LastTimestamp:  now,
		Count:          1,
		Source: corev1.EventSource{
			Component: "kushd-manager",
		},
	}

	_, err := r.client.CoreV1().Events(metav1.NamespaceDefault).Create(ctx, event, metav1.CreateOptions{})
	if err != nil {
		slog.Warn("Failed to emit Kubernetes warning event", "error", err)
	}
}

func (r *ClusterReconciler) transitionPhase(ctx context.Context, csd *v1alpha1.ClusterShutdown, newPhase v1alpha1.ClusterShutdownPhase) error {
	slog.Info("Transitioning ClusterShutdown phase", "from", csd.Status.Phase, "to", newPhase)
	csd.Status.Phase = newPhase

	now := metav1.Now()
	if csd.Status.StartTime == nil {
		csd.Status.StartTime = &now
	}
	if newPhase == v1alpha1.PhaseCompleted || newPhase == v1alpha1.PhaseAborted || newPhase == v1alpha1.PhaseFailed {
		csd.Status.CompletionTime = &now
	}
	if csd.Status.AnchorNode == "" {
		csd.Status.AnchorNode = r.config.AnchorNode
	}

	return r.updateStatus(ctx, csd)
}

func (r *ClusterReconciler) updateStatus(ctx context.Context, csd *v1alpha1.ClusterShutdown) error {
	item, err := r.dynClient.Resource(v1alpha1.ClusterShutdownGVR).Get(ctx, csd.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}

	statusMap := map[string]interface{}{
		"phase":      string(csd.Status.Phase),
		"anchorNode": csd.Status.AnchorNode,
	}
	if csd.Status.StartTime != nil {
		statusMap["startTime"] = csd.Status.StartTime.Format(time.RFC3339)
	}
	if csd.Status.CompletionTime != nil {
		statusMap["completionTime"] = csd.Status.CompletionTime.Format(time.RFC3339)
	}
	if len(csd.Status.Conditions) > 0 {
		var conds []interface{}
		for _, c := range csd.Status.Conditions {
			conds = append(conds, map[string]interface{}{
				"type":               c.Type,
				"status":             c.Status,
				"reason":             c.Reason,
				"message":            c.Message,
				"lastTransitionTime": c.LastTransitionTime.Format(time.RFC3339),
			})
		}
		statusMap["conditions"] = conds
	}

	item.Object["status"] = statusMap
	_, err = r.dynClient.Resource(v1alpha1.ClusterShutdownGVR).UpdateStatus(ctx, item, metav1.UpdateOptions{})
	return err
}

func (r *ClusterReconciler) setClusterCordon(ctx context.Context, unschedulable bool) error {
	nodes, err := r.client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}

	for _, node := range nodes.Items {
		if node.Spec.Unschedulable == unschedulable {
			continue
		}
		patch := map[string]interface{}{
			"spec": map[string]interface{}{
				"unschedulable": unschedulable,
			},
		}
		bytes, err := json.Marshal(patch)
		if err != nil {
			continue
		}
		_, err = r.client.CoreV1().Nodes().Patch(ctx, node.Name, types.MergePatchType, bytes, metav1.PatchOptions{})
		if err != nil {
			slog.Error("Failed to update node cordon status", "node", node.Name, "unschedulable", unschedulable, "error", err)
		}
	}
	return nil
}

func (r *ClusterReconciler) annotateNodeStage(ctx context.Context, nodeName, stage, timeout string) error {
	annotations := map[string]string{
		v1alpha1.AnnotationStage: stage,
	}
	if timeout != "" {
		annotations[v1alpha1.AnnotationDrainTimeout] = timeout
	}

	patch := map[string]interface{}{
		"metadata": map[string]interface{}{
			"annotations": annotations,
		},
	}

	bytes, err := json.Marshal(patch)
	if err != nil {
		return err
	}

	_, err = r.client.CoreV1().Nodes().Patch(ctx, nodeName, types.MergePatchType, bytes, metav1.PatchOptions{})
	return err
}

func (r *ClusterReconciler) getWorkerNodes(ctx context.Context) ([]corev1.Node, error) {
	list, err := r.client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	var workers []corev1.Node
	for _, node := range list.Items {
		_, isCP := node.Labels[v1alpha1.LabelNodeRoleControlPlane]
		_, isMaster := node.Labels[v1alpha1.LabelNodeRoleMaster]
		if !isCP && !isMaster {
			workers = append(workers, node)
		}
	}

	return workers, nil
}

func (r *ClusterReconciler) getControlPlaneFollowers(ctx context.Context) ([]corev1.Node, error) {
	list, err := r.client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	var followers []corev1.Node
	for _, node := range list.Items {
		_, isCP := node.Labels[v1alpha1.LabelNodeRoleControlPlane]
		_, isMaster := node.Labels[v1alpha1.LabelNodeRoleMaster]
		if (isCP || isMaster) && node.Name != r.config.AnchorNode {
			followers = append(followers, node)
		}
	}

	return followers, nil
}
