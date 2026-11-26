package reconciler

import (
	"context"
	"encoding/json"
	"fmt"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"log/slog"
	"sync"
	"syscall"
	"time"

	"kushd/pkg/apis/kushd/v1alpha1"
	"kushd/pkg/manager/ups"
)

type ReconcilerConfig struct {
	AnchorNode      string
	HostBootID      string
	PollInterval    time.Duration
	DryRun          bool
	PowerController hostPowerController
}

type hostPowerController interface {
	HaltHost(context.Context)
}

type ClusterReconciler struct {
	client    kubernetes.Interface
	dynClient dynamic.Interface
	upsClient ups.UPSClient
	config    ReconcilerConfig

	mu                sync.RWMutex
	hasReconciled     bool
	lastReconcileTime time.Time
	lastReconcileErr  error
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

	// Immediate check on start
	if err := r.reconcile(ctx); err != nil {
		slog.Error("Reconcile initial check failed", "error", err)
	}

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

// Health returns an error if the reconciler has not completed an initial check,
// if the latest reconciliation failed, or if the reconciliation loop has stalled.
func (r *ClusterReconciler) Health() error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if !r.hasReconciled {
		return fmt.Errorf("initial reconciliation has not completed")
	}
	if r.lastReconcileErr != nil {
		return fmt.Errorf("cluster reconciler failing: %w", r.lastReconcileErr)
	}
	if time.Since(r.lastReconcileTime) > 3*r.config.PollInterval {
		return fmt.Errorf("cluster reconciler stalled (last reconcile was %v ago)", time.Since(r.lastReconcileTime).Round(time.Second))
	}
	return nil
}

func (r *ClusterReconciler) reconcile(ctx context.Context) error {
	list, err := r.dynClient.Resource(v1alpha1.ClusterShutdownGVR).List(ctx, metav1.ListOptions{})
	r.mu.Lock()
	r.hasReconciled = true
	r.lastReconcileTime = time.Now()
	r.lastReconcileErr = err
	r.mu.Unlock()

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
			if csd.Status.Phase != v1alpha1.PhaseCompleted || csd.Status.AnchorBootID != r.config.HostBootID {
				if err := r.releaseOwnedState(ctx, csd.UID); err != nil {
					return err
				}
			}
			continue
		}

		if err := r.reconcileCSD(ctx, &csd); err != nil {
			slog.Error("Failed to reconcile ClusterShutdown", "name", csd.Name, "phase", csd.Status.Phase, "error", err)
			r.mu.Lock()
			r.lastReconcileErr = err
			r.mu.Unlock()
			return err
		}
	}

	return nil
}

func (r *ClusterReconciler) reconcileCSD(ctx context.Context, csd *v1alpha1.ClusterShutdown) error {
	slog.Info("Reconciling ClusterShutdown", "name", csd.Name, "phase", csd.Status.Phase, "abort", csd.Spec.Abort)
	if csd.Spec.Abort && (csd.Status.Phase == "" || csd.Status.Phase == v1alpha1.PhasePending ||
		csd.Status.Phase == v1alpha1.PhaseCordoningCluster) {
		if err := r.releaseOwnedState(ctx, csd.UID); err != nil {
			return err
		}
		return r.transitionPhase(ctx, csd, v1alpha1.PhaseAborted)
	}
	bound, err := r.ensureBootBinding(ctx, csd)
	if err != nil || !bound {
		return err
	}

	switch csd.Status.Phase {
	case "", v1alpha1.PhasePending:
		return r.transitionPhase(ctx, csd, v1alpha1.PhaseCordoningCluster)

	case v1alpha1.PhaseCordoningCluster:

		slog.Info("Cordoning all cluster nodes", "name", csd.Name)
		if !csd.Spec.DryRun && !r.config.DryRun {
			if err := r.cordonNodes(ctx, csd); err != nil {
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
				if !directiveMatches(&node, csd, v1alpha1.StageDrain) {
					if err := r.annotateNodeStage(ctx, csd, node.Name, v1alpha1.StageDrain, timeoutStr); err != nil {
						slog.Error("Failed to annotate worker node for drain", "node", node.Name, "error", err)
					}
					allDrained = false
				} else if !agentAcknowledged(&node, csd, v1alpha1.StageDrain, v1alpha1.AgentStatusDrained) {
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
				if !directiveMatches(&node, csd, v1alpha1.StageHalt) {
					if err := r.annotateNodeStage(ctx, csd, node.Name, v1alpha1.StageHalt, ""); err != nil {
						return fmt.Errorf("failed to dispatch worker halt to %s: %w", node.Name, err)
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
				if agentAcknowledged(&node, csd, v1alpha1.StageDrain, v1alpha1.AgentStatusDrained) {
					continue
				}

				if !directiveMatches(&node, csd, v1alpha1.StageDrain) {
					slog.Info("Draining CP follower node", "node", node.Name)
					if err := r.annotateNodeStage(ctx, csd, node.Name, v1alpha1.StageDrain, timeoutStr); err != nil {
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
				if !directiveMatches(&node, csd, v1alpha1.StageHalt) {
					slog.Info("Halting CP follower node", "node", node.Name)
					if err := r.annotateNodeStage(ctx, csd, node.Name, v1alpha1.StageHalt, ""); err != nil {
						return fmt.Errorf("failed to dispatch control-plane halt to %s: %w", node.Name, err)
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

		if !csd.Spec.DryRun && !r.config.DryRun && r.config.AnchorNode != "" {
			slog.Info("ClusterShutdown completed successfully; halting anchor node", "anchorNode", r.config.AnchorNode)
			if err := r.annotateNodeStage(ctx, csd, r.config.AnchorNode, v1alpha1.StageHalt, ""); err != nil {
				return err
			}
			if err := r.transitionPhase(ctx, csd, v1alpha1.PhaseCompleted); err != nil {
				slog.Warn("Could not persist completion after control-plane halt directives", "error", err)
			}
			if r.config.PowerController != nil {
				slog.Info("Halting anchor node host in-process via HostPowerController", "anchorNode", r.config.AnchorNode)
				r.config.PowerController.HaltHost(ctx)
			}
			return nil
		}

		return r.transitionPhase(ctx, csd, v1alpha1.PhaseCompleted)
	case v1alpha1.PhaseCompleted:
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
		"phase":        string(csd.Status.Phase),
		"anchorNode":   csd.Status.AnchorNode,
		"anchorBootID": csd.Status.AnchorBootID,
	}
	if csd.Status.NodeBootIDs != nil {
		bindings := make(map[string]interface{}, len(csd.Status.NodeBootIDs))
		for node, id := range csd.Status.NodeBootIDs {
			bindings[node] = id
		}
		statusMap["nodeBootIDs"] = bindings
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

func (r *ClusterReconciler) annotateNodeStage(ctx context.Context, csd *v1alpha1.ClusterShutdown, nodeName, stage, timeout string) error {
	id := csd.Status.NodeBootIDs[nodeName]
	if id == "" || csd.UID == "" {
		return fmt.Errorf("cannot issue an unbound shutdown directive")
	}
	node, err := r.client.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if node.Status.NodeInfo.BootID != id {
		return fmt.Errorf("node %s boot changed before directive dispatch", nodeName)
	}
	if owner := node.Annotations[v1alpha1.AnnotationShutdownUID]; owner != "" && owner != string(csd.UID) {
		return fmt.Errorf("node %s has another shutdown's directive", nodeName)
	}
	if directiveMatches(node, csd, stage) {
		return nil
	}
	annotations := map[string]interface{}{
		v1alpha1.AnnotationStage:           stage,
		v1alpha1.AnnotationDirectiveBootID: id,
		v1alpha1.AnnotationShutdownUID:     string(csd.UID),
		v1alpha1.AnnotationAgentStatus:     nil,
		v1alpha1.AnnotationAgentBootID:     nil,
		v1alpha1.AnnotationDrainTimeout:    nil,
	}
	if timeout != "" {
		annotations[v1alpha1.AnnotationDrainTimeout] = timeout
	}
	patch := map[string]interface{}{
		"metadata": map[string]interface{}{
			"resourceVersion": node.ResourceVersion,
			"annotations":     annotations,
		},
	}
	data, err := json.Marshal(patch)
	if err != nil {
		return err
	}
	_, err = r.client.CoreV1().Nodes().Patch(ctx, nodeName, types.MergePatchType, data, metav1.PatchOptions{})
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

// RecoverClusterOnStartup retires old-boot shutdowns and releases only the state
// owned by completed or cancelled workflows, never an administrator's cordon.
func (r *ClusterReconciler) RecoverClusterOnStartup(ctx context.Context) error {
	list, err := r.dynClient.Resource(v1alpha1.ClusterShutdownGVR).List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list clustershutdowns: %w", err)
	}
	for _, item := range list.Items {
		var csd v1alpha1.ClusterShutdown
		data, err := item.MarshalJSON()
		if err != nil {
			return err
		}
		if err := json.Unmarshal(data, &csd); err != nil {
			return err
		}
		switch csd.Status.Phase {
		case v1alpha1.PhaseAborted, v1alpha1.PhaseFailed:
			if err := r.releaseOwnedState(ctx, csd.UID); err != nil {
				return err
			}
		case v1alpha1.PhaseCompleted:
			if csd.Status.AnchorBootID != r.config.HostBootID {
				if err := r.releaseOwnedState(ctx, csd.UID); err != nil {
					return err
				}
			}
		default:
			// Pre-existing unbound requests cannot prove which boot authorized them.
			// Only requests first observed after startup may acquire a new binding.
			if csd.Status.AnchorBootID == "" || len(csd.Status.NodeBootIDs) == 0 {
				if err := r.retireBootBinding(ctx, &csd, "MissingBootIdentity", "Queued shutdown has no durable boot binding"); err != nil {
					return err
				}
				continue
			}
			if _, err := r.ensureBootBinding(ctx, &csd); err != nil {
				return err
			}
		}
	}
	return nil
}
