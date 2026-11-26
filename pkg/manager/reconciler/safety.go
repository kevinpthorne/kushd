package reconciler

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"kushd/pkg/apis/kushd/v1alpha1"
	"kushd/pkg/bootid"
)

// Persist the complete boot binding before cordoning or issuing directives.
// A new manager process may resume a job only within the same host boots.
func (r *ClusterReconciler) ensureBootBinding(ctx context.Context, csd *v1alpha1.ClusterShutdown) (bool, error) {
	if !bootid.Valid(r.config.HostBootID) || csd.UID == "" {
		return false, fmt.Errorf("host boot ID and shutdown UID are required")
	}
	nodes, err := r.client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return false, err
	}
	if csd.Status.AnchorBootID == "" || len(csd.Status.NodeBootIDs) == 0 {
		if csd.Status.Phase != "" && csd.Status.Phase != v1alpha1.PhasePending {
			return false, r.retireBootBinding(ctx, csd, "MissingBootIdentity", "Active shutdown has no durable boot binding")
		}
		bindings := make(map[string]string, len(nodes.Items))
		for _, node := range nodes.Items {
			id := node.Status.NodeInfo.BootID
			if !bootid.Valid(id) {
				return false, fmt.Errorf("node %s has no valid registered boot ID", node.Name)
			}
			bindings[node.Name] = id
		}
		if bindings[r.config.AnchorNode] != r.config.HostBootID {
			return false, fmt.Errorf("anchor registration does not match the running host boot")
		}
		csd.Status.AnchorNode = r.config.AnchorNode
		csd.Status.AnchorBootID = r.config.HostBootID
		csd.Status.NodeBootIDs = bindings
		return false, r.transitionPhase(ctx, csd, v1alpha1.PhasePending)
	}
	if csd.Status.AnchorNode != r.config.AnchorNode || csd.Status.AnchorBootID != r.config.HostBootID {
		return false, r.retireBootBinding(ctx, csd, "AnchorBootChanged", "Shutdown belongs to a different anchor boot")
	}
	if len(nodes.Items) != len(csd.Status.NodeBootIDs) {
		return false, r.retireBootBinding(ctx, csd, "NodeInventoryChanged", "Node inventory changed after shutdown authorization")
	}
	for _, node := range nodes.Items {
		if csd.Status.NodeBootIDs[node.Name] != node.Status.NodeInfo.BootID {
			return false, r.retireBootBinding(ctx, csd, "NodeBootChanged", "Node "+node.Name+" rebooted after shutdown authorization")
		}
	}
	return true, nil
}

func (r *ClusterReconciler) retireBootBinding(ctx context.Context, csd *v1alpha1.ClusterShutdown, reason, message string) error {
	slog.Warn("Retiring shutdown with invalid boot binding", "name", csd.Name, "reason", reason)
	if err := r.releaseOwnedState(ctx, csd.UID); err != nil {
		return err
	}
	csd.Status.Conditions = append(csd.Status.Conditions, v1alpha1.Condition{
		Type: "BootBindingInvalid", Status: "True", Reason: reason, Message: message, LastTransitionTime: metav1.Now(),
	})
	return r.transitionPhase(ctx, csd, v1alpha1.PhaseAborted)
}

func directiveMatches(node *corev1.Node, csd *v1alpha1.ClusterShutdown, stage string) bool {
	id := csd.Status.NodeBootIDs[node.Name]
	return id != "" && node.Status.NodeInfo.BootID == id &&
		node.Annotations[v1alpha1.AnnotationDirectiveBootID] == id &&
		node.Annotations[v1alpha1.AnnotationShutdownUID] == string(csd.UID) &&
		node.Annotations[v1alpha1.AnnotationStage] == stage
}

func agentAcknowledged(node *corev1.Node, csd *v1alpha1.ClusterShutdown, stage, status string) bool {
	return directiveMatches(node, csd, stage) &&
		node.Annotations[v1alpha1.AnnotationAgentBootID] == csd.Status.NodeBootIDs[node.Name] &&
		node.Annotations[v1alpha1.AnnotationAgentStatus] == status
}

func (r *ClusterReconciler) cordonNodes(ctx context.Context, csd *v1alpha1.ClusterShutdown) error {
	nodes, err := r.client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	for _, node := range nodes.Items {
		if owner := node.Annotations[v1alpha1.AnnotationCordonOwner]; owner != "" && owner != string(csd.UID) {
			return fmt.Errorf("node %s is owned by another shutdown", node.Name)
		}
		if node.Spec.Unschedulable {
			continue // Never claim an administrator's existing cordon.
		}
		patch := map[string]interface{}{
			"metadata": map[string]interface{}{
				"resourceVersion": node.ResourceVersion,
				"annotations":     map[string]string{v1alpha1.AnnotationCordonOwner: string(csd.UID)},
			},
			"spec": map[string]interface{}{"unschedulable": true},
		}
		data, err := json.Marshal(patch)
		if err != nil {
			return err
		}
		if _, err := r.client.CoreV1().Nodes().Patch(ctx, node.Name, types.MergePatchType, data, metav1.PatchOptions{}); err != nil {
			return err
		}
	}
	return nil
}

// Release only state owned by this shutdown. Unrelated and pre-existing cordons
// and another shutdown's directives must survive recovery unchanged.
func (r *ClusterReconciler) releaseOwnedState(ctx context.Context, owner types.UID) error {
	if owner == "" {
		return nil
	}
	nodes, err := r.client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	for _, node := range nodes.Items {
		ownsCordon := node.Annotations[v1alpha1.AnnotationCordonOwner] == string(owner)
		ownsDirective := node.Annotations[v1alpha1.AnnotationShutdownUID] == string(owner)
		if !ownsCordon && !ownsDirective {
			continue
		}
		annotations := make(map[string]interface{})
		patch := map[string]interface{}{}
		if ownsCordon {
			annotations[v1alpha1.AnnotationCordonOwner] = nil
			patch["spec"] = map[string]interface{}{"unschedulable": false}
		}
		if ownsDirective {
			for _, key := range []string{v1alpha1.AnnotationStage, v1alpha1.AnnotationDrainTimeout,
				v1alpha1.AnnotationDirectiveBootID, v1alpha1.AnnotationAgentBootID,
				v1alpha1.AnnotationAgentStatus, v1alpha1.AnnotationShutdownUID} {
				annotations[key] = nil
			}
		}
		patch["metadata"] = map[string]interface{}{"resourceVersion": node.ResourceVersion, "annotations": annotations}
		data, err := json.Marshal(patch)
		if err != nil {
			return err
		}
		if _, err := r.client.CoreV1().Nodes().Patch(ctx, node.Name, types.MergePatchType, data, metav1.PatchOptions{}); err != nil {
			return err
		}
	}
	return nil
}
