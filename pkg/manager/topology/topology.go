package topology

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"kushd/pkg/apis/kushd/v1alpha1"
)

// ValidateAnchorTopology checks if the node hosting the UPS is a control-plane node.
func ValidateAnchorTopology(ctx context.Context, client kubernetes.Interface, nodeName string) error {
	node, err := client.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to retrieve node metadata during topology validation: %w", err)
	}

	_, isControlPlane := node.Labels[v1alpha1.LabelNodeRoleControlPlane]
	_, isMaster := node.Labels[v1alpha1.LabelNodeRoleMaster]

	if !isControlPlane && !isMaster {
		return fmt.Errorf("UPS_ATTACHED_TO_WORKER: node %s lacks %s or %s labels",
			nodeName, v1alpha1.LabelNodeRoleControlPlane, v1alpha1.LabelNodeRoleMaster)
	}

	return nil
}

// ValidateAnchorTopologyOrExit performs immediate topology validation and terminates
// cleanly with os.Exit(1) on failure to induce CrashLoopBackOff without stack trace pollution.
func ValidateAnchorTopologyOrExit(ctx context.Context, client kubernetes.Interface, nodeName string) {
	if err := ValidateAnchorTopology(ctx, client, nodeName); err != nil {
		slog.Error("FATAL: Topology Invariant Violated",
			"error", "UPS_ATTACHED_TO_WORKER",
			"node", nodeName,
			"remediation", "Move physical UPS USB/Serial cable to a designated control-plane node",
		)
		os.Exit(1)
	}
	slog.Info("Topology Placement Invariant verified: UPS connected to designated control-plane node", "node", nodeName)
}
