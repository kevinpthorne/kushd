package agent

import (
	"context"
	"encoding/json"
	"fmt"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"log/slog"
	"time"

	"kushd/pkg/agent/drain"
	"kushd/pkg/apis/kushd/v1alpha1"
	"kushd/pkg/bootid"
)

type AgentConfig struct {
	NodeName     string
	PollInterval time.Duration
	HostBootID   string
}

type hostPowerController interface {
	HaltHost(context.Context)
}

type Agent struct {
	client          kubernetes.Interface
	config          AgentConfig
	powerController hostPowerController
}

func NewAgent(client kubernetes.Interface, config AgentConfig, powerController hostPowerController) *Agent {
	if config.PollInterval <= 0 {
		config.PollInterval = 2 * time.Second
	}
	return &Agent{
		client:          client,
		config:          config,
		powerController: powerController,
	}
}

// Run starts the agent reconciliation loop until context cancellation.
func (a *Agent) Run(ctx context.Context) error {
	if !bootid.Valid(a.config.HostBootID) {
		return fmt.Errorf("valid host boot ID is required")
	}
	slog.Info("Starting kushd-agent", "node", a.config.NodeName, "bootID", a.config.HostBootID)
	if err := a.reconcile(ctx); err != nil {
		slog.Error("Initial agent reconciliation failed", "error", err)
	}
	ticker := time.NewTicker(a.config.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := a.reconcile(ctx); err != nil {
				slog.Error("Agent reconcile iteration failed", "error", err)
			}
		}
	}
}

// Reconcile checks node annotations and triggers drain or halt actions.
func (a *Agent) reconcile(ctx context.Context) error {
	if !bootid.Valid(a.config.HostBootID) {
		return fmt.Errorf("valid host boot ID is required")
	}
	node, err := a.client.CoreV1().Nodes().Get(ctx, a.config.NodeName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get node %s: %w", a.config.NodeName, err)
	}
	stage := node.Annotations[v1alpha1.AnnotationStage]
	if stage == v1alpha1.StageDrain || stage == v1alpha1.StageHalt {
		if node.Annotations[v1alpha1.AnnotationDirectiveBootID] != a.config.HostBootID ||
			node.Annotations[v1alpha1.AnnotationShutdownUID] == "" {
			slog.Warn("Rejecting stale or unbound shutdown directive", "node", a.config.NodeName,
				"stage", stage, "directiveBootID", node.Annotations[v1alpha1.AnnotationDirectiveBootID],
				"hostBootID", a.config.HostBootID)
			return a.clearStaleDirective(ctx, node)
		}
	}
	currentStatus := node.Annotations[v1alpha1.AnnotationAgentStatus]
	if node.Annotations[v1alpha1.AnnotationAgentBootID] != a.config.HostBootID {
		currentStatus = ""
	}
	switch stage {
	case v1alpha1.StageDrain:
		if currentStatus == v1alpha1.AgentStatusDrained || currentStatus == v1alpha1.AgentStatusDraining {
			return nil
		}
		if err := a.updateAgentStatus(ctx, node, v1alpha1.AgentStatusDraining); err != nil {
			return err
		}
		timeoutStr := node.Annotations[v1alpha1.AnnotationDrainTimeout]
		if timeoutStr == "" {
			timeoutStr = v1alpha1.DefaultDrainTimeout
		}
		timeout, err := time.ParseDuration(timeoutStr)
		if err != nil {
			slog.Warn("Failed to parse drain timeout, falling back to 90s", "val", timeoutStr, "error", err)
			timeout = 90 * time.Second
		}
		if err := drain.ExecuteEscalatingDrain(ctx, a.client, a.config.NodeName, timeout); err != nil {
			if statusErr := a.updateAgentStatus(ctx, node, v1alpha1.AgentStatusFailed); statusErr != nil {
				slog.Warn("Failed to acknowledge drain failure", "error", statusErr)
			}
			return err
		}
		return a.updateAgentStatus(ctx, node, v1alpha1.AgentStatusDrained)
	case v1alpha1.StageHalt:
		if currentStatus == v1alpha1.AgentStatusHalting {
			return nil
		}
		if a.powerController == nil {
			return fmt.Errorf("host power controller is unavailable")
		}
		if err := a.updateAgentStatus(ctx, node, v1alpha1.AgentStatusHalting); err != nil {
			return err
		}
		slog.Warn("Executing boot-bound halt directive", "node", a.config.NodeName, "bootID", a.config.HostBootID)
		a.powerController.HaltHost(ctx)
	case "", v1alpha1.StageIdle:
		if currentStatus != v1alpha1.AgentStatusReady {
			return a.updateAgentStatus(ctx, node, v1alpha1.AgentStatusReady)
		}
	}
	return nil
}

func (a *Agent) updateAgentStatus(ctx context.Context, directive *corev1.Node, status string) error {
	current, err := a.client.CoreV1().Nodes().Get(ctx, a.config.NodeName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if current.UID != directive.UID ||
		current.Annotations[v1alpha1.AnnotationStage] != directive.Annotations[v1alpha1.AnnotationStage] ||
		current.Annotations[v1alpha1.AnnotationDirectiveBootID] != directive.Annotations[v1alpha1.AnnotationDirectiveBootID] ||
		current.Annotations[v1alpha1.AnnotationShutdownUID] != directive.Annotations[v1alpha1.AnnotationShutdownUID] {
		return fmt.Errorf("shutdown directive changed before acknowledgement")
	}
	patchData := map[string]interface{}{
		"metadata": map[string]interface{}{
			"resourceVersion": current.ResourceVersion,
			"annotations": map[string]string{
				v1alpha1.AnnotationAgentStatus: status,
				v1alpha1.AnnotationAgentBootID: a.config.HostBootID,
			},
		},
	}
	data, err := json.Marshal(patchData)
	if err != nil {
		return err
	}
	_, err = a.client.CoreV1().Nodes().Patch(ctx, a.config.NodeName, types.MergePatchType, data, metav1.PatchOptions{})
	return err
}

func (a *Agent) clearStaleDirective(ctx context.Context, node *corev1.Node) error {
	patch := map[string]interface{}{
		"metadata": map[string]interface{}{
			"resourceVersion": node.ResourceVersion,
			"annotations": map[string]interface{}{
				v1alpha1.AnnotationStage:           nil,
				v1alpha1.AnnotationDrainTimeout:    nil,
				v1alpha1.AnnotationDirectiveBootID: nil,
				v1alpha1.AnnotationShutdownUID:     nil,
				v1alpha1.AnnotationAgentStatus:     v1alpha1.AgentStatusReady,
				v1alpha1.AnnotationAgentBootID:     a.config.HostBootID,
			},
		},
	}
	data, err := json.Marshal(patch)
	if err != nil {
		return err
	}
	_, err = a.client.CoreV1().Nodes().Patch(ctx, a.config.NodeName, types.MergePatchType, data, metav1.PatchOptions{})
	return err
}
