package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"

	"kushd/pkg/agent/drain"
	"kushd/pkg/agent/power"
	"kushd/pkg/apis/kushd/v1alpha1"
)

type AgentConfig struct {
	NodeName           string
	PollInterval       time.Duration
	PowerControllerOpts power.ControllerOptions
}

type Agent struct {
	client          kubernetes.Interface
	config          AgentConfig
	powerController *power.HostPowerController
}

func NewAgent(client kubernetes.Interface, config AgentConfig, powerController *power.HostPowerController) *Agent {
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
	slog.Info("Starting kushd-agent", "node", a.config.NodeName)

	// Set initial status to ready
	if err := a.updateAgentStatus(ctx, v1alpha1.AgentStatusReady); err != nil {
		slog.Warn("Failed to set initial agent status on node", "error", err)
	}

	ticker := time.NewTicker(a.config.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("Shutting down kushd-agent loop")
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
	node, err := a.client.CoreV1().Nodes().Get(ctx, a.config.NodeName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get node %s: %w", a.config.NodeName, err)
	}

	stage := node.Annotations[v1alpha1.AnnotationStage]
	currentStatus := node.Annotations[v1alpha1.AnnotationAgentStatus]

	switch stage {
	case v1alpha1.StageDrain:
		if currentStatus == v1alpha1.AgentStatusDrained || currentStatus == v1alpha1.AgentStatusDraining {
			return nil
		}

		slog.Info("Drain directive received", "node", a.config.NodeName)
		if err := a.updateAgentStatus(ctx, v1alpha1.AgentStatusDraining); err != nil {
			slog.Warn("Failed to update status to draining", "error", err)
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
			slog.Error("Drain execution failed", "error", err)
			_ = a.updateAgentStatus(ctx, v1alpha1.AgentStatusFailed)
			return err
		}

		slog.Info("Drain completed successfully", "node", a.config.NodeName)
		return a.updateAgentStatus(ctx, v1alpha1.AgentStatusDrained)

	case v1alpha1.StageHalt:
		if currentStatus == v1alpha1.AgentStatusHalting {
			return nil
		}

		slog.Warn("Halt directive received; preparing host poweroff", "node", a.config.NodeName)
		if err := a.updateAgentStatus(ctx, v1alpha1.AgentStatusHalting); err != nil {
			slog.Warn("Failed to update status to halting", "error", err)
		}

		// Execute host power off escalation ladder
		a.powerController.HaltHost(ctx)
		return nil

	case v1alpha1.StageIdle:
		if currentStatus != v1alpha1.AgentStatusReady {
			return a.updateAgentStatus(ctx, v1alpha1.AgentStatusReady)
		}
	}

	return nil
}

func (a *Agent) updateAgentStatus(ctx context.Context, status string) error {
	patchData := map[string]interface{}{
		"metadata": map[string]interface{}{
			"annotations": map[string]string{
				v1alpha1.AnnotationAgentStatus: status,
			},
		},
	}

	bytes, err := json.Marshal(patchData)
	if err != nil {
		return err
	}

	_, err = a.client.CoreV1().Nodes().Patch(ctx, a.config.NodeName, types.MergePatchType, bytes, metav1.PatchOptions{})
	return err
}
