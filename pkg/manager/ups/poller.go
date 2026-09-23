package ups

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"

	"kushd/pkg/apis/kushd/v1alpha1"
)

type PollerConfig struct {
	AnchorNode            string
	TriggerSourceName     string
	PollInterval          time.Duration
	BatteryLowPercent     int
	RuntimeLowSeconds     int
	EnableKillpower       bool
	KillpowerDelaySeconds int
	TargetOutletGroup     string
}

func DefaultPollerConfig() PollerConfig {
	return PollerConfig{
		TriggerSourceName:     "UPS-Hardware",
		PollInterval:          5 * time.Second,
		BatteryLowPercent:     20,
		RuntimeLowSeconds:     300,
		EnableKillpower:       false,
		KillpowerDelaySeconds: 180,
	}
}

type HardwarePoller struct {
	dynClient dynamic.Interface
	upsClient UPSClient
	config    PollerConfig
}

func NewHardwarePoller(dynClient dynamic.Interface, upsClient UPSClient, config PollerConfig) *HardwarePoller {
	if config.PollInterval <= 0 {
		config.PollInterval = 5 * time.Second
	}
	if config.BatteryLowPercent <= 0 {
		config.BatteryLowPercent = 20
	}
	if config.RuntimeLowSeconds <= 0 {
		config.RuntimeLowSeconds = 300
	}
	if config.KillpowerDelaySeconds <= 0 {
		config.KillpowerDelaySeconds = 180
	}
	return &HardwarePoller{
		dynClient: dynClient,
		upsClient: upsClient,
		config:    config,
	}
}

// Run starts the hardware poller loop.
func (p *HardwarePoller) Run(ctx context.Context) error {
	slog.Info("Starting UPS hardware poller loop",
		"triggerSource", p.config.TriggerSourceName,
		"batteryLowThreshold", p.config.BatteryLowPercent,
		"runtimeLowThreshold", p.config.RuntimeLowSeconds,
	)

	ticker := time.NewTicker(p.config.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("Stopping UPS hardware poller loop")
			return ctx.Err()
		case <-ticker.C:
			if err := p.pollOnce(ctx); err != nil {
				slog.Warn("Hardware poller iteration encountered error", "error", err)
			}
		}
	}
}

func (p *HardwarePoller) pollOnce(ctx context.Context) error {
	status, err := p.upsClient.GetStatus(ctx)
	if err != nil {
		return fmt.Errorf("failed to fetch UPS status: %w", err)
	}

	isLowBatt := strings.Contains(status.State, "LB") ||
		strings.Contains(status.State, "LOWBATT") ||
		(status.BatteryChargePercent > 0 && status.BatteryChargePercent <= p.config.BatteryLowPercent) ||
		(status.EstimatedRuntimeSeconds > 0 && status.EstimatedRuntimeSeconds <= p.config.RuntimeLowSeconds)

	isOnline := strings.Contains(status.State, "OL")

	slog.Debug("Polled UPS status",
		"state", status.State,
		"charge", status.BatteryChargePercent,
		"runtime", status.EstimatedRuntimeSeconds,
		"isLowBatt", isLowBatt,
		"isOnline", isOnline,
	)

	activeCSD, err := p.getActiveClusterShutdown(ctx)
	if err != nil {
		return err
	}

	if isLowBatt {
		if activeCSD == nil {
			slog.Warn("UPS LOWBATT condition detected! Triggering new ClusterShutdown",
				"batteryCharge", status.BatteryChargePercent,
				"runtime", status.EstimatedRuntimeSeconds,
			)
			return p.createClusterShutdown(ctx, status)
		}
	} else if isOnline && activeCSD != nil {
		// Mains restored
		phase := v1alpha1.ClusterShutdownPhase(activeCSD.Status.Phase)
		if phase == v1alpha1.PhasePending || phase == v1alpha1.PhaseCordoningCluster {
			if !activeCSD.Spec.Abort {
				slog.Info("Mains power restored (OL) before Point-of-No-Return; signaling abort", "phase", phase)
				return p.setAbort(ctx, activeCSD.Name)
			}
		}
	}

	return nil
}

func (p *HardwarePoller) getActiveClusterShutdown(ctx context.Context) (*v1alpha1.ClusterShutdown, error) {
	list, err := p.dynClient.Resource(v1alpha1.ClusterShutdownGVR).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to list clustershutdowns: %w", err)
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

		switch csd.Status.Phase {
		case v1alpha1.PhaseCompleted, v1alpha1.PhaseAborted, v1alpha1.PhaseFailed:
			// Not active
			continue
		default:
			return &csd, nil
		}
	}

	return nil, nil
}

func (p *HardwarePoller) createClusterShutdown(ctx context.Context, status *UPSStatus) error {
	now := metav1.Now()
	name := fmt.Sprintf("ups-event-%d", time.Now().Unix())

	csd := &v1alpha1.ClusterShutdown{
		TypeMeta: metav1.TypeMeta{
			APIVersion: fmt.Sprintf("%s/%s", v1alpha1.GroupName, v1alpha1.Version),
			Kind:       "ClusterShutdown",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: v1alpha1.ClusterShutdownSpec{
			TriggerSource:           p.config.TriggerSourceName,
			BatteryChargePercent:    status.BatteryChargePercent,
			EstimatedRuntimeSeconds: status.EstimatedRuntimeSeconds,
			Abort:                   false,
			DryRun:                  false,
			PowerManagement: v1alpha1.PowerManagementSpec{
				EnableKillpower:       p.config.EnableKillpower,
				KillpowerDelaySeconds: p.config.KillpowerDelaySeconds,
				TargetOutletGroup:     p.config.TargetOutletGroup,
			},
			Timeouts: v1alpha1.TimeoutsSpec{
				WorkerEvacuationSeconds:       v1alpha1.DefaultWorkerEvacuationSeconds,
				ControlPlaneEvacuationSeconds: v1alpha1.DefaultControlPlaneEvacuationSecs,
			},
		},
		Status: v1alpha1.ClusterShutdownStatus{
			Phase:      v1alpha1.PhasePending,
			StartTime:  &now,
			AnchorNode: p.config.AnchorNode,
		},
	}

	data, err := json.Marshal(csd)
	if err != nil {
		return err
	}

	var u unstructured.Unstructured
	if err := json.Unmarshal(data, &u.Object); err != nil {
		return err
	}

	_, err = p.dynClient.Resource(v1alpha1.ClusterShutdownGVR).Create(ctx, &u, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("failed to create ClusterShutdown CR: %w", err)
	}

	slog.Info("Successfully created ClusterShutdown CR", "name", name)
	return nil
}

func (p *HardwarePoller) setAbort(ctx context.Context, name string) error {
	item, err := p.dynClient.Resource(v1alpha1.ClusterShutdownGVR).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return err
	}

	spec, ok := item.Object["spec"].(map[string]interface{})
	if !ok {
		spec = make(map[string]interface{})
	}
	spec["abort"] = true
	item.Object["spec"] = spec

	_, err = p.dynClient.Resource(v1alpha1.ClusterShutdownGVR).Update(ctx, item, metav1.UpdateOptions{})
	return err
}
