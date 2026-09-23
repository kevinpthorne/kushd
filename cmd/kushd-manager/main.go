package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"kushd/pkg/k8sutil"
	"kushd/pkg/manager/reconciler"
	"kushd/pkg/manager/topology"
	"kushd/pkg/manager/ups"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	nodeName := os.Getenv("NODE_NAME")
	if nodeName == "" {
		slog.Error("FATAL: NODE_NAME environment variable must be specified")
		os.Exit(1)
	}

	enableKillpower := os.Getenv("ENABLE_KILLPOWER") == "true"
	killpowerDelaySec := 180
	if val := os.Getenv("KILLPOWER_DELAY_SECONDS"); val != "" {
		if parsed, err := strconv.Atoi(val); err == nil && parsed > 0 {
			killpowerDelaySec = parsed
		}
	}
	targetOutletGroup := os.Getenv("TARGET_OUTLET_GROUP")

	batteryLowPercent := 20
	if val := os.Getenv("BATTERY_LOW_PERCENT"); val != "" {
		if parsed, err := strconv.Atoi(val); err == nil && parsed > 0 {
			batteryLowPercent = parsed
		}
	}

	runtimeLowSeconds := 300
	if val := os.Getenv("RUNTIME_LOW_SECONDS"); val != "" {
		if parsed, err := strconv.Atoi(val); err == nil && parsed > 0 {
			runtimeLowSeconds = parsed
		}
	}

	upsName := os.Getenv("UPS_NAME")
	if upsName == "" {
		upsName = "ups"
	}

	nutHost := os.Getenv("NUT_HOST")
	if nutHost == "" {
		nutHost = "localhost"
	}

	nutPort := 3493
	if val := os.Getenv("NUT_PORT"); val != "" {
		if parsed, err := strconv.Atoi(val); err == nil && parsed > 0 {
			nutPort = parsed
		}
	}

	dryRun := os.Getenv("DRY_RUN") == "true"

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Initialize secure Kubernetes clients (STRICT TLS ENFORCEMENT)
	clientset, _, err := k8sutil.NewClientset()
	if err != nil {
		slog.Error("Failed to initialize Kubernetes clientset with strict TLS", "error", err)
		os.Exit(1)
	}

	dynClient, _, err := k8sutil.NewDynamicClient()
	if err != nil {
		slog.Error("Failed to initialize dynamic Kubernetes client with strict TLS", "error", err)
		os.Exit(1)
	}

	// 1. Startup Topology Guard
	topology.ValidateAnchorTopologyOrExit(ctx, clientset, nodeName)

	// 2. Instantiate UPS Hardware Client
	nutClient := ups.NewNUTClient(upsName, nutHost, nutPort)

	// 3. Hardware Poller Loop
	pollerConfig := ups.PollerConfig{
		AnchorNode:            nodeName,
		TriggerSourceName:     upsName,
		PollInterval:          5 * time.Second,
		BatteryLowPercent:     batteryLowPercent,
		RuntimeLowSeconds:     runtimeLowSeconds,
		EnableKillpower:       enableKillpower,
		KillpowerDelaySeconds: killpowerDelaySec,
		TargetOutletGroup:     targetOutletGroup,
	}
	hardwarePoller := ups.NewHardwarePoller(dynClient, nutClient, pollerConfig)

	// 4. Cluster Reconciler Loop
	reconcilerConfig := reconciler.ReconcilerConfig{
		AnchorNode:   nodeName,
		PollInterval: 2 * time.Second,
		DryRun:       dryRun,
	}
	clusterReconciler := reconciler.NewClusterReconciler(clientset, dynClient, nutClient, reconcilerConfig)

	// Launch concurrent loops
	errChan := make(chan error, 2)

	go func() {
		if err := hardwarePoller.Run(ctx); err != nil && err != context.Canceled {
			errChan <- err
		}
	}()

	go func() {
		if err := clusterReconciler.Run(ctx); err != nil && err != context.Canceled {
			errChan <- err
		}
	}()

	slog.Info("kushd-manager running (hardware poller + reconciler active)", "node", nodeName)

	select {
	case <-ctx.Done():
		slog.Info("kushd-manager received shutdown signal")
	case err := <-errChan:
		slog.Error("kushd-manager loop exited with error", "error", err)
		os.Exit(1)
	}
}
