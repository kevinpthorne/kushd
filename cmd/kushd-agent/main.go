package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"kushd/pkg/agent"
	"kushd/pkg/agent/power"
	"kushd/pkg/k8sutil"
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

	hostRootPath := os.Getenv("HOST_ROOT_PATH")
	if hostRootPath == "" {
		hostRootPath = "/host"
	}
	dbusSocketPath := os.Getenv("DBUS_SOCKET_PATH")
	if dbusSocketPath == "" {
		dbusSocketPath = "/host/run/dbus/system_bus_socket"
	}
	sysrqTriggerPath := os.Getenv("SYSRQ_TRIGGER_PATH")
	if sysrqTriggerPath == "" {
		sysrqTriggerPath = "/host/proc/sysrq-trigger"
	}

	// Initialize secure Kubernetes client (strict TLS)
	clientset, _, err := k8sutil.NewClientset()
	if err != nil {
		slog.Error("Failed to initialize Kubernetes client with strict TLS", "error", err)
		os.Exit(1)
	}

	powerOpts := power.ControllerOptions{
		DBusSocketPath:   dbusSocketPath,
		HostRootPath:     hostRootPath,
		SysrqTriggerPath: sysrqTriggerPath,
		SyncTimeout:      5 * time.Second,
	}

	powerController, err := power.NewHostPowerController(powerOpts)
	if err != nil {
		slog.Error("Failed to initialize HostPowerController", "error", err)
		os.Exit(1)
	}
	defer powerController.Close()

	agentConfig := agent.AgentConfig{
		NodeName:            nodeName,
		PollInterval:        2 * time.Second,
		PowerControllerOpts: powerOpts,
	}

	kushdAgent := agent.NewAgent(clientset, agentConfig, powerController)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := kushdAgent.Run(ctx); err != nil && err != context.Canceled {
		slog.Error("kushd-agent terminated with error", "error", err)
		os.Exit(1)
	}
}
