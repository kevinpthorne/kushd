package power

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"syscall"
	"time"

	"github.com/godbus/dbus/v5"
)

type ControllerOptions struct {
	DBusSocketPath   string
	HostRootPath     string
	SysrqTriggerPath string
	SyncTimeout      time.Duration
}

func DefaultOptions() ControllerOptions {
	return ControllerOptions{
		DBusSocketPath:   "/host/run/dbus/system_bus_socket",
		HostRootPath:     "/host",
		SysrqTriggerPath: "/host/proc/sysrq-trigger",
		SyncTimeout:      5 * time.Second,
	}
}

type HostPowerController struct {
	opts ControllerOptions
	conn *dbus.Conn
}

func NewHostPowerController(opts ControllerOptions) (*HostPowerController, error) {
	if opts.DBusSocketPath == "" {
		opts.DBusSocketPath = "/host/run/dbus/system_bus_socket"
	}
	if opts.HostRootPath == "" {
		opts.HostRootPath = "/host"
	}
	if opts.SysrqTriggerPath == "" {
		opts.SysrqTriggerPath = "/host/proc/sysrq-trigger"
	}
	if opts.SyncTimeout <= 0 {
		opts.SyncTimeout = 5 * time.Second
	}

	conn, err := dbus.Dial(fmt.Sprintf("unix:path=%s", opts.DBusSocketPath))
	if err != nil {
		slog.Warn("Failed to bind host D-Bus socket; will rely on fallback tiers", "error", err, "path", opts.DBusSocketPath)
		return &HostPowerController{opts: opts, conn: nil}, nil
	}
	return &HostPowerController{opts: opts, conn: conn}, nil
}

func (h *HostPowerController) Close() {
	if h.conn != nil {
		_ = h.conn.Close()
	}
}

// HaltHost implements the Three-Tier Poweroff Escalation Ladder.
func (h *HostPowerController) HaltHost(ctx context.Context) {
	slog.Info("Initiating host storage buffer sync...")

	// 5-second deadline ensures hung network mounts do not block the agent process
	syncDone := make(chan struct{})
	go func() {
		syscall.Sync()
		close(syncDone)
	}()

	select {
	case <-syncDone:
		slog.Info("Storage buffers successfully synced to disk")
	case <-time.After(h.opts.SyncTimeout):
		slog.Warn("Storage sync timed out; advancing to systemd poweroff to prevent power depletion", "timeout", h.opts.SyncTimeout)
	}

	// Tier 1: Primary D-Bus Invocation
	if h.conn != nil {
		slog.Info("Tier 1: Invoking systemd PowerOff via D-Bus...")
		systemd := h.conn.Object("org.freedesktop.systemd1", "/org/freedesktop/systemd1")
		call := systemd.CallWithContext(ctx, "org.freedesktop.systemd1.Manager.PowerOff", 0)
		if call.Err == nil {
			slog.Info("D-Bus PowerOff successfully dispatched")
			return
		}
		slog.Error("Tier 1 Failed: D-Bus call rejected", "error", call.Err)
	}

	// Tier 2: chroot systemctl Fallback
	slog.Warn("Tier 2: Escalating to chroot systemctl fallback...")
	cmd := exec.CommandContext(ctx, "chroot", h.opts.HostRootPath, "/usr/bin/systemctl", "poweroff", "--force", "--force")
	if err := cmd.Run(); err == nil {
		slog.Info("chroot systemctl poweroff successfully executed")
		return
	} else {
		slog.Error("Tier 2 Failed: chroot systemctl execution failed", "error", err)
	}

	// Tier 3: Hard Fencing / STONITH
	// If the host cannot be powered off cleanly, force a kernel crash to eliminate split-brain risk
	h.FenceNode()
}

// FenceNode writes 'c' to sysrq-trigger, triggering a kernel panic.
func (h *HostPowerController) FenceNode() {
	slog.Error("CRITICAL: All clean poweroff mechanisms failed. Triggering immediate STONITH kernel crash.")

	// sysrq-trigger 'c' performs an immediate crash dump/kernel panic, instantly halting node execution
	err := os.WriteFile(h.opts.SysrqTriggerPath, []byte("c"), 0200)
	if err != nil {
		slog.Error("Failed to write to sysrq-trigger; issuing raw reboot syscall", "error", err, "path", h.opts.SysrqTriggerPath)
		// Last-ditch: direct kernel reboot syscall via LINUX_REBOOT_CMD_POWER_OFF
		_ = rawRebootPowerOff()
	}
}
