package power

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestHostPowerController_STONITH(t *testing.T) {
	tempDir := t.TempDir()
	sysrqPath := filepath.Join(tempDir, "sysrq-trigger")

	// Pre-create the sysrq-trigger mock file
	if err := os.WriteFile(sysrqPath, []byte(""), 0644); err != nil {
		t.Fatalf("failed to create temp sysrq file: %v", err)
	}

	opts := ControllerOptions{
		DBusSocketPath:   filepath.Join(tempDir, "nonexistent.sock"),
		HostRootPath:     tempDir,
		SysrqTriggerPath: sysrqPath,
		SyncTimeout:      50 * time.Millisecond,
	}

	controller, err := NewHostPowerController(opts)
	if err != nil {
		t.Fatalf("failed to create controller: %v", err)
	}
	defer controller.Close()

	// HaltHost should fail Tier 1 (missing DBus socket) and Tier 2 (missing chroot/systemctl),
	// and fall through to Tier 3 STONITH, writing 'c' to sysrq-trigger.
	controller.HaltHost(context.Background())

	content, err := os.ReadFile(sysrqPath)
	if err != nil {
		t.Fatalf("failed to read sysrq file: %v", err)
	}

	if string(content) != "c" {
		t.Fatalf("expected sysrq-trigger to contain 'c', got %q", string(content))
	}
}
