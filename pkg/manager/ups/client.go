package ups

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"os/exec"
	"strconv"
	"strings"
)

type UPSStatus struct {
	State                   string // "OL" (online), "OB" (on battery), "LB" (low battery)
	BatteryChargePercent    int
	EstimatedRuntimeSeconds int
}

type UPSClient interface {
	GetStatus(ctx context.Context) (*UPSStatus, error)
	Killpower(ctx context.Context, delaySeconds int, targetOutletGroup string) error
}

// NUTClient queries NUT (Network UPS Tools) via upsc/upscmd CLI or network daemon.
type NUTClient struct {
	UPSName string
	Host    string
	Port    int
}

func NewNUTClient(upsName, host string, port int) *NUTClient {
	if upsName == "" {
		upsName = "ups"
	}
	if host == "" {
		host = "localhost"
	}
	if port <= 0 {
		port = 3493
	}
	return &NUTClient{
		UPSName: upsName,
		Host:    host,
		Port:    port,
	}
}

func (c *NUTClient) target() string {
	return fmt.Sprintf("%s@%s:%d", c.UPSName, c.Host, c.Port)
}

func (c *NUTClient) GetStatus(ctx context.Context) (*UPSStatus, error) {
	// Execute upsc command
	cmd := exec.CommandContext(ctx, "upsc", c.target())
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("upsc %s execution failed: %w", c.target(), err)
	}

	status := &UPSStatus{
		State:                   "OL",
		BatteryChargePercent:    100,
		EstimatedRuntimeSeconds: 3600,
	}

	scanner := bufio.NewScanner(strings.NewReader(string(output)))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		val := strings.TrimSpace(parts[1])

		switch key {
		case "ups.status":
			status.State = val
		case "battery.charge":
			if charge, err := strconv.Atoi(val); err == nil {
				status.BatteryChargePercent = charge
			}
		case "battery.runtime":
			if runtimeSec, err := strconv.Atoi(val); err == nil {
				status.EstimatedRuntimeSeconds = runtimeSec
			}
		}
	}

	return status, nil
}

func (c *NUTClient) Killpower(ctx context.Context, delaySeconds int, targetOutletGroup string) error {
	slog.Info("Sending UPS killpower command via upscmd",
		"ups", c.target(),
		"delaySeconds", delaySeconds,
		"targetOutletGroup", targetOutletGroup,
	)

	// Send load.off.delay command via upscmd
	cmdArgs := []string{c.target(), "load.off.delay", strconv.Itoa(delaySeconds)}
	if targetOutletGroup != "" {
		cmdArgs = append(cmdArgs, targetOutletGroup)
	}

	cmd := exec.CommandContext(ctx, "upscmd", cmdArgs...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("upscmd failed: %s (%w)", string(out), err)
	}

	return nil
}
