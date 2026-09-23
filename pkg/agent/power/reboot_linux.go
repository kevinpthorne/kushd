//go:build linux

package power

import "syscall"

func rawRebootPowerOff() error {
	return syscall.Reboot(syscall.LINUX_REBOOT_CMD_POWER_OFF)
}
