//go:build !linux

package power

import "syscall"

func rawRebootPowerOff() error {
	syscall.Sync()
	return nil
}
