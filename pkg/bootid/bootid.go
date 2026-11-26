package bootid

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Read returns the kernel boot ID from the mounted host filesystem, never a
// container-local fallback. Call once at process startup: a reboot ends the process.
func Read(hostRoot string) (string, error) {
	if !filepath.IsAbs(hostRoot) {
		return "", fmt.Errorf("host root must be absolute")
	}
	data, err := os.ReadFile(filepath.Join(hostRoot, "proc/sys/kernel/random/boot_id"))
	if err != nil {
		return "", fmt.Errorf("read host boot ID: %w", err)
	}
	id := strings.TrimSpace(string(data))
	if !Valid(id) {
		return "", fmt.Errorf("invalid host boot ID")
	}
	return id, nil
}

// Valid recognizes the canonical UUID representation supplied by the kernel.
func Valid(id string) bool {
	if len(id) != 36 {
		return false
	}
	for i := range len(id) {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			if id[i] != '-' {
				return false
			}
		} else if !((id[i] >= '0' && id[i] <= '9') || (id[i] >= 'a' && id[i] <= 'f')) {
			return false
		}
	}
	return true
}
