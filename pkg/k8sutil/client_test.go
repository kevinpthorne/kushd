package k8sutil

import (
	"os"
	"path/filepath"
	"testing"
)

func TestStrictTLSEnforcement(t *testing.T) {
	tempDir := t.TempDir()
	insecureKubeconfig := filepath.Join(tempDir, "insecure-config")

	// Create a kubeconfig with insecure-skip-tls-verify: true
	configContent := `
apiVersion: v1
kind: Config
clusters:
- cluster:
    server: https://127.0.0.1:6443
    insecure-skip-tls-verify: true
  name: test-cluster
contexts:
- context:
    cluster: test-cluster
    user: test-user
  name: test-context
current-context: test-context
users:
- name: test-user
`
	if err := os.WriteFile(insecureKubeconfig, []byte(configContent), 0600); err != nil {
		t.Fatalf("failed to write insecure kubeconfig: %v", err)
	}

	t.Setenv("KUBECONFIG", insecureKubeconfig)

	_, err := GetKubeConfig()
	if err == nil {
		t.Fatalf("expected error when loading kubeconfig with insecure TLS, but succeeded")
	}
}
