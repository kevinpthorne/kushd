package k8sutil

import (
	"fmt"
	"log/slog"
	"os"

	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// GetKubeConfig returns a Kubernetes REST config, strictly enforcing TLS verification.
// InsecureSkipTLSVerify is strictly forbidden under all circumstances.
func GetKubeConfig() (*rest.Config, error) {
	var config *rest.Config
	var err error

	kubeconfigPath := os.Getenv("KUBECONFIG")
	if kubeconfigPath != "" {
		config, err = clientcmd.BuildConfigFromFlags("", kubeconfigPath)
		if err != nil {
			return nil, fmt.Errorf("failed to load kubeconfig from %s: %w", kubeconfigPath, err)
		}
	} else {
		// Fallback to in-cluster config
		config, err = rest.InClusterConfig()
		if err != nil {
			// If in-cluster fails, try standard default kubeconfig path
			home, hErr := os.UserHomeDir()
			if hErr == nil {
				defaultPath := fmt.Sprintf("%s/.kube/config", home)
				if _, statErr := os.Stat(defaultPath); statErr == nil {
					config, err = clientcmd.BuildConfigFromFlags("", defaultPath)
				}
			}
			if config == nil {
				return nil, fmt.Errorf("unable to load in-cluster or local kubeconfig: %w", err)
			}
		}
	}

	// STRICT TLS ENFORCEMENT: Never permit insecure TLS verification
	if config.TLSClientConfig.Insecure {
		slog.Error("CRITICAL SECURITY ERROR: Insecure TLS verification detected in kubeconfig. Aborting.")
		return nil, fmt.Errorf("insecure TLS verification is prohibited by security policy")
	}

	// Explicitly confirm TLS verification is enabled
	config.TLSClientConfig.Insecure = false

	return config, nil
}

// NewClientset creates a kubernetes.Clientset with strict TLS verification.
func NewClientset() (*kubernetes.Clientset, *rest.Config, error) {
	config, err := GetKubeConfig()
	if err != nil {
		return nil, nil, err
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create kubernetes clientset: %w", err)
	}

	return clientset, config, nil
}

// NewDynamicClient creates a dynamic.Interface with strict TLS verification.
func NewDynamicClient() (dynamic.Interface, *rest.Config, error) {
	config, err := GetKubeConfig()
	if err != nil {
		return nil, nil, err
	}

	dynClient, err := dynamic.NewForConfig(config)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create dynamic client: %w", err)
	}

	return dynClient, config, nil
}
