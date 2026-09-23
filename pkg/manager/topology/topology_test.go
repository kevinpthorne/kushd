package topology

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"kushd/pkg/apis/kushd/v1alpha1"
)

func TestValidateAnchorTopology(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name      string
		node      *corev1.Node
		nodeName  string
		expectErr bool
	}{
		{
			name: "Control plane node passes",
			node: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "cp-1",
					Labels: map[string]string{
						v1alpha1.LabelNodeRoleControlPlane: "",
					},
				},
			},
			nodeName:  "cp-1",
			expectErr: false,
		},
		{
			name: "Master role node passes",
			node: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "master-1",
					Labels: map[string]string{
						v1alpha1.LabelNodeRoleMaster: "",
					},
				},
			},
			nodeName:  "master-1",
			expectErr: false,
		},
		{
			name: "Worker node violates topology invariant",
			node: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "worker-1",
					Labels: map[string]string{
						"node.kubernetes.io/instance-type": "standard",
					},
				},
			},
			nodeName:  "worker-1",
			expectErr: true,
		},
		{
			name:      "Non-existent node errors",
			node:      nil,
			nodeName:  "ghost-node",
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var client *fake.Clientset
			if tt.node != nil {
				client = fake.NewSimpleClientset(tt.node)
			} else {
				client = fake.NewSimpleClientset()
			}

			err := ValidateAnchorTopology(ctx, client, tt.nodeName)
			if (err != nil) != tt.expectErr {
				t.Fatalf("expected error: %v, got: %v", tt.expectErr, err)
			}
		})
	}
}
