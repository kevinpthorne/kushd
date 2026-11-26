package ups

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"

	"kushd/pkg/apis/kushd/v1alpha1"
)

func TestShutdownRequiresUnambiguousOnBattery(t *testing.T) {
	for _, tc := range []struct {
		name, state     string
		charge, runtime int
		wantShutdown    bool
	}{
		{"incident on mains", "OL", 100, 288, false},
		{"low battery flag on mains", "OL LB", 10, 90, false},
		{"contradictory power state", "OL OB LB", 10, 90, false},
		{"unknown state", "UNKNOWN", 10, 90, false},
		{"not a state token", "FOOB LOWBATTERY", 10, 90, false},
		{"missing state", "", 10, 90, false},
		{"battery low flag", "OB LB", 100, 1000, true},
		{"battery low charge", "OB", 15, 1000, true},
		{"battery short runtime", "OB", 100, 288, true},
		{"battery healthy", "OB", 100, 1000, false},
		{"battery missing readings", "OB", 0, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
				v1alpha1.ClusterShutdownGVR: "ClusterShutdownList",
			})
			ups := &mockPollerUPSClient{status: &UPSStatus{State: tc.state, BatteryChargePercent: tc.charge, EstimatedRuntimeSeconds: tc.runtime}}
			poller := NewHardwarePoller(dyn, ups, DefaultPollerConfig())
			ctx := context.Background()
			if err := poller.pollOnce(ctx); err != nil {
				t.Fatal(err)
			}
			list, err := dyn.Resource(v1alpha1.ClusterShutdownGVR).List(ctx, metav1.ListOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if (len(list.Items) > 0) != tc.wantShutdown {
				t.Fatalf("shutdown created = %v, want %v", len(list.Items) > 0, tc.wantShutdown)
			}
			if tc.wantShutdown {
				if list.Items[0].GetAnnotations()["kushd.io/ups-state"] != tc.state {
					t.Fatal("trigger lost its original UPS state")
				}
			}
		})
	}
}
