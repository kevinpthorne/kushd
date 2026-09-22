# Kushd (Kubernetes Shutdown Daemon) Engineering Specification

**Document Version:** 1.0.0-RC4

**Status:** Ready for Review

**Target Environments:** Bare-Metal & Edge Kubernetes (v1.28+)

**Primary Subsystems:** Consolidated Daemon Manager (`kushd-manager`), Host Node Daemon (`kushd-agent`), Akri Discovery Integration

---

## 1. Executive Summary & Core Invariants

Kushd is a topology-aware, event-driven shutdown orchestrator for Kubernetes clusters backed by Uninterruptible Power Supply (UPS) hardware. Abrupt power failure introduces severe data corruption risks: `etcd` split-brain states, write-cache truncation on distributed block storage (Ceph, Longhorn, local NVMe/SATA disks), and ungraceful pod termination.

Kushd coordinates an ordered, priority-aware cluster decommissioning down to the host OS level (`systemd`), while protecting auxiliary rack infrastructure (networking, storage appliances) via an opt-in power-cut model.

Hardware discovery and device node injection are delegated entirely to **Akri via the Kubernetes Device Plugin API**, eliminating custom host-level `udev` rules and dynamic symlink management. Cluster management and hardware polling are consolidated into a single control plane binary: **`kushd-manager`**.

```
                         [UPS Hardware]
                         (USB / Serial)
                               │
                               ▼
                    [Akri Discovery Handler]
                               │ (Discovers 051d:0002)
                               ▼
               [Node Extended Resource: akri.sh/ups: 1]
                               │
                               ▼ (Kubelet Schedules & Mounts)
                 ┌───────────────────────────┐
                 │ Control Plane Anchor Node │
                 │                           │
                 │       kushd-manager       │
                 │   (Poller + Reconciler)   │
                 └─────────────┬─────────────┘
                               │
                  Phase 1: Worker Nodes (Parallel)
                               │
                               ▼
                      ┌─────────────────┐
                      │   kushd-agent   │
                      │  (Worker Node)  │
                      └────────┬────────┘
                               ▼
                      [systemd / D-Bus]
                      [Host Poweroff  ] ──(Failure)──► [STONITH Kernel Panic]
                               │
                  Phase 2: Control Plane (Sequential N-1)
                               │
                               ▼
                      ┌─────────────────┐
                      │   kushd-agent   │
                      │  (CP Follower)  │
                      └────────┬────────┘
                               ▼
                      [systemd / D-Bus]
                      [Host Poweroff  ] ──(Failure)──► [STONITH Kernel Panic]
                               │
                  Phase 3: Final Anchor Decommission
                               │
                               ▼
                    [Optional UPS Killpower]
                    [Anchor Host Poweroff  ]

```

### System Invariants

1. **Topology Placement Invariant:** Physical UPS communication hardware (USB/Serial) **must** terminate on a control-plane node. Kushd enforces an in-process, clean exit (`os.Exit(1)`) without stack trace pollution if scheduled on a worker node.
2. **Akri-Driven Implicit Anchor Scheduling:** `kushd-manager` requests the extended resource `akri.sh/ups: 1` alongside a control-plane `nodeSelector`. The Kubernetes scheduler automatically pins the manager to the exact control-plane node where the UPS is physically connected, eliminating manual node labeling (`kushd.io/ups-anchor`).
3. **Fail-Safe Fencing (STONITH):** To prevent split-brain data corruption when forcefully deleting StatefulSets (`gracePeriodSeconds: 0`), `kushd-agent` implements a three-tier power-off escalation ladder. If primary D-Bus and secondary `systemctl` calls fail, the agent triggers an immediate kernel crash via `/proc/sysrq-trigger`.
4. **Point-of-No-Return:** Once worker node evacuation begins, abort sequences are rejected. The cluster must reach full poweroff to prevent partitioned states across partially dismantled cluster topologies.
5. **Configurable Power Cut (`killpower`):** Kushd defaults to `enableKillpower: false` to allow auxiliary rack infrastructure (switches, routers, NAS) to remain on battery reserves. Cutting physical UPS load power is treated as an explicit, opt-in administrative setting.

---

## 2. Component Architecture

### 2.1 `kushd-manager` (Unified Manager Process)

Deployed as a single-replica `Deployment` scheduled via Akri resource binding. It runs two concurrent loops within a single Go process:

* **Hardware Poller Loop:** Directly accesses the device node injected by the Akri Device Plugin (e.g., `/dev/bus/usb/001/004`) using NUT or direct USB HID. Polls battery metrics, monitors power transitions (`ONBATT`, `LOWBATT`), and creates or updates the `ClusterShutdown` Custom Resource.
* **Cluster Reconciler Loop:** Watches `ClusterShutdown` resources, manages the shutdown state machine, cordons nodes, writes execution directives and timeouts to `kushd-agent` annotations, evaluates abort conditions, and executes final anchor host poweroff.

### 2.2 `kushd-agent` (Privileged DaemonSet)

Runs as a privileged DaemonSet on all nodes with Super Privileged Container (`spc_t`) SELinux options. It:

* Watches local `Node` object annotations for directives (`kushd.io/stage`) and evacuation deadlines (`kushd.io/drain-timeout`).
* Executes local pod evictions, automatically escalating to forceful deletion (`gracePeriodSeconds: 0`) at the 75% mark of the assigned timeout to prevent PDB deadlocks.
* Flushes dirty filesystem write buffers via a timeout-bounded synchronization wrapper.
* Issues `org.freedesktop.systemd1.Manager.PowerOff` over D-Bus, escalating to a hard kernel panic (STONITH) if host power-down fails.

---

## 3. Hardware Interfacing & Akri Resource Binding

### 3.1 Akri Device Plugin Discovery

Device discovery, udev event handling, and container node injection are offloaded to Akri. An Akri `Configuration` defines the UPS hardware signature using USB vendor and product IDs.

```yaml
apiVersion: akri.sh/v0
kind: Configuration
metadata:
  name: akri-udev-ups
  namespace: kube-system
spec:
  discoveryHandler:
    name: udev
    discoveryDetails: |
      udevRules:
        - 'ATTRS{idVendor}=="051d", ATTRS{idProduct}=="0002", SUBSYSTEM=="usb"'
  capacity: 1

```

### 3.2 USB Hot-Plugging & Device Node Invalidation

Under Akri, USB hot-plugging and bus resets are managed natively through the Kubernetes Device Plugin lifecycle:

1. When a USB disconnect or bus reset occurs, the kernel destroys the existing device node.
2. Akri's udev discovery handler detects device removal and unregisters the `akri.sh/ups` resource instance.
3. Kubelet detects the revocation of the device assigned to `kushd-manager` and evicts/terminates the pod.
4. When the USB bus settles, the kernel assigns a new device node (e.g., `/dev/bus/usb/001/005`). Akri advertises a fresh `akri.sh/ups` resource.
5. The Kubernetes scheduler restarts `kushd-manager` on the node, injecting the fresh, active device path directly into the container namespace.

### 3.3 Startup Topology Guard

To prevent race conditions where a UPS is plugged into a worker node and exposed via Akri, `kushd-manager` performs an immediate validation probe before starting background loops.

```go
package main

import (
	"context"
	"log/slog"
	"os"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

func validateAnchorTopologyOrExit(ctx context.Context, client kubernetes.Interface, nodeName string) {
	node, err := client.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		slog.Error("Failed to retrieve node metadata during topology validation", "error", err)
		os.Exit(1)
	}

	_, isControlPlane := node.Labels["node-role.kubernetes.io/control-plane"]
	_, isMaster := node.Labels["node-role.kubernetes.io/master"]

	if !isControlPlane && !isMaster {
		slog.Error("FATAL: Topology Invariant Violated",
			"error", "UPS_ATTACHED_TO_WORKER",
			"node", nodeName,
			"remediation", "Move physical UPS USB/Serial cable to a designated control-plane node",
		)
		// Clean exit code 1 induces CrashLoopBackOff without Go runtime stack dump
		os.Exit(1)
	}
}

```

---

## 4. API & Resource Definitions

### 4.1 CustomResourceDefinition: `ClusterShutdown`

```yaml
apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: clustershutdowns.kushd.io
spec:
  group: kushd.io
  names:
    kind: ClusterShutdown
    listKind: ClusterShutdownList
    plural: clustershutdowns
    singular: clustershutdown
    shortNames: ["csd"]
  scope: Cluster
  versions:
    - name: v1alpha1
      served: true
      storage: true
      schema:
        openAPIV3Schema:
          type: object
          required: ["spec"]
          properties:
            spec:
              type: object
              required: ["triggerSource", "batteryChargePercent", "estimatedRuntimeSeconds"]
              properties:
                triggerSource:
                  type: string
                  example: "UPS-BackUPS-1500"
                batteryChargePercent:
                  type: integer
                  minimum: 0
                  maximum: 100
                estimatedRuntimeSeconds:
                  type: integer
                abort:
                  type: boolean
                  default: false
                  description: "Manual override to cancel shutdown if set before Point-of-No-Return."
                dryRun:
                  type: boolean
                  default: false
                powerManagement:
                  type: object
                  properties:
                    enableKillpower:
                      type: boolean
                      default: false
                      description: "Instructs the UPS to cut load power after node halts."
                    killpowerDelaySeconds:
                      type: integer
                      default: 180
                      description: "Must exceed systemd DefaultTimeoutStopSec (90s) plus sync timeouts."
                    targetOutletGroup:
                      type: string
                      default: ""
                timeouts:
                  type: object
                  properties:
                    workerEvacuationSeconds:
                      type: integer
                      default: 120
                    controlPlaneEvacuationSeconds:
                      type: integer
                      default: 60
            status:
              type: object
              properties:
                phase:
                  type: string
                  enum:
                    - Pending
                    - CordoningCluster
                    - DrainingWorkers
                    - HaltingWorkers
                    - DrainingControlPlane
                    - HaltingControlPlane
                    - Finalizing
                    - Completed
                    - Aborted
                    - Failed
                observedGeneration:
                  type: integer
                startTime:
                  type: string
                  format: date-time
                completionTime:
                  type: string
                  format: date-time
                anchorNode:
                  type: string
                conditions:
                  type: array
                  items:
                    type: object
                    properties:
                      type:
                        type: string
                      status:
                        type: string
                      lastTransitionTime:
                        type: string
                        format: date-time
                      reason:
                        type: string
                      message:
                        type: string

```

### 4.2 Expanded Node Annotation Interface

| Annotation Key | Values | Direction | Operational Meaning |
| --- | --- | --- | --- |
| `kushd.io/stage` | `idle`, `drain`, `halt` | Manager $\to$ Agent | Target lifecycle state command. |
| `kushd.io/drain-timeout` | Duration string (e.g., `120s`) | Manager $\to$ Agent | Evacuation window used by agent to schedule polite vs. forceful eviction. |
| `kushd.io/agent-status` | `ready`, `draining`, `drained`, `halting`, `failed` | Agent $\to$ Manager | Reported execution state of the node host. |

---

## 5. Orchestration State Machine

```
                      [Trigger Event: LOWBATT]
                                  │
                                  ▼
                           ┌─────────────┐
                   ┌───────┤   Pending   │◄──────┐
                   │       └──────┬──────┘       │
                   │              │              │
Mains Restored(OL) │              ▼              │ Mains Restored (OL)
[spec.abort: true] │     ┌─────────────────┐     │ [spec.abort: true]
                   │     │ CordoningCluster│─────┘
                   │     └────────┬────────┘
                   │              │
                   │              ▼ ─── [POINT OF NO RETURN] ───
                   │     ┌─────────────────┐
                   │     │ DrainingWorkers │ (Evictions dispatched;
                   │     └────────┬────────┘  Aborts rejected)
                   │              │
                   │              ▼
                   │     ┌─────────────────┐
                   │     │ HaltingWorkers  │
                   │     └────────┬────────┘
                   │              │
                   │              ▼
                   │   ┌──────────────────────┐
                   │   │ DrainingControlPlane │ (N-1 Followers)
                   │   └──────────┬───────────┘
                   │              │
                   │              ▼
                   │   ┌──────────────────────┐
                   │   │ HaltingControlPlane  │ (N-1 Followers)
                   │   └──────────┬───────────┘
                   │              │
                   │              ▼
                   │       ┌─────────────┐
                   │       │ Finalizing  │ (Anchor Disk Sync &
                   │       └──────┬──────┘  Optional Killpower)
                   │              │
                   │              ▼
                   ▼       ┌─────────────┐
           ┌───────────┐   │  Completed  │ (Anchor Host Halts)
           │  Aborted  │   └─────────────┘
           │(Uncordon) │
           └───────────┘

```

### 5.1 Abort Rejection Handling

If an administrator sets `spec.abort: true` (or the UPS recovers to `OL`) **after** the cluster has entered `DrainingWorkers` or subsequent phases:

1. **Rejection Interlock:** The manager refuses to revert the phase.
2. **Condition Recording:** The manager updates `status.conditions` on the `ClusterShutdown` resource:
* `type: "AbortRejected"`
* `status: "True"`
* `reason: "PointOfNoReturnExceeded"`
* `message: "Manual abort rejected: cluster has passed the point-of-no-return (workers are draining/halting). Full shutdown must proceed."`


3. **Event Generation:** The manager emits a cluster-level Warning event:
```
Warning  AbortRejected  clustershutdown/ups-event  Manual abort rejected: cluster has passed the point-of-no-return. Evacuation pipeline continuing to completion.

```


4. **Execution Continuity:** The reconciler proceeds without interruption to `HaltingWorkers`.

### 5.2 Agent-Side PDB Escalation

When `kushd-agent` detects `kushd.io/stage: drain`, it reads the timeout duration from `kushd.io/drain-timeout` (falling back to a default of `90s` if unassigned). It derives two internal phases:

```go
package drain

import (
	"context"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

func ExecuteEscalatingDrain(ctx context.Context, client kubernetes.Interface, nodeName string, timeout time.Duration) error {
	politeDeadline := time.Now().Add(time.Duration(float64(timeout) * 0.75))
	hardDeadline := time.Now().Add(timeout)

	// Phase 1: Polite Eviction (75% of window)
	// Honors PodDisruptionBudgets and graceful termination periods
	for time.Now().Before(politeDeadline) {
		remainingPods := getNonDaemonSetPods(ctx, client, nodeName)
		if len(remainingPods) == 0 {
			return nil
		}
		for _, pod := range remainingPods {
			_ = client.CoreV1().Pods(pod.Namespace).EvictV1(ctx, &evictionPayload(pod))
		}
		time.Sleep(2 * time.Second)
	}

	// Phase 2: Forceful Deletion Escalation (Final 25% of window)
	// Overrides failing PDBs, hung finalizers, and unresponsive storage mounts
	forceContext, cancel := context.WithDeadline(ctx, hardDeadline)
	defer cancel()

	remainingPods := getNonDaemonSetPods(forceContext, client, nodeName)
	for _, pod := range remainingPods {
		gracePeriodZero := int64(0)
		_ = client.CoreV1().Pods(pod.Namespace).Delete(forceContext, pod.Name, metav1.DeleteOptions{
			GracePeriodSeconds: &gracePeriodZero,
		})
	}

	return nil
}

```

---

## 6. Host Power Execution & Fail-Safe Fencing (STONITH)

### 6.1 Stateful Split-Brain Mitigation

When `kushd-agent` force-deletes pods with `gracePeriodSeconds: 0`, the Kubernetes API server immediately purges the pods from `etcd`. For StatefulSets, the controller manager will attempt to recreate these replicas elsewhere as long as surviving control-plane nodes maintain quorum.

If the node agent fails to power off the host (due to D-Bus failures, corrupted IPC sockets, or systemd process hangs), the physical node stays alive while its local container runtime continues running the original pods. This creates an unrecoverable split-brain condition where multiple instances of stateful databases (e.g., PostgreSQL, Ceph OSDs) write to storage concurrently.

To prevent this, Kushd enforces an absolute **Three-Tier Poweroff Escalation Ladder**. Host termination is mathematically guaranteed:

```
                  [Halt Directive: kushd.io/stage: halt]
                                    │
                                    ▼
                         [Bounded Storage Sync]
                           (5-second timeout)
                                    │
                                    ▼
                         [Tier 1: Host D-Bus]
                org.freedesktop.systemd1.Manager.PowerOff()
                                    │
                         /─────────────────────\
                        <  D-Bus Call Succeeded? >
                         \─────────────────────/
                                    │
                         ┌──────────┴──────────┐
                      YES│                   NO│
                         ▼                     ▼
                     [OS Halts]      [Tier 2: chroot Fallback]
                                  /usr/bin/systemctl poweroff
                                               │
                                    /─────────────────────\
                                   <   Command Succeeded?  >
                                    \─────────────────────/
                                               │
                                    ┌──────────┴──────────┐
                                 YES│                   NO│
                                    ▼                     ▼
                                [OS Halts]       [Tier 3: Hard STONITH]
                                                 echo c > /proc/sysrq-trigger
                                                          │
                                                          ▼
                                                 [Kernel Instantly Panics]
                                                 [Hardware Execution Fenced]

```

### 6.2 Host Execution & STONITH Implementation

```go
package main

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

type HostPowerController struct {
	conn *dbus.Conn
}

func NewHostPowerController() (*HostPowerController, error) {
	conn, err := dbus.NewConnection("unix:path=/host/run/dbus/system_bus_socket")
	if err != nil {
		slog.Warn("Failed to bind host D-Bus socket; will rely on fallback tiers", "error", err)
		return &HostPowerController{conn: nil}, nil
	}
	return &HostPowerController{conn: conn}, nil
}

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
	case <-time.After(5 * time.Second):
		slog.Warn("Storage sync timed out after 5s; advancing to systemd poweroff to prevent power depletion")
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
	cmd := exec.CommandContext(ctx, "chroot", "/host", "/usr/bin/systemctl", "poweroff", "--force", "--force")
	if err := cmd.Run(); err == nil {
		slog.Info("chroot systemctl poweroff successfully executed")
		return
	} else {
		slog.Error("Tier 2 Failed: chroot systemctl execution failed", "error", err)
	}

	// Tier 3: Hard Fencing / STONITH
	// If the host cannot be powered off cleanly, force a kernel crash to eliminate split-brain risk
	h.fenceNode()
}

func (h *HostPowerController) fenceNode() {
	slog.Error("CRITICAL: All clean poweroff mechanisms failed. Triggering immediate STONITH kernel crash.")
	
	// sysrq-trigger 'c' performs an immediate crash dump/kernel panic, instantly halting node execution
	err := os.WriteFile("/host/proc/sysrq-trigger", []byte("c"), 0200)
	if err != nil {
		slog.Error("Failed to write to sysrq-trigger; issuing raw reboot syscall", "error", err)
		// Last-ditch: direct kernel reboot syscall via LINUX_REBOOT_CMD_POWER_OFF
		_ = syscall.Reboot(syscall.LINUX_REBOOT_CMD_POWER_OFF)
	}
}

```

### 6.3 Sizing `killpowerDelaySeconds` Against Kernel Unmounts

During host shutdown, `systemd` unmounts active filesystems. If a network-attached filesystem (NFS, Ceph, iSCSI) hangs due to upstream power loss, `systemd` halts on unmount until reaching `DefaultTimeoutStopSec=90s`.

If `enableKillpower: true` is configured, the hardware timer on the UPS begins immediately. To prevent the UPS from dropping power while the final anchor node is waiting out storage unmounts, `killpowerDelaySeconds` must be configured to at least **180 seconds**:

$$\text{killpowerDelaySeconds} \ge \text{DefaultTimeoutStopSec (90s)} + \text{Bounded Sync (5s)} + \text{Firmware/ACPI Flush Margin (85s)}$$

---

## 7. Packaging: Minimal Nix Container Images

Images are built using **Nix flakes** via `dockerTools.buildLayeredImage`.

* `kushd-manager` bundles static Go binaries, IANA TLS certificates, and NUT runtime libraries.
* `kushd-agent` contains strictly the statically compiled Go agent and TLS certificates.

### `flake.nix`

```nix
{
  description = "Kushd Container Image Suite";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-24.05";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };

        buildKushdBin = name: pkgs.buildGoModule {
          pname = name;
          version = "1.4.0";
          src = ./.;
          subPackages = [ "cmd/${name}" ];
          vendorHash = null;
          CGO_ENABLED = 0;
          ldflags = [ "-s" "-w" "-extldflags '-static'" ];
        };

        kushdManager = buildKushdBin "kushd-manager";
        kushdAgent = buildKushdBin "kushd-agent";

        buildKushdImage = { name, package, extraContents ? [] }:
          pkgs.dockerTools.buildLayeredImage {
            inherit name;
            tag = "latest";
            contents = [
              pkgs.cacert
              pkgs.tzdata
            ] ++ extraContents;
            config = {
              Entrypoint = [ "${package}/bin/${name}" ];
              Env = [ "SSL_CERT_FILE=${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt" ];
            };
          };

      in {
        packages = {
          manager-image = buildKushdImage {
            name = "kushd-manager";
            package = kushdManager;
            extraContents = [ pkgs.nut ];
          };
          agent-image = buildKushdImage {
            name = "kushd-agent";
            package = kushdAgent;
          };
        };
      }
    );
}

```

---

## 8. Deployment: Helm Chart Specification

### 8.1 Chart Structure

```
charts/kushd/
├── Chart.yaml
├── values.yaml
├── crds/
│   └── clustershutdowns.kushd.io.yaml
└── templates/
    ├── _helpers.tpl
    ├── rbac-manager.yaml
    ├── rbac-agent.yaml
    ├── deployment-manager.yaml
    └── daemonset-agent.yaml

```

### 8.2 Values Manifest (`charts/kushd/values.yaml`)

```yaml
global:
  imageRegistry: ghcr.io/kushd
  imagePullPolicy: IfNotPresent

manager:
  replicaCount: 1
  image:
    repository: kushd-manager
    tag: v1.4.0
  # Akri extended resource advertised by akri-udev-ups Configuration
  akriResource: "akri.sh/ups"
  powerManagement:
    enableKillpower: false
    # Must exceed 90s systemd stop-job timeout + buffer sync margin
    killpowerDelaySeconds: 180
    targetOutletGroup: ""
  thresholds:
    batteryLowPercent: 20
    runtimeLowSeconds: 300
  resources:
    limits:
      cpu: 100m
      memory: 128Mi
      akri.sh/ups: 1
    requests:
      cpu: 20m
      memory: 64Mi
      akri.sh/ups: 1
  nodeSelector:
    node-role.kubernetes.io/control-plane: ""
  tolerations:
    - key: "node-role.kubernetes.io/control-plane"
      operator: "Exists"
      effect: "NoSchedule"
    - key: "node.kubernetes.io/unschedulable"
      operator: "Exists"
      effect: "NoSchedule"

agent:
  image:
    repository: kushd-agent
    tag: v1.4.0
  hostPID: true
  securityContext:
    privileged: true
    seLinuxOptions:
      type: "spc_t"
  hostRootPath: /
  dbusSocketPath: /run/dbus/system_bus_socket
  sysrqTriggerPath: /proc/sysrq-trigger
  tolerations:
    - operator: "Exists"
  resources:
    limits:
      cpu: 50m
      memory: 64Mi
    requests:
      cpu: 10m
      memory: 32Mi

```

### 8.3 Manager Deployment (`charts/kushd/templates/deployment-manager.yaml`)

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{ include "kushd.fullname" . }}-manager
  labels:
    app.kubernetes.io/component: manager
spec:
  replicas: 1
  strategy:
    type: Recreate
  selector:
    matchLabels:
      app.kubernetes.io/component: manager
  template:
    metadata:
      labels:
        app.kubernetes.io/component: manager
    spec:
      serviceAccountName: {{ include "kushd.fullname" . }}-manager
      nodeSelector:
        {{- toYaml .Values.manager.nodeSelector | nindent 8 }}
      tolerations:
        {{- toYaml .Values.manager.tolerations | nindent 8 }}
      containers:
        - name: manager
          image: "{{ .Values.global.imageRegistry }}/{{ .Values.manager.image.repository }}:{{ .Values.manager.image.tag }}"
          imagePullPolicy: {{ .Values.global.imagePullPolicy }}
          securityContext:
            privileged: true
          env:
            - name: NODE_NAME
              valueFrom:
                fieldRef:
                  fieldPath: spec.nodeName
            - name: ENABLE_KILLPOWER
              value: {{ .Values.manager.powerManagement.enableKillpower | quote }}
            - name: KILLPOWER_DELAY_SECONDS
              value: {{ .Values.manager.powerManagement.killpowerDelaySeconds | quote }}
          resources:
            limits:
              cpu: {{ .Values.manager.resources.limits.cpu }}
              memory: {{ .Values.manager.resources.limits.memory }}
              {{ .Values.manager.akriResource }}: 1
            requests:
              cpu: {{ .Values.manager.resources.requests.cpu }}
              memory: {{ .Values.manager.resources.requests.memory }}
              {{ .Values.manager.akriResource }}: 1

```

### 8.4 Agent DaemonSet (`charts/kushd/templates/daemonset-agent.yaml`)

```yaml
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: {{ include "kushd.fullname" . }}-agent
  labels:
    app.kubernetes.io/component: agent
spec:
  selector:
    matchLabels:
      app.kubernetes.io/component: agent
  template:
    metadata:
      labels:
        app.kubernetes.io/component: agent
    spec:
      hostPID: {{ .Values.agent.hostPID }}
      priorityClassName: system-node-critical
      tolerations:
        {{- toYaml .Values.agent.tolerations | nindent 8 }}
      containers:
        - name: agent
          image: "{{ .Values.global.imageRegistry }}/{{ .Values.agent.image.repository }}:{{ .Values.agent.image.tag }}"
          imagePullPolicy: {{ .Values.global.imagePullPolicy }}
          securityContext:
            privileged: {{ .Values.agent.securityContext.privileged }}
            seLinuxOptions:
              type: {{ .Values.agent.securityContext.seLinuxOptions.type | quote }}
          env:
            - name: NODE_NAME
              valueFrom:
                fieldRef:
                  fieldPath: spec.nodeName
          volumeMounts:
            - name: dbus-socket
              mountPath: /host/run/dbus/system_bus_socket
            - name: host-root
              mountPath: /host
            - name: sysrq-trigger
              mountPath: /host/proc/sysrq-trigger
          resources:
            {{- toYaml .Values.agent.resources | nindent 12 }}
      volumes:
        - name: dbus-socket
          hostPath:
            path: {{ .Values.agent.dbusSocketPath }}
            type: Socket
        - name: host-root
          hostPath:
            path: {{ .Values.agent.hostRootPath }}
            type: Directory
        - name: sysrq-trigger
          hostPath:
            path: {{ .Values.agent.sysrqTriggerPath }}
            type: File

```

---

## 9. CI/CD: Automated Publishing Workflows

### 9.1 Container Release Pipeline (`.github/workflows/publish-containers.yaml`)

```yaml
name: Publish OCI Images

on:
  push:
    tags:
      - 'v*'

jobs:
  build-and-push:
    runs-on: ubuntu-latest
    permissions:
      contents: read
      packages: write

    strategy:
      matrix:
        include:
          - package-name: manager-image
            image-name: kushd-manager
          - package-name: agent-image
            image-name: kushd-agent

    steps:
      - name: Checkout Source
        uses: actions/checkout@v4

      - name: Install Nix
        uses: cachix/install-nix-action@v27
        with:
          extra_nix_config: |
            experimental-features = nix-command flakes

      - name: Authenticate with GHCR
        uses: docker/login-action@v3
        with:
          registry: ghcr.io
          username: ${{ github.actor }}
          password: ${{ secrets.GITHUB_TOKEN }}

      - name: Build and Import Image via Nix
        run: |
          nix build .#${{ matrix.package-name }}
          ./result | docker load

      - name: Publish Image Tags
        run: |
          IMAGE_URI="ghcr.io/${{ github.repository_owner }}/${{ matrix.image-name }}"
          TAG="${GITHUB_REF#refs/tags/}"

          docker tag ${{ matrix.image-name }}:latest ${IMAGE_URI}:${TAG}
          docker tag ${{ matrix.image-name }}:latest ${IMAGE_URI}:latest

          docker push ${IMAGE_URI}:${TAG}
          docker push ${IMAGE_URI}:latest

```

### 9.2 Helm OCI Publishing Pipeline (`.github/workflows/publish-helm.yaml`)

```yaml
name: Publish Helm Chart (OCI)

on:
  push:
    tags:
      - 'v*'

jobs:
  publish-helm:
    runs-on: ubuntu-latest
    permissions:
      contents: read
      packages: write

    steps:
      - name: Checkout Source
        uses: actions/checkout@v4

      - name: Install Helm
        uses: azure/setup-helm@v4
        with:
          version: v3.14.0

      - name: Validate Linting
        run: helm lint charts/kushd

      - name: Package Chart
        run: |
          VERSION=${GITHUB_REF#refs/tags/v}
          mkdir -p .cr-release
          helm package charts/kushd --version ${VERSION} --app-version ${VERSION} -d .cr-release

      - name: Authenticate Helm to GHCR
        run: |
          echo "${{ secrets.GITHUB_TOKEN }}" | helm registry login ghcr.io -u ${{ github.actor }} --password-stdin

      - name: Push Chart OCI Artifact
        run: |
          VERSION=${GITHUB_REF#refs/tags/v}
          helm push .cr-release/kushd-${VERSION}.tgz oci://ghcr.io/${{ github.repository_owner }}/charts

```

---

## 10. Reviewer Sign-Off Checklist

* [ ] **Akri Discovery Verification:** Verify that the Akri discovery handler correctly exports the extended resource name defined in `values.yaml` (`akri.sh/ups: 1`).
* [ ] **Implicit Anchor Pinning:** Confirm that combining `node-role.kubernetes.io/control-plane: ""` with `resources.limits.akri.sh/ups: 1` successfully places `kushd-manager` without static host labeling.
* [ ] **STONITH sysrq Access:** Confirm that host `/proc/sysrq-trigger` is mounted read-write into `kushd-agent` and accessible under the `spc_t` SELinux context.
* [ ] **Timeout Annotation Propagation:** Verify that `kushd-manager` populates `kushd.io/drain-timeout` on nodes during the `DrainingWorkers` phase and that `kushd-agent` accurately calculates the 75% escalation window.
* [ ] **Killpower Delay Sizing:** Confirm that `killpowerDelaySeconds` remains defaulted to $\ge 180$ seconds to ensure systemd stop jobs on unmounted network storage do not overlap with the physical UPS load cut.