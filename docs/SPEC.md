# Kushd (Kubernetes Shutdown Daemon) Engineering Specification

**Document Version:** 1.0.0-RC3

**Status:** Ready for Review

**Target Environments:** Bare-Metal & Edge Kubernetes (v1.28+)

**Primary Subsystems:** Consolidated Daemon Manager (`kushd-manager`), Host Node Daemon (`kushd-agent`)

---

## 1. Executive Summary & Core Invariants

Kushd is a topology-aware, event-driven shutdown orchestrator for Kubernetes clusters operating behind Uninterruptible Power Supply (UPS) hardware. Abrupt power cuts induce stateful data loss: `etcd` quorum fragmentation, write-cache truncation on distributed block storage (Ceph, Longhorn, local persistent disks), and ungraceful pod termination.

Kushd enforces an ordered, priority-aware decommission sequence down to the host OS layer (`systemd`), while protecting shared rack infrastructure (switches, routers, NAS appliances) through an opt-in power-cut model.

Following Kubernetes operator patterns, Kushd consolidates cluster-wide reconciliation and raw hardware device I/O into a single higher-level process: **`kushd-manager`**.

```
                         [UPS Hardware]
                         (USB / Serial)
                               │ (Mounted via /dev/kushd/ups0 directory)
                               ▼
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
                      [Host Poweroff  ]
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
                      [Host Poweroff  ]
                               │
                  Phase 3: Final Anchor Decommission
                               │
                               ▼
                    [Optional UPS Killpower]
                    [Anchor Host Poweroff  ]

```

### System Invariants

1. **Topology Placement Invariant:** Physical UPS communication hardware (USB/Serial) **must** terminate on a control-plane node. Kushd enforces an in-process, clean exit (`os.Exit(1)`) without stack trace pollution if scheduled on a worker node.
2. **Anchor-Node Architecture (No Leader Election):** `kushd-manager` runs as a single, pinned replica on the control-plane node hosting the physical UPS cable. Dynamic leader election is banned: standard Kubernetes Lease mechanisms deadlock as `etcd` members power down during cluster evacuation.
3. **Consolidated Manager Process:** Hardware telemetry polling, cluster state reconciliation, and final hardware power cuts execute within a single container (`kushd-manager`), eliminating inter-process communication over failing networks or degrading `etcd` clusters.
4. **Point-of-No-Return:** Once worker node evacuation begins, abort sequences are rejected. The cluster must reach full poweroff to prevent split-brain workloads across partially dismantled cluster topologies.
5. **Configurable Power Cut (`killpower`):** Kushd defaults to `enableKillpower: false` to allow auxiliary rack infrastructure (switches, routers, storage) to continue running on battery reserves. Cutting physical UPS load power is treated as an explicit, opt-in administrative setting.

---

## 2. Component Architecture

### 2.1 `kushd-manager` (Unified Manager Process)

Deployed as a single-pod `Deployment` pinned to the physical UPS host via `nodeSelector`. It runs two concurrent loops within a single Go process:

* **Hardware Poller Loop:** Polls battery metrics over a persistent device directory (`/dev/kushd/ups0`) via NUT or direct USB HID. Detects power transitions (`ONBATT`, `LOWBATT`), tracks runtime trends, dynamically re-evaluates device symlinks to survive USB bus resets, and creates/updates the `ClusterShutdown` Custom Resource.
* **Cluster Reconciler Loop:** Watches `ClusterShutdown` resources, drives the shutdown state machine, cordons nodes, writes execution parameters to `kushd-agent` annotations, tracks node evictions, handles abort validation, and executes final anchor host poweroff.

### 2.2 `kushd-agent` (Privileged DaemonSet)

Runs as a privileged DaemonSet on all nodes. Mounts the host D-Bus system socket and host root filesystem. It:

* Watches local `Node` object annotations for execution directives (`kushd.io/stage`) and parameters (`kushd.io/drain-timeout`).
* Executes local pod evictions, automatically escalating to forceful deletion (`gracePeriodSeconds: 0`) at the 75% mark of the assigned timeout if PodDisruptionBudgets (PDBs) block shutdown progress.
* Flushes dirty filesystem buffers via a timeout-bounded synchronization wrapper.
* Issues `org.freedesktop.systemd1.Manager.PowerOff` to the host systemd daemon over D-Bus.

---

## 3. Hardware Interfacing & Hot-Plug Inode Invalidation

### 3.1 USB Hot-Plugging & Stale Inode Prevention

When a USB device disconnects or experiences a transient bus reset, the Linux kernel assigns the reconnected device a new character device node (e.g., `/dev/ttyUSB1` replacing `/dev/ttyUSB0`) with a **new inode number**.

If Kubernetes mounts a specific device path directly into a container (`type: CharDevice`), the container runtime pins the mount to the original device inode. When the device resets, the mount retains the dead inode, severing container communication indefinitely.

To guarantee zero-restart recovery during USB re-enumeration:

1. The host udev rule creates a symlink inside an isolated directory (`/dev/kushd/ups0`).
2. Helm mounts the **directory** (`/dev/kushd`) with `type: Directory` instead of binding a specific character device.
3. `kushd-manager` dynamically evaluates the symlink on every polling cycle via `filepath.EvalSymlinks`.

**Host udev Rule (`/etc/udev/rules.d/99-ups.rules`):**

```udev
SUBSYSTEM=="usb", ATTRS{idVendor}=="051d", ATTRS{idProduct}=="0002", ACTION=="add", RUN+="/bin/mkdir -p /dev/kushd", SYMLINK+="kushd/ups0", MODE="0660", GROUP="dialout"

```

### 3.2 Dynamic Symlink Resolution Logic

```go
package hardware

import (
	"fmt"
	"os"
	"path/filepath"
)

// ResolveDevicePath guarantees access to active device inodes across USB bus blips
func ResolveDevicePath(symlinkPath string) (string, error) {
	realPath, err := filepath.EvalSymlinks(symlinkPath)
	if err != nil {
		return "", fmt.Errorf("failed to resolve UPS device symlink %s: %w", symlinkPath, err)
	}

	info, err := os.Stat(realPath)
	if err != nil {
		return "", fmt.Errorf("failed to stat resolved device %s: %w", realPath, err)
	}

	// Verify target is a character device
	if info.Mode()&os.ModeCharDevice == 0 {
		return "", fmt.Errorf("target device %s is not a character device", realPath)
	}

	return realPath, nil
}

```

### 3.3 Startup Topology Guard

On initialization, `kushd-manager` verifies that its hosting node carries the control-plane role. Violations log structured JSON errors and terminate cleanly with exit code 1 to induce a clean `CrashLoopBackOff` without Go runtime stack dump noise.

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

The interface decouples policy from node runtime mechanics, passing both the stage directive and the computed execution parameters directly to the node agent:

| Annotation Key | Values | Direction | Operational Meaning |
| --- | --- | --- | --- |
| `kushd.io/stage` | `idle`, `drain`, `halt` | Manager $\to$ Agent | Target lifecycle state command. |
| `kushd.io/drain-timeout` | Duration string (e.g., `120s`) | Manager $\to$ Agent | Evacuation window used by agent to schedule polite vs. forceful eviction. |
| `kushd.io/agent-status` | `ready`, `draining`, `drained`, `halting`, `failed` | Agent $\to$ Manager | Reported execution state of the host. |

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

If an administrator applies `spec.abort: true` (or the UPS transitions back to `OL`) **after** the cluster has entered `DrainingWorkers` or subsequent phases:

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

When `kushd-agent` detects `kushd.io/stage: drain`, it reads the timeout duration from `kushd.io/drain-timeout` (falling back to a safe default of `90s` if unassigned). It derives two internal phases:

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

## 6. Kernel Deadlocks & Host Power Execution

### 6.1 Bounded Storage Sync vs. Kernel Unmount Deadlocks

While `kushd-agent` bounds its `syscall.Sync()` call to a 5-second context timeout, calling `PowerOff()` transfers execution to `systemd`.

During OS shutdown, `systemd` iterates over all active mounts and issues `umount`. If a network-attached filesystem (NFS, Ceph, iSCSI) is hung due to power loss on an upstream switch, the Linux kernel holds locks on the virtual filesystem (VFS) superblock. `systemd` halts on this unmount job until it hits its internal stop-job timeout (`DefaultTimeoutStopSec=90s`).

```
kushd-agent                     systemd                             Linux Kernel
    │                              │                                      │
    ├── 5s Sync Timeout ──────────►│                                      │
    │   (Advances without hang)    │                                      │
    │                              │                                      │
    └── D-Bus: PowerOff() ────────►│                                      │
                                   ├── SIGTERM / SIGKILL to Units ───────►│
                                   │                                      │
                                   └── Umount Network Filesystems ───────►│
                                       (Kernel blocks on dead storage)    │
                                       │                                  │
                                       ├── [HANGS FOR 90 SECONDS] ────────┤
                                       │   (DefaultTimeoutStopSec)        │
                                       │                                  │
                                       └── Forceful Lazy Unmount / Kill ─►│
                                           │                              │
                                           └── ACPI PowerOff ────────────►│ (HALT)

```

### 6.2 Sizing `killpowerDelaySeconds`

If `enableKillpower: true` is set, the UPS hardware timer begins counting down the moment `kushd-manager` issues the shutdown instruction.

If `killpowerDelaySeconds` is smaller than the systemd stop-job timeout, the UPS will cut power **while the final anchor node is still hanging on storage unmounts**, resulting in filesystem truncation and dirty metadata state.

**Mandatory Baseline:**

`killpowerDelaySeconds` must be configured to at least **180 seconds**:


$$\text{killpowerDelaySeconds} \ge \text{DefaultTimeoutStopSec (90s)} + \text{Bounded Sync (5s)} + \text{Firmware/ACPI Flush Window (85s)}$$

### 6.3 Host Power Execution Loop

```go
package main

import (
	"context"
	"fmt"
	"log/slog"
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
		return nil, fmt.Errorf("failed to bind host D-Bus socket: %w", err)
	}
	return &HostPowerController{conn: conn}, nil
}

func (h *HostPowerController) HaltHost(ctx context.Context) error {
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

	slog.Info("Invoking systemd PowerOff via D-Bus...")
	systemd := h.conn.Object("org.freedesktop.systemd1", "/org/freedesktop/systemd1")
	
	call := systemd.CallWithContext(ctx, "org.freedesktop.systemd1.Manager.PowerOff", 0)
	if call.Err != nil {
		return fmt.Errorf("systemd D-Bus invocation failed: %w", call.Err)
	}

	return nil
}

```

---

## 7. Packaging: Minimal Nix Container Images

Container images are compiled with **Nix flakes** using `dockerTools.buildLayeredImage`.

* `kushd-manager` contains static Go binaries, IANA TLS certificates, and NUT libraries for hardware communication.
* `kushd-agent` contains strictly static Go binaries and runtime certificates. `systemd` packages are omitted because fallback execution calls `chroot /host /usr/bin/systemctl`.

### `flake.nix`

```nix
{
  description = "Kushd Container Image Flake";

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
          version = "1.3.0";
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
    tag: v1.3.0
  upsDevice:
    # Directory mount to avoid stale inode pinning across USB disconnects
    deviceDir: "/dev/kushd"
    symlinkName: "ups0"
    driver: "usbhid-ups"
  powerManagement:
    enableKillpower: false
    # Accommodates 90s systemd unmount stop-job timeout + buffer sync margin
    killpowerDelaySeconds: 180
    targetOutletGroup: ""
  thresholds:
    batteryLowPercent: 20
    runtimeLowSeconds: 300
  resources:
    limits:
      cpu: 100m
      memory: 128Mi
    requests:
      cpu: 20m
      memory: 64Mi
  nodeSelector:
    node-role.kubernetes.io/control-plane: ""
    kushd.io/ups-anchor: "true"
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
    tag: v1.3.0
  hostPID: true
  securityContext:
    privileged: true
  hostRootPath: /
  dbusSocketPath: /run/dbus/system_bus_socket
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
            - name: UPS_DEVICE_DIR
              value: {{ .Values.manager.upsDevice.deviceDir | quote }}
            - name: UPS_SYMLINK_NAME
              value: {{ .Values.manager.upsDevice.symlinkName | quote }}
            - name: ENABLE_KILLPOWER
              value: {{ .Values.manager.powerManagement.enableKillpower | quote }}
            - name: KILLPOWER_DELAY_SECONDS
              value: {{ .Values.manager.powerManagement.killpowerDelaySeconds | quote }}
          volumeMounts:
            # Mount parent directory to survive device inode changes across USB reconnects
            - name: ups-dev-dir
              mountPath: {{ .Values.manager.upsDevice.deviceDir }}
          resources:
            {{- toYaml .Values.manager.resources | nindent 12 }}
      volumes:
        - name: ups-dev-dir
          hostPath:
            path: {{ .Values.manager.upsDevice.deviceDir }}
            type: Directory

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

* [ ] **Manager Nomenclature:** Confirm that all code references, build artifacts, container packages, and Helm manifests reference `kushd-manager`.
* [ ] **Device Directory Mount:** Confirm that the host udev rule creates `/dev/kushd/ups0` and that the Helm chart mounts the directory rather than the leaf character device.
* [ ] **Timeout Annotation Propagation:** Verify that `kushd-manager` sets `kushd.io/drain-timeout` alongside `kushd.io/stage: drain`, and that `kushd-agent` calculates the 75% escalation threshold based on this value.
* [ ] **Abort Rejection Condition:** Confirm that late abort requests write an `AbortRejected` status condition and emit a Warning event rather than aborting an active worker eviction.
* [ ] **Killpower Delay Sizing:** Confirm that `killpowerDelaySeconds` defaults to $\ge 180$ seconds to survive systemd's 90-second unmount timeout on unresponsive network storage.