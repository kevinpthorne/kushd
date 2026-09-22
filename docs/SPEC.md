# Kushd (Kubernetes Shutdown Daemon) Engineering Specification

**Document Version:** 1.0.0-RC2

**Status:** Ready for Review

**Target Environments:** Bare-Metal & Edge Kubernetes (v1.28+)

**Primary Subsystems:** Consolidated Controller (`kushd-controller`), Host Node Daemon (`kushd-agent`)

---

## 1. Executive Summary & Core Invariants

Kushd is a topology-aware, event-driven shutdown orchestrator for Kubernetes clusters operating behind Uninterruptible Power Supply (UPS) hardware. Abrupt power loss risks stateful corruption: `etcd` consensus split-brain, write-cache truncation on persistent volumes (Ceph, Longhorn, local block devices), and ungraceful pod termination.

Kushd enforces an ordered, priority-aware decommission sequence down to the host OS level (`systemd`), while explicitly protecting shared rack infrastructure (networking, storage appliances) through an opt-in power-cut model.

```
                      [UPS Hardware]
                      (USB / Serial)
                            │ (Mounted via /dev/ups0)
                            ▼
              ┌───────────────────────────┐
              │ Control Plane Anchor Node │
              │                           │
              │     kushd-controller      │
              │  (Poller + Reconciler)    │
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
2. **Anchor-Node Architecture (No Leader Election):** `kushd-controller` runs as a single, pinned replica on the control-plane node hosting the UPS cable. Dynamic leader election is banned: standard Kubernetes Lease mechanisms deadlock as `etcd` members power down during cluster evacuation.
3. **Unified Manager Process:** Hardware telemetry polling, cluster state reconciliation, and final hardware power cuts execute within a single container (`kushd-controller`), eliminating inter-process communication over failing networks or degrading `etcd` clusters.
4. **Point-of-No-Return:** Once worker node evacuation begins, abort sequences are rejected. The cluster must reach full poweroff to prevent partitioned states across partially dismantled cluster topologies.
5. **Configurable Power Cut (`killpower`):** Kushd defaults to `enableKillpower: false` to allow auxiliary rack infrastructure (switches, routers, NAS) to continue running on battery reserves. Cutting physical UPS load power is treated as an explicit, opt-in administrative setting.

---

## 2. Component Architecture

### 2.1 `kushd-controller` (Single-Replica Manager)

Deployed as a single-pod `Deployment` pinned to the physical UPS host via `nodeSelector`. It runs two concurrent loops within a single Go binary:

* **Hardware Poller Routine:** Continually polls battery metrics over a persistent device path (`/dev/ups0`) via NUT or direct USB HID. Detects power transitions (`ONBATT`, `LOWBATT`) and creates or reconciles the `ClusterShutdown` Custom Resource.
* **Cluster Reconciler Routine:** Watches `ClusterShutdown` resources, manages the phase state machine, cordons nodes, dispatches stage annotations to `kushd-agent` instances, monitors node health, and executes final anchor host poweroff.

### 2.2 `kushd-agent` (Privileged DaemonSet)

Runs as a privileged DaemonSet on all nodes. Mounts the host D-Bus system socket and host root filesystem. It:

* Watches its local `Node` object annotations (`kushd.io/stage`).
* Executes local pod evictions, automatically escalating to forceful deletion if PodDisruptionBudgets (PDBs) block shutdown progress.
* Flushes dirty filesystem buffers via a timeout-bounded synchronization wrapper.
* Issues `org.freedesktop.systemd1.Manager.PowerOff` to the host systemd daemon.

---

## 3. Hardware & Startup Validation

### 3.1 Persistent Device Symlinking

To survive Linux USB re-enumeration (e.g., `/dev/ttyUSB0` changing to `/dev/ttyUSB1` after transient brownouts), the host must expose a predictable udev path.

**Host udev Rule (`/etc/udev/rules.d/99-ups.rules`):**

```udev
SUBSYSTEM=="usb", ATTRS{idVendor}=="051d", ATTRS{idProduct}=="0002", SYMLINK+="ups0", MODE="0660", GROUP="dialout"

```

### 3.2 Startup Topology Guard

On initialization, `kushd-controller` validates that its hosting node carries the control-plane role. Violations log structured JSON errors and terminate cleanly with exit code 1 to induce a clean `CrashLoopBackOff` without Go runtime stack dump noise.

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
                      default: 60
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

### 4.2 Node Annotation Interface

| Annotation Key | Values | Direction | Operational Meaning |
| --- | --- | --- | --- |
| `kushd.io/stage` | `idle`, `drain`, `halt` | Controller $\to$ Agent | Target lifecycle state command. |
| `kushd.io/agent-status` | `ready`, `draining`, `drained`, `halting`, `failed` | Agent $\to$ Controller | Reported execution state of the node host. |

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

### 5.1 Phase Transitions

| Phase | Entry Condition | Action Taken | Next Phase |
| --- | --- | --- | --- |
| **Pending** | CR Created / Battery Critical | Validate anchor node locks and read configuration. | `CordoningCluster` |
| **CordoningCluster** | Phase == Pending | Set `spec.unschedulable: true` on **all** cluster nodes. | `DrainingWorkers` |
| **DrainingWorkers** | Phase == CordoningCluster | Set `kushd.io/stage: drain` on all worker nodes. Point-of-No-Return crossed. | `HaltingWorkers` |
| **HaltingWorkers** | Workers report `drained` OR `workerEvacuationSeconds` expires | Set `kushd.io/stage: halt` on worker nodes. Controller waits for workers to report `NotReady` or disconnect. | `DrainingControlPlane` |
| **DrainingControlPlane** | All worker nodes offline | Set `kushd.io/stage: drain` on non-anchor control-plane nodes sequentially. | `HaltingControlPlane` |
| **HaltingControlPlane** | Followers drained | Set `kushd.io/stage: halt` on non-anchor control-plane nodes. | `Finalizing` |
| **Finalizing** | Followers offline | Anchor agent syncs storage. If `enableKillpower: true`, controller schedules hardware cut. | `Completed` |
| **Completed** | Finalizing complete | Anchor host calls D-Bus `PowerOff()`. Terminal state. | — |
| **Aborted** | Power restored or `spec.abort: true` before Point-of-No-Return | Remove cordon (`spec.unschedulable: false`) across all nodes. Remove stage annotations. | Terminal |

### 5.2 Two-Stage Evacuation & PDB Escalation

Standard eviction calls can block indefinitely on strict PodDisruptionBudgets (PDBs) or stateful storage daemons (e.g., Ceph OSDs, Longhorn replicas). `kushd-agent` implements a dual-stage eviction loop:

1. **Polite Eviction (0% to 75% of `workerEvacuationSeconds`):**
Issues Kubernetes Eviction requests (`/api/v1/namespaces/{ns}/pods/{name}/eviction`). Respects PDB constraints and honors individual workload `terminationGracePeriodSeconds`.
2. **Forceful Evacuation (75% to 100% of `workerEvacuationSeconds`):**
If pods remain on the node, the agent escalates to direct pod deletion (`DELETE /api/v1/namespaces/{ns}/pods/{name}`) with `gracePeriodSeconds: 0`. This overrides failing storage pods and broken disruption budgets to ensure the host can decommission before battery failure.

---

## 6. Host Integration & Agent Mechanics

### 6.1 Non-Blocking Host Buffer Synchronization

Synchronous `syscall.Sync()` invocations can deadlock indefinitely if network-attached filesystems (NFS, Ceph, iSCSI) become unresponsive due to network switches dropping power. `kushd-agent` isolates filesystem synchronization inside a bounded goroutine with a 5-second deadline.

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

	// Execute storage sync with a strict 5-second deadline to prevent network hangs
	syncDone := make(chan struct{})
	go func() {
		syscall.Sync()
		close(syncDone)
	}()

	select {
	case <-syncDone:
		slog.Info("Storage buffers successfully synced to disk")
	case <-time.After(5 * time.Second):
		slog.Warn("Storage sync timed out after 5s; proceeding with halt to prevent power depletion")
	}

	slog.Info("Invoking systemd PowerOff via D-Bus...")
	systemd := h.conn.Object("org.freedesktop.systemd1", "/org/freedesktop/systemd1")
	
	// 'replace' job mode cancels pending unit transactions and triggers instant poweroff
	call := systemd.CallWithContext(ctx, "org.freedesktop.systemd1.Manager.PowerOff", 0)
	if call.Err != nil {
		return fmt.Errorf("systemd D-Bus invocation failed: %w", call.Err)
	}

	return nil
}

```

### 6.2 Secondary Execution Fallback

If the D-Bus socket is unresponsive, the agent falls back to direct chroot execution:

```bash
chroot /host /usr/bin/systemctl poweroff --force --force

```

---

## 7. Packaging: Minimal Nix Container Images

Container images are compiled with **Nix flakes** using `dockerTools.buildLayeredImage`.

* `kushd-controller` includes static binaries, IANA TLS certificates, and NUT libraries for hardware communication.
* `kushd-agent` is strictly lean; redundant `systemd` packages are omitted since fallback routines execute via `chroot` against the host's `/usr/bin/systemctl`.

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
          version = "1.2.0";
          src = ./.;
          subPackages = [ "cmd/${name}" ];
          vendorHash = null;
          CGO_ENABLED = 0;
          ldflags = [ "-s" "-w" "-extldflags '-static'" ];
        };

        kushdController = buildKushdBin "kushd-controller";
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
          controller-image = buildKushdImage {
            name = "kushd-controller";
            package = kushdController;
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

### 8.1 Values Manifest (`charts/kushd/values.yaml`)

```yaml
global:
  imageRegistry: ghcr.io/kushd
  imagePullPolicy: IfNotPresent

controller:
  # Strictly single replica pinned to the physical UPS anchor host
  replicaCount: 1
  image:
    repository: kushd-controller
    tag: v1.2.0
  upsDevice:
    port: "/dev/ups0"
    driver: "usbhid-ups"
  powerManagement:
    enableKillpower: false
    killpowerDelaySeconds: 60
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
    tag: v1.2.0
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

### 8.2 Controller Deployment (`charts/kushd/templates/deployment-controller.yaml`)

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{ include "kushd.fullname" . }}-controller
  labels:
    app.kubernetes.io/component: controller
spec:
  replicas: 1
  strategy:
    type: Recreate
  selector:
    matchLabels:
      app.kubernetes.io/component: controller
  template:
    metadata:
      labels:
        app.kubernetes.io/component: controller
    spec:
      serviceAccountName: {{ include "kushd.fullname" . }}-controller
      nodeSelector:
        {{- toYaml .Values.controller.nodeSelector | nindent 8 }}
      tolerations:
        {{- toYaml .Values.controller.tolerations | nindent 8 }}
      containers:
        - name: controller
          image: "{{ .Values.global.imageRegistry }}/{{ .Values.controller.image.repository }}:{{ .Values.controller.image.tag }}"
          imagePullPolicy: {{ .Values.global.imagePullPolicy }}
          securityContext:
            privileged: true
          env:
            - name: NODE_NAME
              valueFrom:
                fieldRef:
                  fieldPath: spec.nodeName
            - name: UPS_DEVICE_PORT
              value: {{ .Values.controller.upsDevice.port | quote }}
            - name: ENABLE_KILLPOWER
              value: {{ .Values.controller.powerManagement.enableKillpower | quote }}
          volumeMounts:
            - name: ups-dev
              mountPath: {{ .Values.controller.upsDevice.port }}
          resources:
            {{- toYaml .Values.controller.resources | nindent 12 }}
      volumes:
        - name: ups-dev
          hostPath:
            path: {{ .Values.controller.upsDevice.port }}
            type: CharDevice

```

### 8.3 Agent DaemonSet (`charts/kushd/templates/daemonset-agent.yaml`)

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
            {{- toYaml .Values.agent.securityContext | nindent 12 }}
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

```

---

## 9. CI/CD: Automated Publishing Workflows

### 9.1 Container Release (`.github/workflows/publish-containers.yaml`)

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
          - package-name: controller-image
            image-name: kushd-controller
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

### 9.2 Helm OCI Publishing (`.github/workflows/publish-helm.yaml`)

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

* [ ] **Anchor Identification:** Confirm that bare-metal provisioning automations label exactly one control plane node with `kushd.io/ups-anchor: "true"`.
* [ ] **Storage Sync Baseline:** Verify that a 5-second `syscall.Sync()` boundary is sufficient for local NVMe/SATA write caches while avoiding network block hangs.
* [ ] **PDB Forceful Deletion Window:** Confirm that allowing 75% of the total drain duration for polite eviction provides adequate windowing for internal databases (PostgreSQL/etcd) to checkpoint.
* [ ] **Shared Outlet Isolation:** Confirm `enableKillpower` remains defaulted to `false` unless a dedicated, single-purpose rack is explicitly declared.