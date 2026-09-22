# Kushd (Kubernetes Shutdown Daemon) Engineering Specification

**Document Version:** 1.0.0-RC1

**Status:** Under Review

**Target Environments:** Bare-Metal & Edge Kubernetes (v1.28+)

**Primary Subsystems:** Hardware Telemetry (`kushd-trigger`), Cluster State Reconciler (`kushd-controller`), Host Node Daemon (`kushd-agent`)

---

## 1. Executive Summary & System Invariants

Kushd is a distributed, event-driven shutdown orchestrator for Kubernetes clusters operating behind Uninterruptible Power Supply (UPS) hardware. Abrupt power failure poses catastrophic risks to stateful workloads: `etcd` consensus corruption, write-cache truncation on persistent volumes (Ceph, Longhorn, local storage), and ungraceful pod termination.

Kushd enforces an ordered, priority-aware decommission of the cluster down to the host OS level (`systemd`), while explicitly providing configurable protection for shared rack infrastructure (e.g., switches, modems, routers, storage appliances).

### Core Invariants

1. **Topology Placement Invariant:** Physical UPS communication hardware (USB/Serial via RJ45) **must** terminate on a control-plane node. Kushd enforces an in-process, fail-fast panic if the UPS device is bound to a worker node, eliminating premature telemetry severance during worker eviction.
2. **Asymmetric Phasing:** Worker nodes must completely evacuate and halt before control-plane nodes initiate decommissioning.
3. **Quorum Preservation:** Control-plane nodes must decommission sequentially ($N-1 \to \text{Final}$). The node hosting the active `kushd-controller` leader must remain online as the final cluster host.
4. **Point-of-No-Return:** Once worker node eviction begins, power-restoration abort sequences are rejected to prevent partitioned states across partially dismantled cluster topologies.
5. **Configurable Power Cut (`killpower`):** Kushd defaults to `enableKillpower: false` to allow auxiliary rack infrastructure (networking, NAS) to remain on battery reserves. Cutting physical UPS load power is treated as an explicit, opt-in administrative setting.

---

## 2. System Architecture

```
                  ┌─────────────────────────────────────────────────────┐
                  │                 Control Plane Node                  │
                  │                                                     │
[UPS Hardware] ──►│ [kushd-trigger]                                     │
 (USB/Serial RJ45)│       │ (Validates CP role; Panics if on worker)    │
                  │       ▼                                             │
                  │ [ClusterShutdown CR]                                │
                  │       │                                             │
                  │       ▼                                             │
                  │ [kushd-controller] (Leader)                         │
                  └───────┬─────────────────────────────┬───────────────┘
                          │                             │
             Phase 1: Worker Nodes         Phase 2: Control Plane
                          │                             │
                          ▼                             ▼
                 ┌─────────────────┐           ┌─────────────────┐
                 │   kushd-agent   │           │   kushd-agent   │
                 │  (Worker Node)  │           │  (CP Follower)  │
                 └────────┬────────┘           └────────┬────────┘
                          ▼                             ▼
                 [systemd / D-Bus]             [systemd / D-Bus]
                 [Host Poweroff  ]             [Host Poweroff  ]

```

### Component Roles

* **`kushd-trigger`**: Hardware monitoring daemon. Interfaces with the UPS via Network UPS Tools (NUT) or raw USB HID. Detects power events (`ONBATT`, `LOWBATT`), tracks voltage/runtime trends, and creates the `ClusterShutdown` custom resource.
* **`kushd-controller`**: Clustered control-plane reconciler (leader elected). Watches `ClusterShutdown` CRs, drives the global shutdown state machine, calculates drain timeouts, cordons nodes, and sets lifecycle annotations.
* **`kushd-agent`**: Privileged host-integrated DaemonSet. Watches local `Node` metadata annotations, drains local pods, coordinates with `systemd-logind` via D-Bus, syncs dirty disk buffers, and executes host halts.

---

## 3. The Topology Guard: In-Process Trigger Panic

Connecting the UPS communication link to a worker node introduces an immediate race condition: worker nodes are evacuated and halted *first*, destroying telemetry and abort controls midway through the sequence.

`kushd-trigger` blocks this configuration at startup by inspecting its local node topology via the Kubernetes API.

```go
package main

import (
	"context"
	"fmt"
	"os"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

func assertControlPlaneTopology(ctx context.Context, client kubernetes.Interface, nodeName string) {
	node, err := client.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		panic(fmt.Sprintf("FATAL: kushd-trigger cannot retrieve node metadata: %v", err))
	}

	_, isControlPlane := node.Labels["node-role.kubernetes.io/control-plane"]
	_, isMaster := node.Labels["node-role.kubernetes.io/master"]

	if !isControlPlane && !isMaster {
		fmt.Fprintf(os.Stderr, "FATAL_TOPOLOGY_VIOLATION: UPS cable detected on worker node %q. "+
			"kushd-trigger must run strictly on a control-plane node to prevent premature power severance "+
			"during worker node drain phases. Halting startup immediately.\n", nodeName)
		
		// In-process panic triggers an exit code 1, inducing CrashLoopBackOff
		panic("TOPOLOGY_MISMATCH_WORKER_ATTACHED_UPS")
	}
}

```

### Failure Modes & Observability

* Pod enters `CrashLoopBackOff` immediately upon scheduling on a worker.
* Log aggregation engines index the structured string `FATAL_TOPOLOGY_VIOLATION`.
* No `ClusterShutdown` custom resource can be emitted from an invalid host.

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
                dryRun:
                  type: boolean
                  default: false
                powerManagement:
                  type: object
                  properties:
                    enableKillpower:
                      type: boolean
                      default: false
                      description: "Instructs the UPS to schedule a hard load cut. If false, nodes power off but UPS outlets remain energized."
                    killpowerDelaySeconds:
                      type: integer
                      default: 60
                    targetOutletGroup:
                      type: string
                      default: ""
                      description: "Optional addressable outlet bank (e.g. '1'). Blank targets the whole UPS."
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
                    - Failed
                observedGeneration:
                  type: integer
                startTime:
                  type: string
                  format: date-time
                completionTime:
                  type: string
                  format: date-time
                activeLeaderNode:
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

The controller and agents communicate asynchronously via atomic updates to `Node` object annotations:

| Annotation Key | Values | Direction | Operational Meaning |
| --- | --- | --- | --- |
| `kushd.io/stage` | `idle`, `drain`, `halt` | Controller $\to$ Agent | Target lifecycle state command. |
| `kushd.io/agent-status` | `ready`, `draining`, `drained`, `halting`, `failed` | Agent $\to$ Controller | Reported execution state of the node host. |

---

## 5. Orchestration State Machine

```
   [Trigger Event]
          │
          ▼
   ┌───────────────┐
   │    Pending    │  Validate CR integrity, elect leader, check locks
   └──────┬────────┘
          │
          ▼
 ┌─────────────────┐
 │ CordoningCluster│  Apply spec.unschedulable = true across ALL nodes
 └────────┬────────┘
          │
          ▼
 ┌─────────────────┐
 │ DrainingWorkers │  Set kushd.io/stage: drain on worker nodes
 └────────┬────────┘
          │ (All workers report "drained" OR timeout expires)
          ▼
 ┌─────────────────┐
 │ HaltingWorkers  │  Set kushd.io/stage: halt on worker nodes
 └────────┬────────┘
          │ (All workers transition to NotReady / offline)
          ▼
┌──────────────────────┐
│ DrainingControlPlane │  Set kushd.io/stage: drain on (N-1) CP follower nodes
└─────────┬────────────┘
          │ (Followers report "drained")
          ▼
┌──────────────────────┐
│ HaltingControlPlane  │  Halt (N-1) CP nodes; active leader host stays online
└─────────┬────────────┘
          │
          ▼
   ┌───────────────┐
   │  Finalizing   │  Flush local buffers; conditional killpower command
   └──────┬────────┘
          │
          ▼
   ┌───────────────┐
   │   Completed   │  Leader node executes systemd PowerOff()
   └───────────────┘

```

### Shared Loads & The Killpower Decision

```
                 [Finalizing Phase]
                         │
        /────────────────────────────────\
       <   spec.powerManagement.          >
       <   enableKillpower == true?       >
        \────────────────────────────────/
               │                    │
            YES│                    │NO
               ▼                    ▼
   ┌──────────────────────┐  ┌───────────────────────────────────┐
   │ Issue UPS shutdown   │  │ Bypass UPS load cut.              │
   │ command with delay.  │  │ Shared loads (switches, modems)   │
   │ Outlets cut off.     │  │ remain energized on battery.      │
   └───────────┬──────────┘  └─────────────────┬─────────────────┘
               │                               │
               └───────────────┬───────────────┘
                               │
                               ▼
            ┌────────────────────────────────────┐
            │ Leader host calls PowerOff()       │
            └────────────────────────────────────┘

```

* **When `enableKillpower: true**`: Suitable for dedicated compute racks. Once the leader host halts, the UPS cuts power. Upon mains power restoration, the motherboards detect AC power return and auto-power-on via BIOS configuration.
* **When `enableKillpower: false` (Default)**: Prevents killing shared infrastructure (e.g., WAN routers, network switches, ambient low-power monitors). Compute hosts remain in soft-off (`S5`), and require Wake-on-LAN (WoL) or BMC/IPMI triggers once mains power stabilizes.

---

## 6. Node Agent & Host Power Execution

The `kushd-agent` interacts with the host system through the native systemd D-Bus IPC socket (`/var/run/dbus/system_bus_socket`), bypassing shell wrapper overhead.

```go
package main

import (
	"context"
	"fmt"
	"syscall"

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
	// Synchronously flush all dirty block buffers to persistent storage
	syscall.Sync()

	systemd := h.conn.Object("org.freedesktop.systemd1", "/org/freedesktop/systemd1")
	
	// 'replace' job mode cancels existing jobs and executes immediate shutdown
	call := systemd.CallWithContext(ctx, "org.freedesktop.systemd1.Manager.PowerOff", 0)
	if call.Err != nil {
		return fmt.Errorf("D-Bus PowerOff invocation failed: %w", call.Err)
	}
	return nil
}

```

### Secondary Fallback Execution

If D-Bus is unreachable due to socket corruption or service failures, `kushd-agent` falls back to direct chroot execution:

```bash
chroot /host /usr/bin/systemctl poweroff --force --force

```

---

## 7. Packaging: Nix-Based Container Images

Kushd container images are constructed using **Nix `dockerTools**` via flakes. This produces minimal, bit-for-bit reproducible, unbloated container layers containing strictly the compiled static Go binary, IANA TLS certificates, and essential host D-Bus bindings.

### `flake.nix`

```nix
{
  description = "Kushd (Kubernetes Shutdown Daemon) Container Suite";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-24.05";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };

        # Build statically linked Go binaries
        buildKushdBin = name: pkgs.buildGoModule {
          pname = name;
          version = "0.1.0";
          src = ./.;
          subPackages = [ "cmd/${name}" ];
          vendorHash = null; # Set appropriate vendor hash
          CGO_ENABLED = 0;
          ldflags = [ "-s" "-w" "-extldflags '-static'" ];
        };

        kushdController = buildKushdBin "kushd-controller";
        kushdAgent = buildKushdBin "kushd-agent";
        kushdTrigger = buildKushdBin "kushd-trigger";

        # Base layered container construction
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
          };
          agent-image = buildKushdImage {
            name = "kushd-agent";
            package = kushdAgent;
            # Include systemd client tooling for fallback routines
            extraContents = [ pkgs.systemd ];
          };
          trigger-image = buildKushdImage {
            name = "kushd-trigger";
            package = kushdTrigger;
            extraContents = [ pkgs.nut ];
          };
        };
      }
    );
}

```

---

## 8. Deployment: Helm Chart Architecture

### 8.1 Chart Directory Layout

```
charts/kushd/
├── Chart.yaml
├── values.yaml
├── crds/
│   └── clustershutdowns.kushd.io.yaml
└── templates/
    ├── _helpers.tpl
    ├── rbac-controller.yaml
    ├── rbac-agent.yaml
    ├── deployment-controller.yaml
    ├── daemonset-agent.yaml
    └── deployment-trigger.yaml

```

### 8.2 `Chart.yaml`

```yaml
apiVersion: v2
name: kushd
description: Kubernetes Shutdown Daemon - Topology-Aware UPS Orchestrator
type: application
version: 0.1.0
appVersion: "0.1.0"
kubeVersion: ">=1.26.0"
keywords:
  - power-management
  - ups
  - bare-metal
  - high-availability

```

### 8.3 `values.yaml`

```yaml
global:
  imageRegistry: ghcr.io/kushd
  imagePullPolicy: IfNotPresent

controller:
  replicaCount: 2
  image:
    repository: kushd-controller
    tag: v0.1.0
  resources:
    limits:
      cpu: 100m
      memory: 128Mi
    requests:
      cpu: 20m
      memory: 64Mi
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
    tag: v0.1.0
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

trigger:
  enabled: true
  image:
    repository: kushd-trigger
    tag: v0.1.0
  upsDevice:
    name: "ups"
    driver: "usbhid-ups"
    port: "auto"
  powerManagement:
    enableKillpower: false
    killpowerDelaySeconds: 60
    targetOutletGroup: ""
  thresholds:
    batteryLowPercent: 20
    runtimeLowSeconds: 300
  nodeSelector:
    node-role.kubernetes.io/control-plane: ""
  tolerations:
    - key: "node-role.kubernetes.io/control-plane"
      operator: "Exists"
      effect: "NoSchedule"

```

---

## 9. CI/CD: GitHub Actions Publishing Pipeline

The delivery pipeline consists of two isolated, reproducible GitHub Actions workflows:

1. **Container Image Pipeline:** Builds the Nix container layers and pushes multi-architecture images to GitHub Packages (GHCR).
2. **Helm Release Pipeline:** Lints, templates, packages, and pushes the Helm chart as an **OCI artifact** directly to GHCR.

### 9.1 Container Release Workflow (`.github/workflows/publish-containers.yaml`)

```yaml
name: Publish Container Images

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
        component: [controller-image, agent-image, trigger-image]
        include:
          - component: controller-image
            image-name: kushd-controller
          - component: agent-image
            image-name: kushd-agent
          - component: trigger-image
            image-name: kushd-trigger

    steps:
      - name: Checkout Repository
        uses: actions/checkout@v4

      - name: Install Nix with Flake Support
        uses: cachix/install-nix-action@v27
        with:
          extra_nix_config: |
            experimental-features = nix-command flakes

      - name: Log into GitHub Container Registry
        uses: docker/login-action@v3
        with:
          registry: ghcr.io
          username: ${{ github.actor }}
          password: ${{ secrets.GITHUB_TOKEN }}

      - name: Build Layered OCI Archive via Nix
        run: |
          nix build .#${{ matrix.component }}
          ./result | docker load

      - name: Tag and Publish Image
        run: |
          IMAGE_ID=ghcr.io/${{ github.repository_owner }}/${{ matrix.image-name }}
          VERSION=${GITHUB_REF#refs/tags/}
          
          docker tag ${{ matrix.image-name }}:latest $IMAGE_ID:$VERSION
          docker tag ${{ matrix.image-name }}:latest $IMAGE_ID:latest
          
          docker push $IMAGE_ID:$VERSION
          docker push $IMAGE_ID:latest

```

### 9.2 Helm OCI Release Workflow (`.github/workflows/publish-helm.yaml`)

```yaml
name: Publish Helm Chart (OCI)

on:
  push:
    tags:
      - 'v*'

jobs:
  publish-chart:
    runs-on: ubuntu-latest
    permissions:
      contents: read
      packages: write

    steps:
      - name: Checkout Code
        uses: actions/checkout@v4

      - name: Set up Helm
        uses: azure/setup-helm@v4
        with:
          version: v3.14.0

      - name: Lint Helm Chart
        run: |
          helm lint charts/kushd

      - name: Package Chart
        run: |
          VERSION=${GITHUB_REF#refs/tags/v}
          helm package charts/kushd --version $VERSION --app-version $VERSION -d .cr-release-packages

      - name: Log in to GHCR for OCI
        run: |
          echo "${{ secrets.GITHUB_TOKEN }}" | helm registry login ghcr.io -u ${{ github.actor }} --password-stdin

      - name: Publish Chart to GHCR OCI Registry
        run: |
          VERSION=${GITHUB_REF#refs/tags/v}
          helm push .cr-release-packages/kushd-${VERSION}.tgz oci://ghcr.io/${{ github.repository_owner }}/charts

```

---

## 10. Reviewer Feedback & Iteration Matrix

| Section / Proposal | Focus Area | Reviewer Checklist & Verification |
| --- | --- | --- |
| **Topology Panic** | Hardware Safety | Does the fail-fast check execute before any discovery or network allocation occurs? |
| **Killpower Optionality** | Rack Architecture | Are the defaults correctly established to prevent drops of shared network switches? |
| **D-Bus vs chroot** | Node Host Security | Does the host path socket mount satisfy modern container security policies (SELinux / AppArmor)? |
| **Nix Container Tooling** | Supply Chain Security | Do static binary outputs cleanly build within GitHub Actions execution budgets? |
| **OCI Helm Registry** | Distribution Model | Are CRDs cleanly isolated in `crds/` to avoid helm lifecycle reconcile overwrites? |