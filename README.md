# Kushd (Kubernetes Shutdown Daemon)

[![Build & Test](https://github.com/kushd/kushd/actions/workflows/publish-containers.yaml/badge.svg)](https://github.com/kushd/kushd/actions)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

**Kushd** is a topology-aware, event-driven shutdown orchestrator for Kubernetes clusters backed by Uninterruptible Power Supply (UPS) hardware.

Abrupt power failure introduces severe data corruption risks: `etcd` split-brain states, write-cache truncation on distributed block storage (Ceph, Longhorn, local NVMe/SATA), and ungraceful pod termination. Kushd coordinates an ordered, priority-aware cluster decommissioning down to the host OS level (`systemd`), while protecting auxiliary rack infrastructure (switches, routers, NAS) via an opt-in power-cut model.

---

## Architecture Overview

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

### Core Components

1. **`kushd-manager` (Deployment)**:
   - Scheduled onto the control-plane node where the UPS is physically connected via Akri extended resource requests (`akri.sh/ups: 1`).
   - Runs two concurrent loops:
     - **Hardware Poller Loop**: Monitors UPS state via Network UPS Tools (NUT) or USB HID. When battery drops below thresholds or `LOWBATT` is signaled, triggers a `ClusterShutdown` Custom Resource.
     - **Cluster Reconciler Loop**: Manages the shutdown state machine: cordons the cluster, instructs worker agents to drain and halt, orchestrates sequential N-1 control plane decommissioning, and handles final anchor host poweroff.

2. **`kushd-agent` (Privileged DaemonSet)**:
   - Runs across all cluster nodes with access to host D-Bus, root filesystem, and `/proc/sysrq-trigger`.
   - Watches local `Node` annotations (`kushd.io/stage`, `kushd.io/drain-timeout`).
   - Executes local pod drain with PDB escalation (75% polite eviction, final 25% forceful deletion with `gracePeriodSeconds: 0`).
   - Implements a **Three-Tier Poweroff Escalation Ladder (STONITH)** to prevent stateful split-brain corruption:
     1. Host D-Bus (`org.freedesktop.systemd1.Manager.PowerOff`)
     2. chroot fallback (`/usr/bin/systemctl poweroff --force --force`)
     3. Hard STONITH kernel crash (`echo c > /proc/sysrq-trigger`)

---

## Core Invariants

1. **Topology Placement Invariant**: Physical UPS communication hardware must terminate on a control-plane node. `kushd-manager` validates this on startup and exits cleanly (`os.Exit(1)`) without stack traces if scheduled on a worker node.
2. **Akri-Driven Implicit Anchor Scheduling**: `kushd-manager` requests `akri.sh/ups: 1` alongside control-plane `nodeAffinity`. Kubernetes pins the manager to the node with the UPS without manual node labeling.
3. **Point-of-No-Return**: Once worker node evacuation begins, abort requests (whether manual `spec.abort: true` or mains power restoration) are rejected with an `AbortRejected` status condition and Kubernetes Warning event to prevent partitioned cluster states.
4. **Configurable Power Cut (`killpower`)**: `enableKillpower` defaults to `false` to preserve battery reserves for auxiliary rack equipment. When enabled, `killpowerDelaySeconds` is safely clamped to $\ge 180$ seconds to account for `systemd` stop jobs on hung network mounts.
5. **Strict TLS Enforcement**: Kubernetes API access strictly verifies TLS certificates at all times; insecure verification is forbidden.

---

## Quickstart

### Prerequisites

- Kubernetes v1.28+
- [Akri](https://akri.sh) installed with udev discovery handler configured for your UPS:
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

### Installation with Helm

```bash
# Install the ClusterShutdown CRD
kubectl apply -f charts/kushd/crds/clustershutdowns.kushd.io.yaml

# Deploy Kushd via Helm
helm install kushd charts/kushd \
  --namespace kube-system \
  --set manager.powerManagement.enableKillpower=false \
  --set manager.thresholds.batteryLowPercent=20 \
  --set manager.thresholds.runtimeLowSeconds=300
```

---

## Configuration Reference

Key values in `charts/kushd/values.yaml`:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `manager.akriResource` | `akri.sh/ups` | Akri extended resource name for the UPS hardware |
| `manager.powerManagement.enableKillpower` | `false` | Whether to instruct the UPS to cut load power after node halts |
| `manager.powerManagement.killpowerDelaySeconds` | `180` | Delay before UPS power cut (must be $\ge 180\text{s}$) |
| `manager.thresholds.batteryLowPercent` | `20` | Battery charge percentage threshold triggering shutdown |
| `manager.thresholds.runtimeLowSeconds` | `300` | Estimated battery runtime threshold triggering shutdown |
| `manager.timeouts.workerEvacuationSeconds` | `120` | Evacuation window for worker nodes |
| `manager.timeouts.controlPlaneEvacuationSeconds` | `60` | Evacuation window per control plane follower node |
| `agent.securityContext.privileged` | `true` | Required for D-Bus and sysrq-trigger host access |
| `agent.securityContext.seLinuxOptions.type` | `spc_t` | Super Privileged Container context for SELinux |

---

## Manual / Dry-Run Trigger

You can manually trigger or test a cluster shutdown by creating a `ClusterShutdown` resource:

```yaml
apiVersion: kushd.io/v1alpha1
kind: ClusterShutdown
metadata:
  name: manual-shutdown-test
spec:
  triggerSource: "ManualAdminTrigger"
  batteryChargePercent: 100
  estimatedRuntimeSeconds: 3600
  dryRun: true # Set to true to test orchestration without terminating hosts
  powerManagement:
    enableKillpower: false
```

---

## Development & Testing

### Running Tests

```bash
# Run unit and integration tests with race detection
go test -v -race ./...

# Run static analysis
go vet ./...

# Lint Helm charts
helm lint charts/kushd
```

### Building Container Images via Nix

```bash
nix build .#manager-image
nix build .#agent-image
```

---

## License

Apache License 2.0. See [LICENSE](LICENSE) for details.
