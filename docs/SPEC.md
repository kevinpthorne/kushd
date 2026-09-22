# Kushd (Kubernetes Shutdown Daemon) System Architecture & Technical Specification

Kushd is an event-driven, topology-aware cluster shutdown orchestrator for Kubernetes clusters backed by an uninterruptible power supply (UPS). It enforces ordered node decommissioning to protect `etcd` consensus, prevent dirty disk writes on persistent volumes, and ensure bare-metal or hypervisor hosts cleanly halt via `systemd`.

---

## 1. Core System Invariants

1. **Topology Placement Invariant:** Physical UPS communication hardware (USB/Serial via RJ45) **must** terminate on a control-plane node. Kushd enforces a fail-fast startup panic if a UPS device is bound to a worker node.
2. **Asymmetric Shutdown Sequencing:** Worker nodes must completely evacuate and halt before control-plane nodes begin decommissioning.
3. **Quorum Preservation:** Control-plane nodes must decommission sequentially ($N-1 \to \text{Final}$). The node hosting the active `kushd-controller` leader must be the absolute final host to power down.
4. **Point-of-No-Return:** Once worker node eviction begins, power-restoration abort sequences are rejected. The cluster must reach full poweroff to prevent partitioned states across half-decommissioned workloads.

---

## 2. Component Architecture

```
                  ┌──────────────────────────────────────────────┐
                  │              Control Plane Node              │
                  │                                              │
[UPS Hardware] ──►│ [kushd-trigger]                              │
  (USB / Serial)  │       │ (Validates CP role; Panics if worker)│
                  │       ▼                                      │
                  │ [ClusterShutdown CR]                         │
                  │       │                                      │
                  │       ▼                                      │
                  │ [kushd-controller] (Leader)                  │
                  └───────┬──────────────────────────────┬───────┘
                          │                              │
             Phase 1: Worker Nodes          Phase 2: Control Plane
                          │                              │
                          ▼                              ▼
                 ┌─────────────────┐            ┌─────────────────┐
                 │   kushd-agent   │            │   kushd-agent   │
                 │  (Worker Node)  │            │ (CP Follower)   │
                 └────────┬────────┘            └────────┬────────┘
                          ▼                              ▼
                 [systemd / D-Bus]              [systemd / D-Bus]
                 [Host Poweroff  ]              [Host Poweroff  ]

```

### Component Roles

* **`kushd-trigger`**: A single-replica monitoring pod running on the node holding the physical UPS connection (managed via NUT or Akri). Discovers the UPS, monitors battery metrics, and applies the `ClusterShutdown` custom resource when critical power thresholds are crossed.
* **`kushd-controller`**: A leader-elected controller deployed strictly to control-plane nodes. Reconciles `ClusterShutdown` resources, drives the global shutdown state machine, and updates node-level lifecycle annotations.
* **`kushd-agent`**: A privileged DaemonSet deployed across all cluster nodes. Mounts the host D-Bus system socket, monitors its local `Node` object annotations, drains local pods, and calls `systemd` poweroff routines.

---

## 3. The Topology Guard: In-Process Trigger Panic

To eliminate race conditions where a worker node halts while holding the sole UPS connection, `kushd-trigger` performs a mandatory validation probe on initialization.

### Startup Check Algorithm

```
                  ┌──────────────────────────────┐
                  │    kushd-trigger Starts      │
                  └──────────────┬───────────────┘
                                 │
                                 ▼
                  ┌──────────────────────────────┐
                  │ Read Downward API $NODE_NAME │
                  └──────────────┬───────────────┘
                                 │
                                 ▼
                  ┌──────────────────────────────┐
                  │ GET /api/v1/nodes/$NODE_NAME │
                  └──────────────┬───────────────┘
                                 │
                                 ▼
                  /──────────────────────────────\
                 <   Is node-role control-plane?  >
                  \──────────────────────────────/
                                 │
                    ┌────────────┴────────────┐
                 YES│                         │NO
                    ▼                         ▼
   ┌────────────────────────────────┐  ┌──────────────────────────────────┐
   │ Connect to UPS & Start Monitor │  │ 1. Log FATAL topology mismatch   │
   └────────────────────────────────┘  │ 2. panic() with exit code 1      │
                                       │ 3. Enter CrashLoopBackOff        │
                                       └──────────────────────────────────┘

```

### Implementation Contract (`kushd-trigger`)

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
		// Log message optimized for log aggregation and alert matching
		fmt.Fprintf(os.Stderr, "FATAL_TOPOLOGY_VIOLATION: UPS cable detected on worker node %q. "+
			"kushd-trigger must run strictly on a control-plane node to prevent premature power severance "+
			"during worker node drain phases. Exiting immediately.\n", nodeName)
		
		// In-process panic triggers immediate container exit and CrashLoopBackOff
		panic("TOPOLOGY_MISMATCH_WORKER_ATTACHED_UPS")
	}
}

```

### Operational Consequences of the Panic

* Pod status becomes `CrashLoopBackOff`.
* Prometheus fires `KubePodCrashLooping` or a custom alert matching `FATAL_TOPOLOGY_VIOLATION`.
* No `ClusterShutdown` custom resource can be created from this node.
* Deployment remains blocked until an administrator physically moves the USB cable or corrects node labels.

---

## 4. API & Resource Definitions

### 4.1 Custom Resource Definition: `ClusterShutdown`

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

### 4.2 Node Annotation Protocol

Communication between `kushd-controller` and individual `kushd-agent` instances occurs through atomic node annotations:

| Annotation Key | Values | Directed To | Description |
| --- | --- | --- | --- |
| `kushd.io/stage` | `idle`, `drain`, `halt` | Agent | Action directive sent from controller to node agent. |
| `kushd.io/agent-status` | `ready`, `draining`, `drained`, `halting`, `failed` | Controller | Health and status indicator reported by node agent. |
| `kushd.io/killpower-eligible` | `true`, `false` | Agent | Marks the final host designated to issue hardware cut commands. |

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
 │ CordoningCluster│  Set spec.unschedulable = true on ALL cluster nodes
 └────────┬────────┘
          │
          ▼
 ┌─────────────────┐
 │ DrainingWorkers │  Set kushd.io/stage: drain on worker nodes
 └────────┬────────┘
          │ (All workers report "drained" OR workerEvacuationSeconds expires)
          ▼
 ┌─────────────────┐
 │ HaltingWorkers  │  Set kushd.io/stage: halt on worker nodes
 └────────┬────────┘
          │ (All workers transition to "NotReady" or network drops)
          ▼
┌──────────────────────┐
│ DrainingControlPlane │  Set kushd.io/stage: drain on (N-1) control-plane nodes
└─────────┬────────────┘
          │ (Followers report "drained")
          ▼
┌──────────────────────┐
│ HaltingControlPlane  │  Halt (N-1) control plane nodes. 
└─────────┬────────────┘  Preserve leader node running kushd-controller.
          │
          ▼
   ┌───────────────┐
   │  Finalizing   │  Leader executes local etcd snapshot flush;
   └──────┬────────┘  Transfers killpower command to UPS via USB
          │
          ▼
   ┌───────────────┐
   │   Completed   │  Leader host invokes systemd PowerOff(); Power severed.
   └───────────────┘

```

### Phase Breakdown

#### Phase 1: Global Cordoning (`CordoningCluster`)

* The controller iterates over every node in the cluster and applies `spec.unschedulable: true`.
* **Goal:** Terminate new pod creation and block StatefulSet failover loops before any evictions begin.

#### Phase 2: Worker Node Evacuation (`DrainingWorkers`)

* Controller sets `kushd.io/stage: drain` on all nodes matching `!node-role.kubernetes.io/control-plane`.
* Local `kushd-agent` pods receive the change and begin evicting local pods using the Kubernetes Eviction API (`/api/v1/namespaces/{namespace}/pods/{name}/eviction`), honoring PodDisruptionBudgets (PDBs) up to the configured `workerEvacuationSeconds` timeout.
* The agent reports `kushd.io/agent-status: drained` upon completion.

#### Phase 3: Worker Node Halt (`HaltingWorkers`)

* Controller updates annotations on all drained worker nodes: `kushd.io/stage: halt`.
* The `kushd-agent` on each worker executes host poweroff via D-Bus.
* The controller watches for worker nodes to report `Ready: False` or fail to respond to heartbeats.

#### Phase 4: Control-Plane Decommissioning (`DrainingControlPlane` & `HaltingControlPlane`)

* Draining master nodes terminates API servers. The controller handles this by:
1. Reading its own `$NODE_NAME` via downward API to identify the leader host.
2. Selecting all follower control-plane nodes ($N-1$).
3. Setting `kushd.io/stage: drain` then `halt` on followers one-by-one, keeping quorum intact until the last possible moment.



#### Phase 5: Finalizing & Power Cut (`Finalizing` & `Completed`)

* The controller performs an explicit write and sync on local storage to preserve state.
* If configured, the trigger daemon emits the UPS `killpower` instruction (e.g., `upsdrvctl shutdown`), initiating a 30-to-60-second hardware load-off delay.
* The local agent executes `systemd` `PowerOff()` on the leader node host.

---

## 6. Node Agent Specification

### 6.1 Host Bus Interface

The agent interacts with the host OS by communicating directly over the D-Bus IPC socket (`/var/run/dbus/system_bus_socket`), avoiding fragile shell subprocess invocations.

```
┌────────────────────────────────────────────────────────┐
│                      kushd-agent                       │
│                                                        │
│  1. Inotify/Client Watch (kushd.io/stage == "halt")    │
│  2. sync() (Flush Dirty Disk Buffers)                  │
│  3. Connect D-Bus (unix:///host/run/dbus/system_bus)   │
│  4. Call org.freedesktop.systemd1.Manager.PowerOff()   │
└──────────────────────────┬─────────────────────────────┘
                           │
                           ▼
┌────────────────────────────────────────────────────────┐
│                        Host OS                         │
│                                                        │
│  systemd-logind ──► SIGTERM to System Services         │
│                 ──► Kubelet Inhibitor Closes Pods      │
│                 ──► Unmount File Systems               │
│                 ──► ACPI Power State S5 (Halt)         │
└────────────────────────────────────────────────────────┘

```

### 6.2 Host Shutdown Implementation

```go
package main

import (
	"context"
	"fmt"
	"os"
	"syscall"

	"github.com/godbus/dbus/v5"
)

type HostPowerController struct {
	conn *dbus.Conn
}

func NewHostPowerController() (*HostPowerController, error) {
	// Explicitly connect to the host system bus mounted into the container
	conn, err := dbus.NewConnection("unix:path=/host/run/dbus/system_bus_socket")
	if err != nil {
		return nil, fmt.Errorf("failed to connect to host D-Bus: %w", err)
	}
	return &HostPowerController{conn: conn}, nil
}

func (h *HostPowerController) PowerOff(ctx context.Context) error {
	// Sync host filesystem buffers prior to triggering poweroff
	syscall.Sync()

	systemd := h.conn.Object("org.freedesktop.systemd1", "/org/freedesktop/systemd1")
	
	// Mode 'replace' cancels any pending unit jobs and halts immediately
	call := systemd.CallWithContext(ctx, "org.freedesktop.systemd1.Manager.PowerOff", 0)
	if call.Err != nil {
		return fmt.Errorf("systemd PowerOff call failed: %w", call.Err)
	}

	return nil
}

```

### 6.3 Secondary Fallback

If the D-Bus socket is corrupted, unmounted, or unresponsive, the agent falls back to direct chroot execution:

```bash
chroot /host /usr/bin/systemctl poweroff --force --force

```

---

## 7. RBAC & Security Posture

### 7.1 Principle of Least Privilege

Kushd operates with explicitly bounded ClusterRoles. It does not require `cluster-admin`.

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: kushd-controller
  namespace: kube-system
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: kushd-controller
rules:
  - apiGroups: [""]
    resources: ["nodes"]
    verbs: ["get", "list", "watch", "patch", "update"]
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["get", "list", "watch"]
  - apiGroups: [""]
    resources: ["pods/eviction"]
    verbs: ["create"]
  - apiGroups: ["kushd.io"]
    resources: ["clustershutdowns"]
    verbs: ["get", "list", "watch", "update", "patch"]
  - apiGroups: ["kushd.io"]
    resources: ["clustershutdowns/status"]
    verbs: ["get", "update", "patch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: kushd-controller
subjects:
  - kind: ServiceAccount
    name: kushd-controller
    namespace: kube-system
roleRef:
  kind: ClusterRole
  name: kushd-controller
  apiGroup: rbac.authorization.k8s.io
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: kushd-agent
  namespace: kube-system
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: kushd-agent
rules:
  - apiGroups: [""]
    resources: ["nodes"]
    verbs: ["get", "watch", "patch"]
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["get", "list"]
  - apiGroups: [""]
    resources: ["pods/eviction"]
    verbs: ["create"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: kushd-agent
subjects:
  - kind: ServiceAccount
    name: kushd-agent
    namespace: kube-system
roleRef:
  kind: ClusterRole
  name: kushd-agent
  apiGroup: rbac.authorization.k8s.io

```

### 7.2 Pod Security Standards

* `kushd-controller`: Operates under the `restricted` profile. Non-root user, read-only root filesystem, drop all capabilities.
* `kushd-agent`: Requires `privileged: true`, `hostPID: true`, and access to `/run/systemd` and `/var/run/dbus/system_bus_socket` to control the host shutdown subsystems.

---

## 8. Failure Modes & Edge Case Matrix

| Failure Mode | Detection Point | Automated Recovery / Behavior |
| --- | --- | --- |
| **UPS cable plugged into worker node** | `kushd-trigger` initialization | Immediate `panic()`. Pod enters `CrashLoopBackOff`. Cluster-wide shutdown cannot be initiated. |
| **Mains power restored during worker drain** | `kushd-controller` Reconcile loop | Abort rejected if phase $\ge$ `HaltingWorkers`. System continues to full halt to prevent inconsistent state. |
| **Worker node pod eviction deadlock (PDB)** | `kushd-agent` local drain | Eviction respects `workerEvacuationSeconds`. Once expired, remaining non-critical pods are forcibly skipped/terminated via host poweroff. |
| **D-Bus socket failure / disconnect** | `kushd-agent` | Agent falls back to `chroot /host /usr/bin/systemctl poweroff --force --force`. |
| **API Server unresponsive during CP drain** | `kushd-agent` on master | Controller uses an internal static execution schedule: each master delays its shutdown by `Index * 15s`, ensuring sequential teardown even if API server connectivity drops midway. |
| **Partial power cut (half of cluster nodes lose wall power)** | `kushd-controller` | CR reflects actual telemetry from UPS. If UPS battery threshold trips, entire cluster gracefully shuts down regardless of per-node power states. |