package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	GroupName = "kushd.io"
	Version   = "v1alpha1"

	// Annotation keys
	AnnotationStage        = "kushd.io/stage"
	AnnotationDrainTimeout = "kushd.io/drain-timeout"
	AnnotationAgentStatus  = "kushd.io/agent-status"

	// Stage values (Manager -> Agent)
	StageIdle  = "idle"
	StageDrain = "drain"
	StageHalt  = "halt"

	// Agent status values (Agent -> Manager)
	AgentStatusReady    = "ready"
	AgentStatusDraining = "draining"
	AgentStatusDrained  = "drained"
	AgentStatusHalting  = "halting"
	AgentStatusFailed   = "failed"

	// Default timeouts
	DefaultDrainTimeout               = "90s"
	DefaultWorkerEvacuationSeconds    = 120
	DefaultControlPlaneEvacuationSecs = 60
	DefaultKillpowerDelaySeconds      = 180

	// Node role labels
	LabelNodeRoleControlPlane = "node-role.kubernetes.io/control-plane"
	LabelNodeRoleMaster       = "node-role.kubernetes.io/master"

	// Condition types & reasons
	ConditionTypeAbortRejected     = "AbortRejected"
	ConditionReasonPointOfNoReturn = "PointOfNoReturnExceeded"
)

type ClusterShutdownPhase string

const (
	PhasePending              ClusterShutdownPhase = "Pending"
	PhaseCordoningCluster     ClusterShutdownPhase = "CordoningCluster"
	PhaseDrainingWorkers      ClusterShutdownPhase = "DrainingWorkers"
	PhaseHaltingWorkers       ClusterShutdownPhase = "HaltingWorkers"
	PhaseDrainingControlPlane ClusterShutdownPhase = "DrainingControlPlane"
	PhaseHaltingControlPlane  ClusterShutdownPhase = "HaltingControlPlane"
	PhaseFinalizing           ClusterShutdownPhase = "Finalizing"
	PhaseCompleted            ClusterShutdownPhase = "Completed"
	PhaseAborted              ClusterShutdownPhase = "Aborted"
	PhaseFailed               ClusterShutdownPhase = "Failed"
)

// PowerManagementSpec defines UPS load power-cut configuration.
type PowerManagementSpec struct {
	EnableKillpower       bool   `json:"enableKillpower,omitempty"`
	KillpowerDelaySeconds int    `json:"killpowerDelaySeconds,omitempty"`
	TargetOutletGroup     string `json:"targetOutletGroup,omitempty"`
}

// TimeoutsSpec defines per-tier evacuation timeout limits.
type TimeoutsSpec struct {
	WorkerEvacuationSeconds       int `json:"workerEvacuationSeconds,omitempty"`
	ControlPlaneEvacuationSeconds int `json:"controlPlaneEvacuationSeconds,omitempty"`
}

// ClusterShutdownSpec defines desired state of a cluster shutdown request.
type ClusterShutdownSpec struct {
	TriggerSource           string              `json:"triggerSource"`
	BatteryChargePercent    int                 `json:"batteryChargePercent"`
	EstimatedRuntimeSeconds int                 `json:"estimatedRuntimeSeconds"`
	Abort                   bool                `json:"abort,omitempty"`
	DryRun                  bool                `json:"dryRun,omitempty"`
	PowerManagement         PowerManagementSpec `json:"powerManagement,omitempty"`
	Timeouts                TimeoutsSpec        `json:"timeouts,omitempty"`
}

// Condition represents the latest available observations of a shutdown state.
type Condition struct {
	Type               string      `json:"type"`
	Status             string      `json:"status"`
	LastTransitionTime metav1.Time `json:"lastTransitionTime,omitempty"`
	Reason             string      `json:"reason,omitempty"`
	Message            string      `json:"message,omitempty"`
}

// ClusterShutdownStatus defines observed state of cluster shutdown.
type ClusterShutdownStatus struct {
	Phase              ClusterShutdownPhase `json:"phase,omitempty"`
	ObservedGeneration int64                `json:"observedGeneration,omitempty"`
	StartTime          *metav1.Time         `json:"startTime,omitempty"`
	CompletionTime     *metav1.Time         `json:"completionTime,omitempty"`
	AnchorNode         string               `json:"anchorNode,omitempty"`
	Conditions         []Condition          `json:"conditions,omitempty"`
}

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ClusterShutdown represents a cluster-wide orderly shutdown workflow.
type ClusterShutdown struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ClusterShutdownSpec   `json:"spec"`
	Status ClusterShutdownStatus `json:"status,omitempty"`
}

// DeepCopyObject returns a generically typed copy of an object
func (in *ClusterShutdown) DeepCopyObject() runtime.Object {
	if in == nil {
		return nil
	}
	out := new(ClusterShutdown)
	in.DeepCopyInto(out)
	return out
}

func (in *ClusterShutdown) DeepCopyInto(out *ClusterShutdown) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	out.Spec = in.Spec
	in.Status.DeepCopyInto(&out.Status)
}

func (in *ClusterShutdownStatus) DeepCopyInto(out *ClusterShutdownStatus) {
	*out = *in
	if in.StartTime != nil {
		in, out := &in.StartTime, &out.StartTime
		*out = (*in).DeepCopy()
	}
	if in.CompletionTime != nil {
		in, out := &in.CompletionTime, &out.CompletionTime
		*out = (*in).DeepCopy()
	}
	if in.Conditions != nil {
		in, out := &in.Conditions, &out.Conditions
		*out = make([]Condition, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
}

func (in *Condition) DeepCopyInto(out *Condition) {
	*out = *in
	in.LastTransitionTime.DeepCopyInto(&out.LastTransitionTime)
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

type ClusterShutdownList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ClusterShutdown `json:"items"`
}

func (in *ClusterShutdownList) DeepCopyObject() runtime.Object {
	if in == nil {
		return nil
	}
	out := new(ClusterShutdownList)
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		out.Items = make([]ClusterShutdown, len(in.Items))
		for i := range in.Items {
			in.Items[i].DeepCopyInto(&out.Items[i])
		}
	}
	return out
}

var (
	SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: Version}
	ClusterShutdownGVR = schema.GroupVersionResource{Group: GroupName, Version: Version, Resource: "clustershutdowns"}
)
