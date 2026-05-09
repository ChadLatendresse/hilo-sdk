package hilo

// OperationStatus is the lifecycle status of a write operation as the
// digital-twin backend tracks it. The Hilo app surfaces only REPORT in its
// UI; this SDK exposes the full set so callers can detect rejection,
// supersession, and timeouts.
type OperationStatus string

const (
	OperationStatusReport     OperationStatus = "REPORT"
	OperationStatusAccepted   OperationStatus = "ACCEPTED"
	OperationStatusRejected   OperationStatus = "REJECTED"
	OperationStatusSucceeded  OperationStatus = "SUCCEEDED"
	OperationStatusFailed     OperationStatus = "FAILED"
	OperationStatusSuperseded OperationStatus = "SUPERSEDED"
	OperationStatusTimedOut   OperationStatus = "TIMED_OUT"
)

func (s OperationStatus) IsKnown() bool {
	switch s {
	case OperationStatusReport, OperationStatusAccepted, OperationStatusRejected,
		OperationStatusSucceeded, OperationStatusFailed, OperationStatusSuperseded,
		OperationStatusTimedOut:
		return true
	}
	return false
}

// IsTerminal reports whether the operation has reached a final state.
func (s OperationStatus) IsTerminal() bool {
	switch s {
	case OperationStatusSucceeded, OperationStatusFailed, OperationStatusRejected,
		OperationStatusSuperseded, OperationStatusTimedOut:
		return true
	}
	return false
}

// OperationStatusReason is the reason field returned with a status.
type OperationStatusReason string

const (
	OperationStatusReasonNone            OperationStatusReason = "NONE"
	OperationStatusReasonInvalidArgument OperationStatusReason = "INVALID_ARGUMENT"
)

func (r OperationStatusReason) IsKnown() bool {
	switch r {
	case OperationStatusReasonNone, OperationStatusReasonInvalidArgument:
		return true
	}
	return false
}

// Operation is the structured response returned by the digital-twin backend
// for any write/command. The GraphQL Subscription endpoint also emits this
// shape on `onAnyLocationUpdated`.
type Operation struct {
	SessionID    string                `json:"sessionId,omitempty"`
	OperationID  string                `json:"operationId,omitempty"`
	DeviceType   string                `json:"deviceType"`
	Status       OperationStatus       `json:"status"`
	StatusReason OperationStatusReason `json:"statusReason"`
}
