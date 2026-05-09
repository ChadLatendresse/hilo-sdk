package hilo

import (
	"encoding/json"
	"testing"
)

func TestOperationUnmarshal(t *testing.T) {
	t.Parallel()
	in := []byte(`{"sessionId":"7c5b3e10-4d20-4f0a-9c0d-3a1bb7d9e110","operationId":"a1b2c3d4-e5f6-4789-9abc-def012345678","deviceType":"BasicThermostat","status":"SUCCEEDED","statusReason":"NONE"}`)
	var op Operation
	if err := json.Unmarshal(in, &op); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if op.Status != OperationStatusSucceeded {
		t.Errorf("status=%v", op.Status)
	}
	if op.StatusReason != OperationStatusReasonNone {
		t.Errorf("reason=%v", op.StatusReason)
	}
	if op.DeviceType != "BasicThermostat" {
		t.Errorf("deviceType=%v", op.DeviceType)
	}
}

func TestOperationStatusTerminal(t *testing.T) {
	t.Parallel()
	terminals := []OperationStatus{
		OperationStatusSucceeded, OperationStatusFailed, OperationStatusRejected,
		OperationStatusSuperseded, OperationStatusTimedOut,
	}
	for _, s := range terminals {
		if !s.IsTerminal() {
			t.Errorf("%s should be terminal", s)
		}
	}
	if OperationStatusAccepted.IsTerminal() {
		t.Error("ACCEPTED should not be terminal")
	}
}
