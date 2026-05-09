package hilo

import "testing"

func TestEnumIsKnown(t *testing.T) {
	t.Parallel()
	if !DeviceStateOn.IsKnown() {
		t.Error("DeviceStateOn should be known")
	}
	if DeviceState("MADE_UP_VALUE").IsKnown() {
		t.Error("MADE_UP_VALUE should not be known")
	}
	if !ThermostatModeAuto.IsKnown() {
		t.Error("ThermostatModeAuto should be known")
	}
	if !OperationStatusSucceeded.IsKnown() {
		t.Error("OperationStatusSucceeded should be known")
	}
}
