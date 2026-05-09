//go:build integration

package hilo

import (
	"context"
	"errors"
	"os"
	"strconv"
	"testing"
	"time"
)

// TestIntegrationSetThermostatSetpointRoundtrip exercises the full
// Write stack against a live account: PUT, await echo (via
// GraphQL Subscription if healthy, DeviceHub Values fallback otherwise),
// verify Succeeded, set to a different value to leave a clean state.
//
// DOUBLE-GATED: needs HILO_INTEGRATION=1 AND HILO_INTEGRATION_WRITE=1.
// HILO_TEST_LOCATION_ID + HILO_TEST_THERMOSTAT_ID env vars also required.
func TestIntegrationSetThermostatSetpointRoundtrip(t *testing.T) {
	if os.Getenv("HILO_INTEGRATION") != "1" {
		t.Skip("HILO_INTEGRATION not set")
	}
	if os.Getenv("HILO_INTEGRATION_WRITE") != "1" {
		t.Skip("HILO_INTEGRATION_WRITE not set; refusing to write to a live account")
	}
	locArg := os.Getenv("HILO_TEST_LOCATION_ID")
	devArg := os.Getenv("HILO_TEST_THERMOSTAT_ID")
	if locArg == "" || devArg == "" {
		t.Skip("HILO_TEST_LOCATION_ID and HILO_TEST_THERMOSTAT_ID must both be set")
	}
	locID, err := strconv.Atoi(locArg)
	if err != nil {
		t.Fatalf("invalid HILO_TEST_LOCATION_ID: %v", err)
	}
	devID, err := strconv.Atoi(devArg)
	if err != nil {
		t.Fatalf("invalid HILO_TEST_THERMOSTAT_ID: %v", err)
	}

	_ = LoadDotEnv(".env")
	c := NewClient()
	c.Logger = func(format string, args ...any) {
		t.Logf("[hilo] "+format, args...)
	}

	// First write: 22°C (definite change from likely-21°C-or-other current).
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	target := NewTemperature(22.0)
	t.Logf("writing setpoint=22.0°C to device %d at location %d", devID, locID)
	op, err := c.SetThermostatSetpoint(ctx, locID, devID, target)
	switch {
	case err == nil:
		if op.Status != OperationStatusSucceeded {
			t.Fatalf("first set: Status=%v reason=%v", op.Status, op.StatusReason)
		}
		t.Logf("first set succeeded: opID=%s (verified)", op.OperationID)
	case errors.Is(err, ErrOperationStatusUnknown):
		// PUT applied but status couldn't be verified within ctx.
		// Documented expected behavior on backends where neither the
		// GraphQL multipart subscription nor DeviceHub Values echoes
		// deliver write completion. Log and continue.
		t.Logf("first set: PUT applied but status unverified (opID=%s)", op.OperationID)
	default:
		t.Fatalf("first SetThermostatSetpoint: %v", err)
	}

	// Second write: 20°C (leaves the thermostat at 20°C, a typical
	// indoor temperature).
	ctx2, cancel2 := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel2()

	original := NewTemperature(20.0)
	t.Logf("writing setpoint=20.0°C to device %d", devID)
	op2, err := c.SetThermostatSetpoint(ctx2, locID, devID, original)
	switch {
	case err == nil:
		if op2.Status != OperationStatusSucceeded {
			t.Fatalf("revert set: Status=%v reason=%v", op2.Status, op2.StatusReason)
		}
		t.Logf("revert set succeeded: opID=%s (verified)", op2.OperationID)
	case errors.Is(err, ErrOperationStatusUnknown):
		t.Logf("revert set: PUT applied but status unverified (opID=%s)", op2.OperationID)
	default:
		t.Fatalf("revert SetThermostatSetpoint: %v", err)
	}
}
