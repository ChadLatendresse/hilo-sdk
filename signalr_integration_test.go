//go:build integration

package hilo

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"
)

func TestIntegrationDeviceHubSnapshot(t *testing.T) {
	if os.Getenv("HILO_INTEGRATION") != "1" {
		t.Skip("set HILO_INTEGRATION=1 to run")
	}
	locArg := os.Getenv("HILO_TEST_LOCATION_ID")
	if locArg == "" {
		t.Skip("HILO_TEST_LOCATION_ID not set; can't pick a location")
	}
	locID, err := strconv.Atoi(locArg)
	if err != nil {
		t.Fatalf("HILO_TEST_LOCATION_ID is not an integer: %v", err)
	}

	_ = LoadDotEnv(".env")
	c := NewClient()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	stream, err := c.SubscribeDeviceList(ctx, locID)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	gotSnapshot := false
	deadline := time.NewTimer(30 * time.Second)
	defer deadline.Stop()

loop:
	for {
		select {
		case upd, ok := <-stream.Updates():
			if !ok {
				break loop
			}
			if upd.Kind == DeviceListSnapshot {
				gotSnapshot = true
				if len(upd.Devices) == 0 {
					t.Error("Snapshot had zero devices")
				}
				break loop
			}
		case <-deadline.C:
			break loop
		}
	}

	if !gotSnapshot {
		t.Fatal("never received DeviceListSnapshot within 30s")
	}

	// Cancel and verify clean shutdown.
	cancel()
	select {
	case <-stream.Updates():
	case <-time.After(5 * time.Second):
		t.Fatal("Updates channel did not close within 5s of cancel")
	}
}
