//go:build integration

// READ-ONLY GUARANTEE.
// Every test in this file may only call GET-only client methods against the
// live API. Adding write tests here (PATCH/POST/PUT/DELETE) requires explicit
// review and a separate gating env var — do NOT add them under HILO_INTEGRATION
// alone, because that variable is documented as safe to enable on real accounts.
//
// If you need to test a write path, define a new build tag (e.g. `destructive`)
// and a new env var (e.g. HILO_INTEGRATION_WRITE=1) and document them
// explicitly. The device-write layer is the right place for that.

package hilo

import (
	"context"
	"os"
	"testing"
)

// TestIntegrationListLocations exercises the live API. Requires HILO_EMAIL +
// HILO_PASSWORD in env (or in a .env file in cwd). Read-only: a single GET to
// /Automation/v1/api/Locations.
func TestIntegrationListLocations(t *testing.T) {
	if os.Getenv("HILO_INTEGRATION") != "1" {
		t.Skip("set HILO_INTEGRATION=1 to run")
	}
	_ = LoadDotEnv(".env")
	c := NewClient()
	if c.Email == "" || c.Password == "" {
		t.Skip("HILO_EMAIL/HILO_PASSWORD not set")
	}
	locs, err := c.ListLocations(context.Background())
	if err != nil {
		t.Fatalf("ListLocations: %v", err)
	}
	if len(locs) == 0 {
		t.Fatal("no locations on this account")
	}
	t.Logf("got %d location(s); first: %s (%s)", len(locs), locs[0].Name, locs[0].LocationHiloID)
}

// TestIntegrationGetLocationGraphQL exercises one GraphQL Query against the
// live digital-twin API. Read-only.
func TestIntegrationGetLocationGraphQL(t *testing.T) {
	if os.Getenv("HILO_INTEGRATION") != "1" {
		t.Skip("set HILO_INTEGRATION=1 to run")
	}
	_ = LoadDotEnv(".env")
	c := NewClient()
	locs, err := c.ListLocations(context.Background())
	if err != nil {
		t.Fatalf("locations: %v", err)
	}
	cont, err := c.GetLocation(context.Background(), locs[0].LocationHiloID)
	if err != nil {
		t.Fatalf("GetLocation: %v", err)
	}
	if len(cont.Devices) == 0 {
		t.Fatal("no devices in container")
	}
	t.Logf("container hiloId=%s, devices=%d", cont.HiloID, len(cont.Devices))
}
