package hilo

import (
	"encoding/json"
	"os"
	"testing"
)

func loadFixture(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatalf("read fixture %s: %v", name, err)
	}
	return b
}

func TestContainerUnmarshal(t *testing.T) {
	t.Parallel()
	b := loadFixture(t, "twin_container.json")
	var resp struct {
		GetLocation Container `json:"getLocation"`
	}
	if err := json.Unmarshal(b, &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	c := resp.GetLocation
	if c.HiloID == "" {
		t.Error("HiloID empty")
	}
	if len(c.Devices) == 0 {
		t.Error("no devices")
	}

	// Verify dispatch picks the right concrete types.
	sawGateway := false
	sawThermostat := false
	for _, d := range c.Devices {
		switch d := d.(type) {
		case *Gateway:
			sawGateway = true
			if d.HiloID == "" {
				t.Error("gateway hiloId empty")
			}
		case *BasicThermostat:
			sawThermostat = true
			if d.AmbientTemperature.Value == 0 {
				t.Error("thermostat ambientTemperature value zero")
			}
		}
	}
	if !sawGateway {
		t.Error("expected at least one Gateway")
	}
	if !sawThermostat {
		t.Error("expected at least one BasicThermostat")
	}
}
