package hilo

import (
	"encoding/json"
	"testing"
)

func TestTemperatureRoundTrip(t *testing.T) {
	t.Parallel()
	in := []byte(`{"value":21.5,"kind":"DEGREE_CELSIUS"}`)
	var got Temperature
	if err := json.Unmarshal(in, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Value != 21.5 || got.Kind != TemperatureKindDegreeCelsius {
		t.Fatalf("got %+v", got)
	}
	out, _ := json.Marshal(got)
	if string(out) != `{"value":21.5,"kind":"DEGREE_CELSIUS"}` {
		t.Fatalf("marshal: %s", out)
	}
}

func TestTemperatureCelsius(t *testing.T) {
	t.Parallel()
	tests := []struct {
		in   Temperature
		want float64
	}{
		{Temperature{Value: 20, Kind: TemperatureKindDegreeCelsius}, 20},
		{Temperature{Value: 68, Kind: TemperatureKindDegreeFahrenheit}, 20},
		{Temperature{Value: 293.15, Kind: TemperatureKindKelvin}, 20},
		{Temperature{Value: 20000, Kind: TemperatureKindMillidegreeCelsius}, 20},
	}
	for _, tc := range tests {
		got := tc.in.Celsius()
		if got < tc.want-0.01 || got > tc.want+0.01 {
			t.Errorf("Celsius(%v)=%v, want %v", tc.in, got, tc.want)
		}
	}
}

func TestHiloIDString(t *testing.T) {
	t.Parallel()
	id := HiloID("urn:hilo:crm:00000000-anonymous:0")
	if id.String() != "urn:hilo:crm:00000000-anonymous:0" {
		t.Errorf("got %q", id)
	}
}
