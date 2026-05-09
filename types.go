package hilo

// HiloID is the typed identifier shape used by the Hilo backend.
// Examples: "urn:hilo:crm:00000000-anonymous:0", "urn:hilo:philo:0000000000aa:0".
type HiloID string

func (h HiloID) String() string { return string(h) }

// Temperature is a value+unit pair as returned by the GraphQL digital-twin API.
type Temperature struct {
	Value float64         `json:"value"`
	Kind  TemperatureKind `json:"kind"`
}

// Celsius converts the temperature to degrees Celsius. Unrecognized kinds
// fall through as the raw value.
func (t Temperature) Celsius() float64 {
	switch t.Kind {
	case TemperatureKindDegreeCelsius:
		return t.Value
	case TemperatureKindDegreeFahrenheit:
		return (t.Value - 32) * 5 / 9
	case TemperatureKindKelvin:
		return t.Value - 273.15
	case TemperatureKindMillidegreeCelsius:
		return t.Value / 1000
	}
	return t.Value
}

// NewTemperature returns a Temperature with Kind=DegreeCelsius and the
// given Celsius value. Convenience for callers writing setpoints —
// SetThermostatSetpoint takes a Temperature, not a bare float, to
// preserve unit safety.
func NewTemperature(celsius float64) Temperature {
	return Temperature{Value: celsius, Kind: TemperatureKindDegreeCelsius}
}

// Power is a value+unit pair as returned by the GraphQL digital-twin API.
type Power struct {
	Value float64   `json:"value"`
	Kind  PowerKind `json:"kind"`
}

// Watts converts the power reading to watts. Unrecognized kinds fall through.
func (p Power) Watts() float64 {
	switch p.Kind {
	case PowerKindWatt:
		return p.Value
	case PowerKindKilowatt:
		return p.Value * 1000
	case PowerKindMegawatt:
		return p.Value * 1_000_000
	case PowerKindMilliwatt:
		return p.Value / 1000
	}
	return p.Value
}
