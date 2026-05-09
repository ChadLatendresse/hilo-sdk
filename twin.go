package hilo

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// Container is the root object returned by getLocation.
type Container struct {
	HiloID            HiloID    `json:"hiloId"`
	LastUpdate        time.Time `json:"lastUpdate"`
	LastUpdateVersion uint64    `json:"lastUpdateVersion"`
	TransmissionTime  time.Time `json:"transmissionTime"`
	Devices           []Device  `json:"devices"`
}

// Device is the union of all device types returned by the digital-twin API.
// Use a type switch to access concrete fields.
type Device interface {
	GetCommon() IBasicDevice
}

// IBasicDevice is the field set common to every device type.
type IBasicDevice struct {
	HiloID             HiloID                 `json:"hiloId"`
	GatewayHiloID      HiloID                 `json:"gatewayHiloId"`
	PhysicalAddress    string                 `json:"physicalAddress"`
	LastUpdate         time.Time              `json:"lastUpdate"`
	LastUpdateVersion  uint64                 `json:"lastUpdateVersion"`
	LastConnectionTime time.Time              `json:"lastConnectionTime"`
	ConnectionStatus   DeviceConnectionStatus `json:"connectionStatus"`
}

// Concrete device types. Each embeds IBasicDevice for the common fields.

type Gateway struct {
	IBasicDevice
	ZigBeeChannel             int  `json:"zigBeeChannel"`
	SmartMeterPairingStatus   bool `json:"smartMeterPairingStatus"`
	WillBeConnectedToSmartMtr bool `json:"willBeConnectedToSmartMeter"`
	SmartMeterZigBeeChannel   int  `json:"smartMeterZigBeeChannel"`
}

func (g *Gateway) GetCommon() IBasicDevice { return g.IBasicDevice }

type BasicSmartMeter struct {
	IBasicDevice
	SmartMeterType SmartMeterKind `json:"smartMeterType"`
	Power          *Power         `json:"power,omitempty"`
	ZigBeeChannel  int            `json:"zigBeeChannel"`
}

func (m *BasicSmartMeter) GetCommon() IBasicDevice { return m.IBasicDevice }

type BasicThermostat struct {
	IBasicDevice
	ThermostatType              ThermostatKind   `json:"thermostatType"`
	Mode                        ThermostatMode   `json:"mode"`
	AllowedModes                []ThermostatMode `json:"allowedModes"`
	HeatDemand                  *int             `json:"heatDemand,omitempty"`
	AmbientHumidity             *int             `json:"ambientHumidity,omitempty"`
	Power                       *Power           `json:"power,omitempty"`
	AmbientTemperature          Temperature      `json:"ambientTemperature"`
	AmbientTempSetpoint         *Temperature     `json:"ambientTempSetpoint,omitempty"`
	MinAmbientTempSetpoint      *Temperature     `json:"minAmbientTempSetpoint,omitempty"`
	MinAmbientTempSetpointLimit *Temperature     `json:"minAmbientTempSetpointLimit,omitempty"`
	MaxAmbientTempSetpoint      *Temperature     `json:"maxAmbientTempSetpoint,omitempty"`
	GDState                     DeviceGDState    `json:"gDState"`
	Model                       string           `json:"model,omitempty"`
	Version                     string           `json:"version,omitempty"`
	ZigbeeVersion               string           `json:"zigbeeVersion,omitempty"`
	Alerts                      []string         `json:"alerts,omitempty"`
}

func (t *BasicThermostat) GetCommon() IBasicDevice { return t.IBasicDevice }

type LowVoltageThermostat struct {
	IBasicDevice
	Mode                ThermostatMode            `json:"mode"`
	FanMode             LowVoltageFanMode         `json:"fanMode"`
	FanCurrentState     LowVoltageFanCurrentState `json:"fanCurrentState"`
	CurrentState        LowVoltageCurrentState    `json:"currentState"`
	AmbientTemperature  Temperature               `json:"ambientTemperature"`
	AmbientTempSetpoint *Temperature              `json:"ambientTempSetpoint,omitempty"`
	Model               string                    `json:"model,omitempty"`
}

func (t *LowVoltageThermostat) GetCommon() IBasicDevice { return t.IBasicDevice }

type HeatingFloorThermostat struct {
	IBasicDevice
	Mode                ThermostatMode      `json:"mode"`
	FloorMode           FloorThermostatMode `json:"floorMode"`
	AmbientTemperature  Temperature         `json:"ambientTemperature"`
	AmbientTempSetpoint *Temperature        `json:"ambientTempSetpoint,omitempty"`
	FloorTemperature    *Temperature        `json:"floorTemperature,omitempty"`
	FloorTempSetpoint   *Temperature        `json:"floorTempSetpoint,omitempty"`
	Model               string              `json:"model,omitempty"`
}

func (t *HeatingFloorThermostat) GetCommon() IBasicDevice { return t.IBasicDevice }

type WaterHeater struct {
	IBasicDevice
	CCRMode CCRMode       `json:"ccrMode"`
	CCRType CCRKind       `json:"ccrType"`
	Power   *Power        `json:"power,omitempty"`
	State   DeviceState   `json:"state"`
	GDState DeviceGDState `json:"gDState"`
	Model   string        `json:"model,omitempty"`
}

func (w *WaterHeater) GetCommon() IBasicDevice { return w.IBasicDevice }

type BasicChargeController struct {
	IBasicDevice
	CCRMode CCRMode       `json:"ccrMode"`
	CCRType CCRKind       `json:"ccrType"`
	State   DeviceState   `json:"state"`
	GDState DeviceGDState `json:"gDState"`
	Power   *Power        `json:"power,omitempty"`
	Model   string        `json:"model,omitempty"`
}

func (c *BasicChargeController) GetCommon() IBasicDevice { return c.IBasicDevice }

type BasicLight struct {
	IBasicDevice
	LightDeviceType  LightDeviceKind `json:"lightDeviceType"`
	State            DeviceState     `json:"state"`
	Level            *int            `json:"level,omitempty"`
	Hue              *int            `json:"hue,omitempty"`
	Saturation       *int            `json:"saturation,omitempty"`
	ColorTemperature *int            `json:"colorTemperature,omitempty"`
	LightType        LightType       `json:"lightType"`
	ColorMode        LightColorMode  `json:"colorMode"`
	Model            string          `json:"model,omitempty"`
}

func (l *BasicLight) GetCommon() IBasicDevice { return l.IBasicDevice }

type BasicDimmer struct {
	IBasicDevice
	DimmerType DimmerKind  `json:"dimmerType"`
	State      DeviceState `json:"state"`
	Level      *int        `json:"level,omitempty"`
	Power      *Power      `json:"power,omitempty"`
	Model      string      `json:"model,omitempty"`
}

func (d *BasicDimmer) GetCommon() IBasicDevice { return d.IBasicDevice }

type BasicSwitch struct {
	IBasicDevice
	SwitchType SwitchKind  `json:"switchType"`
	State      DeviceState `json:"state"`
	Power      *Power      `json:"power,omitempty"`
	Model      string      `json:"model,omitempty"`
}

func (s *BasicSwitch) GetCommon() IBasicDevice { return s.IBasicDevice }

type BasicEVCharger struct {
	IBasicDevice
	ChargingPointType ChargingPointKind   `json:"chargingPointType"`
	Status            ChargingPointStatus `json:"status"`
	Power             *Power              `json:"power,omitempty"`
}

func (e *BasicEVCharger) GetCommon() IBasicDevice { return e.IBasicDevice }

// BasicDevice is the catch-all type for hardware that doesn't fit a more
// specific category yet.
type BasicDevice struct {
	IBasicDevice
	Model string `json:"model,omitempty"`
}

func (d *BasicDevice) GetCommon() IBasicDevice { return d.IBasicDevice }

// UnmarshalJSON for *Container delegates to a custom unmarshaler that
// dispatches each entry in `devices` to the right concrete type based on
// `__typename`.
func (c *Container) UnmarshalJSON(data []byte) error {
	type alias struct {
		HiloID            HiloID            `json:"hiloId"`
		LastUpdate        time.Time         `json:"lastUpdate"`
		LastUpdateVersion uint64            `json:"lastUpdateVersion"`
		TransmissionTime  time.Time         `json:"transmissionTime"`
		Devices           []json.RawMessage `json:"devices"`
	}
	var a alias
	if err := json.Unmarshal(data, &a); err != nil {
		return err
	}
	c.HiloID = a.HiloID
	c.LastUpdate = a.LastUpdate
	c.LastUpdateVersion = a.LastUpdateVersion
	c.TransmissionTime = a.TransmissionTime
	c.Devices = make([]Device, 0, len(a.Devices))
	for i, raw := range a.Devices {
		dev, err := unmarshalDevice(raw)
		if err != nil {
			return fmt.Errorf("device[%d]: %w", i, err)
		}
		c.Devices = append(c.Devices, dev)
	}
	return nil
}

// unmarshalDevice picks the concrete type based on the GraphQL __typename
// field and unmarshals into it.
func unmarshalDevice(raw json.RawMessage) (Device, error) {
	var probe struct {
		Typename string `json:"__typename"`
	}
	if err := json.Unmarshal(raw, &probe); err != nil {
		return nil, err
	}
	var dev Device
	switch probe.Typename {
	case "Gateway":
		dev = &Gateway{}
	case "BasicSmartMeter":
		dev = &BasicSmartMeter{}
	case "BasicThermostat":
		dev = &BasicThermostat{}
	case "LowVoltageThermostat":
		dev = &LowVoltageThermostat{}
	case "HeatingFloorThermostat":
		dev = &HeatingFloorThermostat{}
	case "WaterHeater":
		dev = &WaterHeater{}
	case "BasicChargeController":
		dev = &BasicChargeController{}
	case "BasicLight":
		dev = &BasicLight{}
	case "BasicDimmer":
		dev = &BasicDimmer{}
	case "BasicSwitch":
		dev = &BasicSwitch{}
	case "BasicEVCharger":
		dev = &BasicEVCharger{}
	case "BasicDevice", "":
		dev = &BasicDevice{}
	default:
		return nil, fmt.Errorf("unknown device __typename %q", probe.Typename)
	}
	if err := json.Unmarshal(raw, dev); err != nil {
		return nil, fmt.Errorf("decode %s: %w", probe.Typename, err)
	}
	return dev, nil
}

// GetLocation runs the canonical full-device query against the digital-twin
// GraphQL API for one location and returns the typed Container.
func (c *Client) GetLocation(ctx context.Context, locationHiloID HiloID) (*Container, error) {
	const q = `query($id: String!) {
		getLocation(id: $id) {
			hiloId lastUpdate lastUpdateVersion transmissionTime
			devices {
				__typename
				... on IBasicDevice { hiloId gatewayHiloId connectionStatus lastUpdate lastUpdateVersion lastConnectionTime physicalAddress }
				... on Gateway { zigBeeChannel smartMeterPairingStatus willBeConnectedToSmartMeter smartMeterZigBeeChannel }
				... on BasicSmartMeter { smartMeterType power { value kind } zigBeeChannel }
				... on BasicThermostat {
					thermostatType mode allowedModes heatDemand ambientHumidity model version zigbeeVersion alerts gDState
					ambientTemperature { value kind } ambientTempSetpoint { value kind }
					minAmbientTempSetpoint { value kind } minAmbientTempSetpointLimit { value kind } maxAmbientTempSetpoint { value kind }
					power { value kind }
				}
				... on LowVoltageThermostat {
					mode fanMode fanCurrentState currentState model
					ambientTemperature { value kind } ambientTempSetpoint { value kind }
				}
				... on HeatingFloorThermostat {
					mode floorMode model
					ambientTemperature { value kind } ambientTempSetpoint { value kind }
				}
				... on WaterHeater { ccrMode ccrType state gDState model power { value kind } }
				... on BasicChargeController { ccrMode ccrType state gDState model power { value kind } }
				... on BasicLight {
					lightDeviceType state level hue saturation colorTemperature lightType colorMode model
				}
				... on BasicDimmer { dimmerType state level model power { value kind } }
				... on BasicSwitch { switchType state model power { value kind } }
				... on BasicEVCharger { chargingPointType status power { value kind } }
			}
		}
	}`
	body := map[string]any{
		"query":     q,
		"variables": map[string]any{"id": string(locationHiloID)},
	}
	var resp struct {
		Data struct {
			GetLocation *Container `json:"getLocation"`
		} `json:"data"`
		Errors []GraphQLError `json:"errors,omitempty"`
	}
	if err := c.Post(ctx, "/api/digital-twin/v3/graphql", body, &resp); err != nil {
		return nil, err
	}
	if len(resp.Errors) > 0 {
		return nil, fmt.Errorf("graphql: %s", resp.Errors[0].Message)
	}
	return resp.Data.GetLocation, nil
}
