package hilo

// DeviceState is the on/off state of a switch, dimmer, light, or charger.
type DeviceState string

const (
	DeviceStateOn      DeviceState = "ON"
	DeviceStateOff     DeviceState = "OFF"
	DeviceStateUnknown DeviceState = "UNKNOWN"
)

func (s DeviceState) IsKnown() bool {
	switch s {
	case DeviceStateOn, DeviceStateOff, DeviceStateUnknown:
		return true
	}
	return false
}

// DeviceConnectionStatus indicates whether the device's gateway is reachable.
type DeviceConnectionStatus string

const (
	DeviceConnectionStatusConnected    DeviceConnectionStatus = "CONNECTED"
	DeviceConnectionStatusDisconnected DeviceConnectionStatus = "DISCONNECTED"
)

func (s DeviceConnectionStatus) IsKnown() bool {
	switch s {
	case DeviceConnectionStatusConnected, DeviceConnectionStatusDisconnected:
		return true
	}
	return false
}

// DeviceGDState exposes the demand-response / generation-demand state of a
// device. Most values are not surfaced in the public Hilo app.
type DeviceGDState string

const (
	DeviceGDStateExcluded                            DeviceGDState = "EXCLUDED"
	DeviceGDStateActive                              DeviceGDState = "ACTIVE"
	DeviceGDStateUnknown                             DeviceGDState = "UNKNOWN"
	DeviceGDStateOptOutThroughLocation               DeviceGDState = "OPT_OUT_THROUGH_LOCATION"
	DeviceGDStateOptOutThroughMobileApp              DeviceGDState = "OPT_OUT_THROUGH_MOBILE_APP"
	DeviceGDStateOptOutThroughPhysicalDevice         DeviceGDState = "OPT_OUT_THROUGH_PHYSICAL_DEVICE"
	DeviceGDStateCCRActiveSecurityWithClosedRelays   DeviceGDState = "CCR_ACTIVE_SECURITY_WITH_CLOSED_RELAYS"
	DeviceGDStateCCRInactiveSecurityWithOpenedRelays DeviceGDState = "CCR_INACTIVE_SECURITY_WITH_OPENED_RELAYS"
	DeviceGDStateCCRInactiveSecurityWithClosedRelays DeviceGDState = "CCR_INACTIVE_SECURITY_WITH_CLOSED_RELAYS"
)

func (s DeviceGDState) IsKnown() bool {
	switch s {
	case DeviceGDStateExcluded, DeviceGDStateActive, DeviceGDStateUnknown,
		DeviceGDStateOptOutThroughLocation, DeviceGDStateOptOutThroughMobileApp,
		DeviceGDStateOptOutThroughPhysicalDevice,
		DeviceGDStateCCRActiveSecurityWithClosedRelays,
		DeviceGDStateCCRInactiveSecurityWithOpenedRelays,
		DeviceGDStateCCRInactiveSecurityWithClosedRelays:
		return true
	}
	return false
}

// ThermostatMode is the mode of a thermostat.
type ThermostatMode string

const (
	ThermostatModeUnknown       ThermostatMode = "UNKNOWN"
	ThermostatModeHeat          ThermostatMode = "HEAT"
	ThermostatModeAuto          ThermostatMode = "AUTO"
	ThermostatModeAutoHeat      ThermostatMode = "AUTO_HEAT"
	ThermostatModeEmergencyHeat ThermostatMode = "EMERGENCY_HEAT"
	ThermostatModeCool          ThermostatMode = "COOL"
	ThermostatModeAutoCool      ThermostatMode = "AUTO_COOL"
	ThermostatModeSouthernAway  ThermostatMode = "SOUTHERN_AWAY"
	ThermostatModeOff           ThermostatMode = "OFF"
	ThermostatModeManual        ThermostatMode = "MANUAL"
)

func (m ThermostatMode) IsKnown() bool {
	switch m {
	case ThermostatModeUnknown, ThermostatModeHeat, ThermostatModeAuto,
		ThermostatModeAutoHeat, ThermostatModeEmergencyHeat, ThermostatModeCool,
		ThermostatModeAutoCool, ThermostatModeSouthernAway, ThermostatModeOff,
		ThermostatModeManual:
		return true
	}
	return false
}

// ThermostatKind discriminates the physical thermostat type.
type ThermostatKind string

const (
	ThermostatKindGeneric    ThermostatKind = "GENERIC"
	ThermostatKindFloor      ThermostatKind = "FLOOR"
	ThermostatKindLowVoltage ThermostatKind = "LOW_VOLTAGE"
)

func (k ThermostatKind) IsKnown() bool {
	switch k {
	case ThermostatKindGeneric, ThermostatKindFloor, ThermostatKindLowVoltage:
		return true
	}
	return false
}

// FloorThermostatMode covers the AMBIENT/FLOOR/HYBRID modes of a floor thermostat.
type FloorThermostatMode string

const (
	FloorThermostatModeAmbient FloorThermostatMode = "AMBIENT"
	FloorThermostatModeFloor   FloorThermostatMode = "FLOOR"
	FloorThermostatModeHybrid  FloorThermostatMode = "HYBRID"
)

func (m FloorThermostatMode) IsKnown() bool {
	switch m {
	case FloorThermostatModeAmbient, FloorThermostatModeFloor, FloorThermostatModeHybrid:
		return true
	}
	return false
}

// LowVoltageCurrentState is what a 24V HVAC system is doing right now.
type LowVoltageCurrentState string

const (
	LowVoltageCurrentStateUnknown    LowVoltageCurrentState = "UNKNOWN"
	LowVoltageCurrentStateHeating    LowVoltageCurrentState = "HEATING"
	LowVoltageCurrentStateCooling    LowVoltageCurrentState = "COOLING"
	LowVoltageCurrentStateAuxHeating LowVoltageCurrentState = "AUX_HEATING"
	LowVoltageCurrentStateOff        LowVoltageCurrentState = "OFF"
)

func (s LowVoltageCurrentState) IsKnown() bool {
	switch s {
	case LowVoltageCurrentStateUnknown, LowVoltageCurrentStateHeating,
		LowVoltageCurrentStateCooling, LowVoltageCurrentStateAuxHeating,
		LowVoltageCurrentStateOff:
		return true
	}
	return false
}

// LowVoltageFanMode is the configured fan mode of a 24V HVAC system.
type LowVoltageFanMode string

const (
	LowVoltageFanModeUnknown        LowVoltageFanMode = "UNKNOWN"
	LowVoltageFanModeOn             LowVoltageFanMode = "ON"
	LowVoltageFanModeOff            LowVoltageFanMode = "OFF"
	LowVoltageFanModeAuto           LowVoltageFanMode = "AUTO"
	LowVoltageFanModeCirculate      LowVoltageFanMode = "CIRCULATE"
	LowVoltageFanModeFollowSchedule LowVoltageFanMode = "FOLLOW_SCHEDULE"
)

func (m LowVoltageFanMode) IsKnown() bool {
	switch m {
	case LowVoltageFanModeUnknown, LowVoltageFanModeOn, LowVoltageFanModeOff,
		LowVoltageFanModeAuto, LowVoltageFanModeCirculate, LowVoltageFanModeFollowSchedule:
		return true
	}
	return false
}

// LowVoltageFanCurrentState is what the fan is doing right now.
type LowVoltageFanCurrentState string

const (
	LowVoltageFanCurrentStateUnknown LowVoltageFanCurrentState = "UNKNOWN"
	LowVoltageFanCurrentStateOn      LowVoltageFanCurrentState = "ON"
	LowVoltageFanCurrentStateOff     LowVoltageFanCurrentState = "OFF"
)

func (s LowVoltageFanCurrentState) IsKnown() bool {
	switch s {
	case LowVoltageFanCurrentStateUnknown, LowVoltageFanCurrentStateOn, LowVoltageFanCurrentStateOff:
		return true
	}
	return false
}

// TemperatureKind enumerates the temperature units the API can return.
type TemperatureKind string

const (
	TemperatureKindDegreeCelsius      TemperatureKind = "DEGREE_CELSIUS"
	TemperatureKindDegreeFahrenheit   TemperatureKind = "DEGREE_FAHRENHEIT"
	TemperatureKindKelvin             TemperatureKind = "KELVIN"
	TemperatureKindMillidegreeCelsius TemperatureKind = "MILLIDEGREE_CELSIUS"
	TemperatureKindDegreeDelisle      TemperatureKind = "DEGREE_DELISLE"
	TemperatureKindDegreeNewton       TemperatureKind = "DEGREE_NEWTON"
	TemperatureKindDegreeRankine      TemperatureKind = "DEGREE_RANKINE"
	TemperatureKindDegreeReaumur      TemperatureKind = "DEGREE_REAUMUR"
	TemperatureKindDegreeRoemer       TemperatureKind = "DEGREE_ROEMER"
	TemperatureKindSolarTemperature   TemperatureKind = "SOLAR_TEMPERATURE"
)

func (k TemperatureKind) IsKnown() bool {
	switch k {
	case TemperatureKindDegreeCelsius, TemperatureKindDegreeFahrenheit, TemperatureKindKelvin,
		TemperatureKindMillidegreeCelsius, TemperatureKindDegreeDelisle, TemperatureKindDegreeNewton,
		TemperatureKindDegreeRankine, TemperatureKindDegreeReaumur, TemperatureKindDegreeRoemer,
		TemperatureKindSolarTemperature:
		return true
	}
	return false
}

// PowerKind enumerates the power units the API can return.
// The schema lists ~25 values; we name the ones the Hilo API actually uses
// and leave the rest as round-trippable strings.
type PowerKind string

const (
	PowerKindWatt                      PowerKind = "WATT"
	PowerKindKilowatt                  PowerKind = "KILOWATT"
	PowerKindMegawatt                  PowerKind = "MEGAWATT"
	PowerKindGigawatt                  PowerKind = "GIGAWATT"
	PowerKindMilliwatt                 PowerKind = "MILLIWATT"
	PowerKindMicrowatt                 PowerKind = "MICROWATT"
	PowerKindNanowatt                  PowerKind = "NANOWATT"
	PowerKindDecawatt                  PowerKind = "DECAWATT"
	PowerKindDeciwatt                  PowerKind = "DECIWATT"
	PowerKindFemtowatt                 PowerKind = "FEMTOWATT"
	PowerKindBoilerHorsepower          PowerKind = "BOILER_HORSEPOWER"
	PowerKindElectricalHorsepower      PowerKind = "ELECTRICAL_HORSEPOWER"
	PowerKindHydraulicHorsepower       PowerKind = "HYDRAULIC_HORSEPOWER"
	PowerKindBritishThermalUnitPerHour PowerKind = "BRITISH_THERMAL_UNIT_PER_HOUR"
	PowerKindGigajoulePerHour          PowerKind = "GIGAJOULE_PER_HOUR"
	PowerKindJoulePerHour              PowerKind = "JOULE_PER_HOUR"
)

func (k PowerKind) IsKnown() bool {
	switch k {
	case PowerKindWatt, PowerKindKilowatt, PowerKindMegawatt, PowerKindGigawatt,
		PowerKindMilliwatt, PowerKindMicrowatt, PowerKindNanowatt,
		PowerKindDecawatt, PowerKindDeciwatt, PowerKindFemtowatt,
		PowerKindBoilerHorsepower, PowerKindElectricalHorsepower, PowerKindHydraulicHorsepower,
		PowerKindBritishThermalUnitPerHour, PowerKindGigajoulePerHour, PowerKindJoulePerHour:
		return true
	}
	return false
}

// LightDeviceKind discriminates the physical light type.
type LightDeviceKind string

const LightDeviceKindGeneric LightDeviceKind = "GENERIC"

func (k LightDeviceKind) IsKnown() bool { return k == LightDeviceKindGeneric }

// LightType is whether a light is white-only or color-capable.
type LightType string

const (
	LightTypeWhite LightType = "WHITE"
	LightTypeColor LightType = "COLOR"
)

func (t LightType) IsKnown() bool { return t == LightTypeWhite || t == LightTypeColor }

// LightColorMode is the active color rendering mode.
type LightColorMode string

const (
	LightColorModeWhite LightColorMode = "WHITE"
	LightColorModeColor LightColorMode = "COLOR"
)

func (m LightColorMode) IsKnown() bool { return m == LightColorModeWhite || m == LightColorModeColor }

// SwitchKind / DimmerKind / SmartMeterKind / ChargingPointKind currently
// have one value each but are kept as enums for forward compatibility.
type SwitchKind string

const SwitchKindGeneric SwitchKind = "GENERIC"

func (k SwitchKind) IsKnown() bool { return k == SwitchKindGeneric }

type DimmerKind string

const DimmerKindGeneric DimmerKind = "GENERIC"

func (k DimmerKind) IsKnown() bool { return k == DimmerKindGeneric }

type SmartMeterKind string

const SmartMeterKindGeneric SmartMeterKind = "GENERIC"

func (k SmartMeterKind) IsKnown() bool { return k == SmartMeterKindGeneric }

type ChargingPointKind string

const ChargingPointKindGeneric ChargingPointKind = "GENERIC"

func (k ChargingPointKind) IsKnown() bool { return k == ChargingPointKindGeneric }

// ChargingPointStatus is the state machine of an EV charger.
type ChargingPointStatus string

const (
	ChargingPointStatusOutOfService ChargingPointStatus = "OUT_OF_SERVICE"
	ChargingPointStatusAvailable    ChargingPointStatus = "AVAILABLE"
	ChargingPointStatusInUse        ChargingPointStatus = "IN_USE"
	ChargingPointStatusReserved     ChargingPointStatus = "RESERVED"
)

func (s ChargingPointStatus) IsKnown() bool {
	switch s {
	case ChargingPointStatusOutOfService, ChargingPointStatusAvailable,
		ChargingPointStatusInUse, ChargingPointStatusReserved:
		return true
	}
	return false
}

// CCR (Charge Controller for Rate) — water heater controller modes/kinds.
type CCRMode string

const (
	CCRModeUnknown    CCRMode = "UNKNOWN"
	CCRModeOff        CCRMode = "OFF"
	CCRModeAuto       CCRMode = "AUTO"
	CCRModeAutoBypass CCRMode = "AUTO_BYPASS"
	CCRModeManual     CCRMode = "MANUAL"
)

func (m CCRMode) IsKnown() bool {
	switch m {
	case CCRModeUnknown, CCRModeOff, CCRModeAuto, CCRModeAutoBypass, CCRModeManual:
		return true
	}
	return false
}

type CCRKind string

const (
	CCRKindGeneric     CCRKind = "GENERIC"
	CCRKindWaterHeater CCRKind = "WATER_HEATER"
)

func (k CCRKind) IsKnown() bool { return k == CCRKindGeneric || k == CCRKindWaterHeater }
