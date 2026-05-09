# Go library reference

Module path: `hilo` (single package). Construct a client with `NewClient()` after `LoadDotEnv(".env")`. Every method takes `context.Context`. The client is safe for concurrent use.

```go
_ = hilo.LoadDotEnv(".env")
c := hilo.NewClient()
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()
```

## Layering — mental model

- **`client.go`** — HTTP transport, auth wiring, generic `Get/Post/Put/Patch/Delete`.
- **`auth.go`, `pkce.go`, `do.go`** — Azure B2C form-scrape login, token store, retry/refresh.
- **Domain files** (one per backing service): `automation.go`, `coach.go`, `challenge.go`, `clientele.go`, `notifications.go`, `scenes.go`, `event_optout.go`, `device_management.go`, `status.go`.
- **Twin** (`twin.go`) — typed GraphQL digital-twin (snapshot read).
- **SignalR hubs**: `device_hub.go`, `notification_hub.go`, `challenge_hub.go`, `digital_twin_subscription.go`. Underlying transport: `signalr.go`.
- **Writes**: `device_control.go` (attribute writes with completion tracking), `scenes.go` (`ActivateScene`), `event_optout.go`, `device_management.go` (rename/favorite).

When adding a new endpoint, keep domain files thin and parallel — REST helpers tend to be 5-15 lines each.

## Authentication & client

```go
hilo.LoadDotEnv(path string) error           // optional; reads HILO_* into os.Setenv
hilo.NewClient() *Client                      // reads HILO_EMAIL / HILO_PASSWORD from env

(c *Client).Logout() error                    // wipes ~/.config/hilo/tokens.json
```

## Read APIs (REST + GraphQL)

```go
// Locations & metadata (REST, /Automation/...)
ListLocations(ctx) ([]Location, error)                            // both int ID and HiloID URN
GetLocationREST(ctx, idStr string) (*Location, error)
LocationFeatureFlags(ctx, idStr) (*LocationFeatureFlags, error)
LocationWeather(ctx, idStr) (json.RawMessage, error)
LocationSunTime(ctx, idStr) (json.RawMessage, error)
Gateways(ctx, idStr) (json.RawMessage, error)
LocationPreferences(ctx, idStr) (json.RawMessage, error)
LocationResidence(ctx, idStr) (json.RawMessage, error)            // backed by Clientele
LocationNotifications(ctx, idStr) (json.RawMessage, error)

// Devices, scenes, automations (REST)
GetDevice(ctx, locIDStr, devIDStr) (json.RawMessage, error)
GetDeviceAttributes(ctx, locIDStr, devIDStr) (json.RawMessage, error)
Groups(ctx, locIDStr) (json.RawMessage, error)                    // rooms
Scenes(ctx, locIDStr) (json.RawMessage, error)
GetScene(ctx, locIDStr, sceneIDStr) (json.RawMessage, error)
Automations(ctx, locIDStr) (json.RawMessage, error)
UpcomingAutomations(ctx, locIDStr) (json.RawMessage, error)
GetAutomation(ctx, locIDStr, autoIDStr) (json.RawMessage, error)

// Coach / energy
EnergyHistory(ctx, locIDStr, timescale string, date time.Time) (json.RawMessage, error)
LocationComparison(ctx, locIDStr) (json.RawMessage, error)
LocationOverconsumption(ctx, locIDStr, periodID string) (json.RawMessage, error)
SeasonsSummary(ctx, locIDStr) (json.RawMessage, error)
FlexSavings(ctx, locIDStr, season int) (json.RawMessage, error)
RateEvents(ctx, locIDStr, rate string, season int) (json.RawMessage, error)

// Hilo Challenge events (peak hours)
HiloEvents(ctx, locIDStr, season int) (json.RawMessage, error)
HiloEvent(ctx, locIDStr, eventID string) (json.RawMessage, error)
EventOptOutDetails(ctx, locIDStr, eventID, devIDStr) (json.RawMessage, error)
GetDeviceOptoutDetails(ctx, locID int, devID int, eventID string) (*DeviceOptoutDetails, error)
FlexReductionStatusLimits(ctx) (json.RawMessage, error)

// Account
Account(ctx) (*AccountInfo, error)
AccountPayments(ctx) (json.RawMessage, error)

// Status
MinVersion(ctx) (string, error)
NotificationAlert(ctx) (json.RawMessage, error)
```

### GraphQL twin (typed)

```go
GetLocation(ctx, locHiloID HiloID) (*Container, error)
```

`Container.Devices` is `[]Device` — a typed union dispatched by GraphQL `__typename`. Use a type switch:

| Concrete type | When |
|---|---|
| `*Gateway` | Hilo gateway hub |
| `*BasicSmartMeter` | Hydro smart meter (`Power Watts()`) |
| `*BasicThermostat` | Standard thermostat (`AmbientTemperature.Celsius()`, `AmbientTempSetpoint`, `Mode`, `HeatDemand`) |
| `*LowVoltageThermostat` | Low-voltage / HVAC stat |
| `*HeatingFloorThermostat` | Floor heating |
| `*WaterHeater` | CCR-controlled water heater |
| `*BasicChargeController` | Standalone CCR |
| `*BasicLight`, `*BasicDimmer`, `*BasicSwitch` | Lighting |
| `*BasicEVCharger` | EV charger |
| `*BasicDevice` | Catch-all for unknown `__typename` (forward compat) |

Every concrete type embeds `IBasicDevice` (HiloID, GatewayHiloID, PhysicalAddress, ConnectionStatus, last-update timestamps). Access via `dev.GetCommon()`.

### Temperatures & power

```go
type Temperature struct { Value float64; Kind TemperatureKind }
(t Temperature) Celsius() float64
hilo.NewTemperature(celsius float64) Temperature   // for setpoint writes

type Power struct { Value float64; Kind PowerKind }
(p Power) Watts() float64
```

Values come back in whatever unit the backend chose; always normalize via `.Celsius()` / `.Watts()`.

## Live streams (SignalR over WebSocket)

All subscription methods return a `*Stream[T]` and an error. The stream closes when `ctx` is cancelled. Always `defer cancel()` and drain `Updates()`.

```go
type Stream[T any] // not generic in Go yet, see actual file — but conceptually:
(s *Stream[T]) Updates() <-chan T          // application updates
(s *Stream[T]) State() <-chan StreamState  // transport state changes
(s *Stream[T]) Err() error                 // terminal error after channels close
```

Available subscriptions:

```go
// DeviceHub — uses int locID
SubscribeDeviceList(ctx, locID int) (*Stream[DeviceListUpdate], error)
//   Update.Kind ∈ {Snapshot, Delta, Added, Deleted}
//   Update.Devices []HubDevice  (ID, HiloID, Name, GroupID, Type, Identifier, ...)
SubscribeDeviceValues(ctx, locID int) (*Stream[DeviceValuesUpdate], error)
//   Update.Values []DeviceAttrValue {DeviceID, AttributeType, Value json.RawMessage, OperationID, Timestamp}

// NotificationHub
SubscribeNotifications(ctx) (*Stream[NotificationEvent], error)
MarkNotificationRead(ctx, id string) error
MarkAllNotificationsRead(ctx) error
MarkNotificationViewed(ctx, id string) error
MarkAllNotificationsViewed(ctx) error

// ChallengeHub — uses HiloID
SubscribeEventList(ctx, locHiloID HiloID) (*Stream[EventListUpdate], error)
SubscribeEventCHDetails(ctx, locHiloID, eventID) (*Stream[EventCHDetailsUpdate], error)
RequestEventCHConsumption(ctx, locHiloID, eventID) error
SubscribeEventFlexDetails(ctx, locHiloID, eventID) (*Stream[EventFlexDetailsUpdate], error)
RequestEventFlexConsumption(ctx, locHiloID, eventID) error

// GraphQL onAnyDeviceUpdated subscription (used internally for write completion;
// can also be opened directly for finer-grained twin deltas)
PrimeLocationHiloID(locID int, hiloID HiloID)        // avoids a lookup roundtrip
RefreshLocationHiloIDCache(ctx) error
```

`HubDevice.Identifier` (uppercased Zigbee MAC) matches twin device `PhysicalAddress` — use this to join hub data with twin readings.

## Writes (device control)

All writes are operationId-keyed and tracked through one of two completion channels (GraphQL `onAnyDeviceUpdated` or DeviceHub `DevicesValuesReceived`). `Set*` blocks until terminal status; `*NoWait` returns after PUT acknowledgement.

```go
// High-level helpers (preferred when applicable)
SetThermostatSetpoint(ctx, locID, devID int, target Temperature) (*Operation, error)
SetThermostatMode(ctx, locID, devID int, mode ThermostatMode) (*Operation, error)
SetSwitchState(ctx, locID, devID int, on bool) (*Operation, error)
SetLightState(ctx, locID, devID int, on bool) (*Operation, error)
SetLightLevel(ctx, locID, devID int, level int) (*Operation, error)        // 0..100
SetLightColor(ctx, locID, devID int, hue, saturation int) (*Operation, error)
SetWaterHeaterMode(ctx, locID, devID int, mode CCRMode) (*Operation, error)

// Generic — when a helper doesn't fit
SetAttribute(ctx, locID, devID int, attr AttributeType, value any) (*Operation, error)
SetAttributes(ctx, locID, devID int, attrs map[AttributeType]any) ([]Operation, error)
SetBatchAttributes(ctx, locID int, writes []AttributeWrite) ([]Operation, error)

// Fire-and-forget
SetAttributeNoWait(ctx, locID, devID int, attr AttributeType, value any) (string, error)
SetAttributesNoWait(ctx, locID, devID int, attrs map[AttributeType]any) ([]string, error)
SetBatchAttributesNoWait(ctx, locID int, writes []AttributeWrite) ([]string, error)
```

`AttributeType` constants live in `device_control.go`: `AttrTargetTemperature`, `AttrThermostatMode`, `AttrPower`, `AttrLevel`, `AttrHue`, `AttrSaturation`, `AttrColorTemperature`, `AttrCCRMode`, `AttrGdState`, etc.

### Completion semantics — read this before claiming a write succeeded

```go
op, err := c.SetThermostatSetpoint(ctx, locID, devID, hilo.NewTemperature(21.0))
switch {
case err == nil:
    // Status confirmed terminal. op.Status carries the result.
case errors.Is(err, hilo.ErrOperationStatusUnknown):
    // PUT was applied; completion not observed within ctx deadline.
    // op is non-nil with OperationID + Status=OperationStatusReport (in-flight).
    // Treat as success-with-uncertainty, NOT failure.
default:
    // Genuine error; PUT did not apply.
}
```

Background: on some backends the GraphQL subscription returns `application/json` instead of `multipart/mixed`, and the DeviceHub echo skips writes — neither completion path fires. The PUT itself still went through.

## Other write APIs

```go
// Device management
UpdateDevice(ctx, locID int, dev HubDevice) (*HubDevice, error)
ToggleDeviceFavorite(ctx, locID, devID int) error
SetDevicesFavorite(ctx, locID int, updates []FavoriteUpdate) error

// Scenes
CreateScene(ctx, locID int, scene Scene) (*Scene, error)
UpdateScene(ctx, locID int, scene Scene) (*Scene, error)
DeleteScene(ctx, locID, sceneID int) error
ActivateScene(ctx, locID, sceneID int) error                       // fire-and-forget

// Hilo Challenge opt-out
OptOutDevice(ctx, locID int, eventID string, devID int) error
SetLocationPreferences(ctx, locID int, prefs LocationPreferences) error
```

## Generic JSON shapes

Many endpoints return `json.RawMessage` because typed structs aren't worth maintaining for endpoints with rare or evolving payloads. When the user wants to peek at one of these, just `json.Unmarshal` into a local struct or print as pretty JSON.

## Testing

- `make test` runs unit tests (`-race`), no network.
- `HILO_INTEGRATION=1 go test -tags=integration ./...` exercises the live read API.
- Write integration test is double-gated: `HILO_INTEGRATION_WRITE=1` + `HILO_TEST_LOCATION_ID` + `HILO_TEST_THERMOSTAT_ID`.
- Unit tests use `testdata/*.json` fixtures captured from real responses.

## Style conventions

- Stdlib-only (one transitive websocket dep). Don't add deps lightly.
- Channel directions on internal helpers (`<-chan` / `chan<-`) are used deliberately.
- Goroutine-safety contracts are documented on every long-lived type — preserve them.
- `t.Error` for independent assertions; `t.Fatal` only when subsequent steps depend on the prior assertion.
- Match existing file shape when adding new endpoints; keep domain files thin.
