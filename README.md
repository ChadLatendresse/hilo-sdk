# hilo

Unofficial Go SDK and CLI for the [Hilo Energie](https://www.hiloenergie.com/)
(Hydro-Québec) smart-home API. Reverse-engineered from the public Android app
(`com.hiloenergie.hilo`); not affiliated with or endorsed by Hilo or
Hydro-Québec. The client ID and APIM subscription key are public values
shipped to every device.

## Quick start

```sh
git clone https://github.com/ChadLatendresse/hilo-sdk.git hilo
cd hilo
cp .env.example .env && $EDITOR .env       # add HILO_EMAIL and HILO_PASSWORD
go build -o ./bin/hilo ./cmd/hilo
./bin/hilo whoami                          # triggers form-scrape login on first run
./bin/hilo locations                       # list your locations
```

## CLI

All commands read `.env` from the current directory. Authenticated commands
trigger an Azure B2C form-scrape login on first use and persist tokens to
`~/.config/hilo/tokens.json` (override with `HILO_TOKEN_STORE`).

### Read

```sh
./bin/hilo minversion                          # unauth — server min-version probe
./bin/hilo whoami                              # triggers form-scrape login
./bin/hilo locations
./bin/hilo location <locationId>               # REST representation
./bin/hilo twin <locationHiloId>               # full GraphQL device tree (typed)
./bin/hilo history <locationId>
./bin/hilo events <locationId> 2025-2026
./bin/hilo notifications
./bin/hilo scenes <locationId>
./bin/hilo automations <locationId>
```

### Real-time

```sh
./bin/hilo watch <locationId>                  # live device telemetry stream (^C to stop)
```

### Write

These need `HILO_LOCATION` set (the integer location id, not the HiloID).

```sh
./bin/hilo set <devID> setpoint <celsius>
./bin/hilo set <devID> mode <auto|manual|off>
./bin/hilo set <devID> on|off
./bin/hilo set <devID> level <0..100>
./bin/hilo set <devID> waterheater <off|auto|manual|bypass>
./bin/hilo scene <locId> <sceneId>             # activate a scene
./bin/hilo optout <locId> <eventId> <devId>    # opt out one device from one event
./bin/hilo prefs <locId> <thermostat|other> <opt-in|opt-out>
./bin/hilo rename <devID> <new-name>           # rename a device
./bin/hilo favorite <devID>                    # toggle isFavorite
./bin/hilo scene-create <locId> <name>
./bin/hilo scene-update <locId> <sceneId> <new-name>
./bin/hilo scene-delete <locId> <sceneId>
```

### Escape hatches

```sh
./bin/hilo get <path>                          # raw GET against the REST base
./bin/hilo gql <query>                         # raw GraphQL against the digital-twin endpoint
./bin/hilo logout                              # wipe ~/.config/hilo/tokens.json
```

## Library use

```go
package main

import (
    "context"
    "fmt"

    "github.com/ChadLatendresse/hilo-sdk"
)

func main() {
    _ = hilo.LoadDotEnv(".env")
    c := hilo.NewClient()
    cont, err := c.GetLocation(context.Background(), hilo.HiloID("urn:hilo:crm:..."))
    if err != nil { panic(err) }
    for _, d := range cont.Devices {
        switch d := d.(type) {
        case *hilo.BasicThermostat:
            fmt.Printf("thermostat %s: %.1f°C", d.HiloID, d.AmbientTemperature.Celsius())
            if d.AmbientTempSetpoint != nil {
                fmt.Printf(" (setpoint %.1f°C)", d.AmbientTempSetpoint.Celsius())
            }
            fmt.Println()
        case *hilo.BasicSmartMeter:
            if d.Power != nil {
                fmt.Printf("smart meter: %.2f W\n", d.Power.Watts())
            }
        }
    }
}
```

Two ID conventions to be aware of: REST and GraphQL use string `HiloID`
values (URN-shaped, e.g. `urn:hilo:crm:...`); SignalR hubs and write APIs
use integer location and device ids. Look both up via `hilo locations`.

### Real-time telemetry

```go
package main

import (
    "context"
    "fmt"
    "os/signal"
    "syscall"

    "github.com/ChadLatendresse/hilo-sdk"
)

func main() {
    _ = hilo.LoadDotEnv(".env")
    c := hilo.NewClient()

    ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
    defer stop()

    // DeviceHub uses the integer location ID, not the HiloID.
    list, err := c.SubscribeDeviceList(ctx, 12345)
    if err != nil { panic(err) }

    go func() {
        for s := range list.State() { fmt.Println("transport:", s) }
    }()

    for upd := range list.Updates() {
        switch upd.Kind {
        case hilo.DeviceListSnapshot:
            fmt.Printf("snapshot: %d devices\n", len(upd.Devices))
        case hilo.DeviceListAdded:
            fmt.Printf("added: %s\n", upd.Devices[0].Name)
        }
    }
    if err := list.Err(); err != nil { fmt.Println("ended:", err) }
}
```

### Device control

```go
package main

import (
    "context"
    "fmt"
    "time"

    "github.com/ChadLatendresse/hilo-sdk"
)

func main() {
    _ = hilo.LoadDotEnv(".env")
    c := hilo.NewClient()

    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()

    op, err := c.SetThermostatSetpoint(ctx, 12345, 67890, hilo.NewTemperature(21.0))
    if err != nil { panic(err) }
    if op.Status != hilo.OperationStatusSucceeded {
        fmt.Printf("setpoint write %s: %s\n", op.Status, op.StatusReason)
        return
    }
    fmt.Println("setpoint set: opID=", op.OperationID)

    if err := c.ActivateScene(ctx, 12345, 4321); err != nil { panic(err) }
    fmt.Println("scene activated")
}
```

The call blocks until either:

- The GraphQL `onAnyDeviceUpdated` subscription delivers a real status
  (preferred path), OR
- The DeviceHub `DevicesValuesReceived` echo arrives carrying the matching
  `operationId` (fallback), OR
- `ctx` expires.

### Operation status caveat

Empirical testing against the live API surfaced that **neither completion
path is reliable on every backend.** Hilo's `/api/digital-twin/v3/graphql`
endpoint may return `application/json` instead of `multipart/mixed` for
subscription requests (no multipart support), and the `DevicesValuesReceived`
push doesn't fire for write operations within a 90-second window in our
testing.

When neither path can confirm completion before `ctx` expires, `Set*`
returns:

- a non-nil `*Operation` with `OperationID` set and `Status =
  OperationStatusReport` (the bundle's "in-flight" status), and
- a wrapped `ErrOperationStatusUnknown` error.

Use `errors.Is` to handle this gracefully:

```go
op, err := c.SetThermostatSetpoint(ctx, 12345, 67890, hilo.NewTemperature(21.0))
switch {
case err == nil:
    // Status was confirmed.
case errors.Is(err, hilo.ErrOperationStatusUnknown):
    // PUT applied; status couldn't be confirmed within ctx deadline.
    // op.OperationID is set.
default:
    // Genuine REST or transport error; PUT did NOT apply.
}
```

If you don't need confirmation at all, use the `*NoWait` variants which
return `(opID string, err error)` immediately after the PUT:

```go
opID, err := c.SetAttributeNoWait(ctx, 12345, 67890, hilo.AttrTargetTemperature, 21.0)
```

`SetAttributesNoWait` and `SetBatchAttributesNoWait` are the multi-attr
and multi-device equivalents.

## Environment variables

Loaded from `.env` in the current directory by `LoadDotEnv` (or `hilo.NewClient()`
implicitly via the CLI). Only `HILO_EMAIL` and `HILO_PASSWORD` are required;
everything else has a sensible default baked in.

### Required

| Variable        | Purpose                                                    |
| --------------- | ---------------------------------------------------------- |
| `HILO_EMAIL`    | Account email (Azure B2C login)                            |
| `HILO_PASSWORD` | Account password                                           |

### CLI write commands

| Variable        | Purpose                                                    |
| --------------- | ---------------------------------------------------------- |
| `HILO_LOCATION` | Integer location id used by `hilo set`, `hilo scene`, `hilo optout`, `hilo prefs`, `hilo rename`, `hilo favorite`. Look it up via `hilo locations`. |

### Storage

| Variable           | Default                              | Purpose                                |
| ------------------ | ------------------------------------ | -------------------------------------- |
| `HILO_TOKEN_STORE` | `~/.config/hilo/tokens.json`         | On-disk OAuth token cache             |

### Endpoint and OAuth overrides (rarely needed)

Defaults are extracted from the Android bundle; override only if Hilo rotates
endpoints or you're testing against a staging tenant.

| Variable                | Default                                              |
| ----------------------- | ---------------------------------------------------- |
| `HILO_API_BASE`         | REST API base URL                                    |
| `HILO_PLATFORM_BASE`    | Platform (digital-twin / SignalR) base URL           |
| `HILO_DISCOVERY_URL`    | OAuth OpenID discovery URL                           |
| `HILO_AUTHORIZE_URL`    | OAuth authorize endpoint                             |
| `HILO_TOKEN_URL`        | OAuth token endpoint                                 |
| `HILO_REDIRECT_URI`     | OAuth redirect URI                                   |
| `HILO_CLIENT_ID`        | Public Azure B2C client id                           |
| `HILO_SCOPE`            | OAuth scope                                          |
| `HILO_B2C_POLICY`       | Azure B2C policy name (signin/signup flow)           |
| `HILO_SUBSCRIPTION_KEY` | Public APIM subscription key                         |

### Integration tests (off by default)

| Variable                     | Purpose                                                 |
| ---------------------------- | ------------------------------------------------------- |
| `HILO_INTEGRATION=1`         | Enable read-only tests against the live account         |
| `HILO_INTEGRATION_WRITE=1`   | Enable write tests (PUT/PATCH/POST against live account)|
| `HILO_TEST_LOCATION_ID`      | Integer location id used by integration tests           |
| `HILO_TEST_THERMOSTAT_ID`    | Integer device id used by write tests                   |

## Tests

```sh
go test ./...                                       # unit tests against fixtures
HILO_INTEGRATION=1 go test -tags=integration ./...  # exercises the live account (read-only)
```

Write tests additionally require `HILO_INTEGRATION_WRITE=1`,
`HILO_TEST_LOCATION_ID`, and `HILO_TEST_THERMOSTAT_ID`. They mutate a real
account — use a thermostat you control.

## Dependencies

The SDK is stdlib-only except for the WebSocket transport, which adds
one direct dependency with no transitives:

- `github.com/coder/websocket`

Run `go mod graph` for the full closure.
