# Concepts and gotchas

A short glossary of things that aren't obvious from reading the code, plus the wire-level traps that will burn time if you don't know about them.

## Identifiers

| Term | Form | Used by |
|---|---|---|
| `locationId` | int (e.g. `12345`) | REST `/Automation/...`, DeviceHub, NotificationHub |
| `HiloID` (location) | URN string `urn:hilo:crm:<addressId>:0` | GraphQL twin, ChallengeHub |
| Device `id` | int | REST device endpoints, DeviceHub, write APIs |
| Device `HiloID` | URN string `urn:hilo:philo:<mac>:0` | GraphQL twin |
| Device `Identifier` / `PhysicalAddress` | hex Zigbee MAC, e.g. `XXXXXXFFFEXXXXXX` | Cross-source join key |

`Identifier` (DeviceHub) and `PhysicalAddress` (twin) are the same value, sometimes in different case. Always uppercase before comparing.

`ListLocations` returns both `int ID` and `HiloID URN` on each `Location`. Cache the mapping at the start of any program — you'll need both.

## Twin vs DeviceHub vs hubs at large

- **Twin (GraphQL `getLocation`)** is a pull-mode snapshot of all devices for a location. Typed Go structs. Use for: "what is the current state of everything?" and for any reading that needs full attribute fidelity (humidity, heat demand, gd state, allowed modes, model/version).

- **DeviceHub (SignalR)** is push-mode. Two streams:
  - **List** — initial snapshot + deltas + add/delete; carries `HubDevice` (id/name/groupId/type/identifier/supported & settable attrs).
  - **Values** — `DevicesValuesReceived` pushes when an attribute changes; carries `(deviceId, attributeType, value, timestamp, operationId)`.

  DeviceHub is the live channel. Use for: monitoring, reactive controls, write completion observation.

- **NotificationHub (SignalR)** — push for new notifications + mark-read/viewed RPCs.

- **ChallengeHub (SignalR)** — push for Hilo Challenge event lifecycle + per-event consumption summaries. Uses `HiloID`, not int.

- **Digital-Twin GraphQL subscription (`onAnyDeviceUpdated`)** — finer-grained twin-shaped deltas. The SDK uses it internally to confirm write completions; you can also open it directly when you want twin-shaped updates rather than DeviceHub-shaped.

## Rooms = Groups

There is no "room" concept in the wire format — only "groups". The Android app surfaces them as rooms, and they have a `type` enum (`Bedroom`, `LivingRoom`, `Kitchen`, `Washroom`, `DiningRoom`, `Other`, etc.) plus a free-form `name`. Devices reference their room via the `groupId` field on `HubDevice` and on the REST representation under `/Devices`.

## URL constant footgun

The bundle's API base names (preserved in the SDK) don't match the conceptual category:

- Scenes live under `/program/v1/api/...`, not `/scenes/...`.
- Energy lives under `/Automation/...` (`Coach` service), not `/energy/...`.
- Notification paths are sometimes doubled (`/notification/notifications/...`).

When extending the SDK with a new endpoint: don't reason from the conceptual category — find the actual path in the existing code or in a captured fixture. The project memory captures this explicitly: "Treat the APK bundle as a hint, not ground truth."

## Write completion semantics

A `Set*` call goes through three stages:

1. **PUT to `/Automation/.../Devices/{id}/Attributes`** — server allocates an `operationId`, returns `ReportInProgress` (`OperationStatusReport`).
2. **One of two completion channels fires:**
   - GraphQL `onAnyDeviceUpdated` subscription — preferred, finer-grained.
   - DeviceHub `DevicesValuesReceived` push — fallback.
3. **Terminal status** — `Success` / `Failed` / `Timeout`.

On some backends neither (2) fires. The PUT in (1) still applied. The SDK surfaces this as `*Operation, ErrOperationStatusUnknown` so you can distinguish it from a transport failure. Treat it as success-with-uncertainty.

`*NoWait` skips (2) entirely and returns after (1).

## Token store and auth

- First call (often `whoami`) triggers Azure B2C form-scrape login using `HILO_EMAIL` / `HILO_PASSWORD`.
- Tokens persist at `~/.config/hilo/tokens.json` (override with `HILO_TOKEN_STORE`).
- Refresh is automatic via `do.go` middleware on 401.
- `Logout()` / `./bin/hilo logout` clears the store.
- Client ID and APIM subscription key are public values shipped with the Android app; they're not secrets.

## SDK scope

- Read-only REST + typed GraphQL twin + CLI surface.
- SignalR subscriptions (DeviceHub, NotificationHub, ChallengeHub, twin GraphQL subs).
- Device writes with operationId tracking + scene CRUD + opt-out.
- GraphQL operation-status bridge + NoWait write variants.
- `make lint` clean (gofmt + vet + staticcheck); goroutine-safety contracts on every long-lived type.

Don't propose endpoints that aren't already in the SDK without first running `grep -n "func (c \*Client)" *.go` to confirm. The escape hatches `Get` / `Post` / `Put` / `Patch` / `Delete` on the client (and `hilo get` / `hilo gql` on the CLI) cover anything unmodeled.

## Stdlib-only

One direct dep: `nhooyr.io/websocket`. One transitive: `github.com/klauspost/compress`. That's it. The user has called this out as deliberate — adding a third-party dep is a design conversation, not a routine refactor.

## Linting and tests

- `make lint` = `gofmt -l` + `go vet` + `staticcheck`. All three must produce no output. Run after Go edits.
- `make test` runs unit tests with `-race`. Keep it green.
- `HILO_INTEGRATION=1 go test -tags=integration ./...` exercises the live read API.
- The single live-write test is double-gated behind `HILO_INTEGRATION_WRITE=1` plus location/thermostat env vars.

## Where the source of truth lives

- For the wire shape of an endpoint: existing fixtures under `testdata/` (captured from real responses).
- For "is this method right?": existing tests, then exercise it live with `HILO_INTEGRATION=1 go test -tags=integration -run <name> ./...`.
- For unfamiliar pushes from a SignalR hub: temporarily add a sink with no filter on the relevant `hubConn` and log raw method names — the bare SignalR layer in `signalr.go` exposes everything that arrives.
- For domain semantics that aren't in code or fixtures: empirically test against a live account. The SDK is verified that way.
