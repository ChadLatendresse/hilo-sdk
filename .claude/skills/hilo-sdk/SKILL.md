---
name: hilo-sdk
description: Use this skill whenever the user wants to interact with the Hilo Energie (Hydro-Québec) smart-home platform from this repo — listing locations/rooms, reading thermostat or smart-meter values, streaming live device telemetry, controlling thermostats / lights / water heater / scenes, opting devices out of Hilo Challenge events, or fetching energy / coach / notification data. Trigger on any mention of hilo, hilo energie, hilo-go, hydro-quebec smart home, thermostat setpoint in this repo, "watch devices", scene activation, peak event opt-out, or the `hilo` CLI. Even if the user just asks "what's the temperature in the bedroom" or "turn off the kitchen light" without naming the SDK, this is the right skill.
---

# Hilo Go SDK

Unofficial Go SDK + CLI for Hilo Energie (Hydro-Québec). Reverse-engineered, stdlib-only (one transitive websocket dep), token cache + auto-refresh, typed REST + GraphQL twin + SignalR streams + device control. The CLI at `./bin/hilo` exposes the full surface.

## Decide first: CLI or library?

- **CLI** (`./bin/hilo …`) — for ad-hoc reads, one-off writes, scripting, debugging. Authenticates from `.env` in cwd, caches tokens at `~/.config/hilo/tokens.json`. Output is JSON. **Default to this** unless the user is writing Go.
- **Library** (`import "hilo"`) — for embedded use, custom join logic, long-running streams. Same auth model.
- **Build once if missing:** `go build -o ./bin/hilo ./cmd/hilo`
- **Lint:** `make lint` must stay clean (`gofmt` + `go vet` + `staticcheck`). Run after Go edits.

When the user wants a one-shot answer that requires joining data from multiple endpoints (e.g. "rooms with temperatures"), prefer the CLI's escape hatches first (`hilo get <path>` + `jq`, or `hilo gql -`). Reach for a small Go program only when shell pipelines stop being a good fit (typed iteration over the twin device union, write loops, long-running streams).

## Five concepts that prevent 90% of mistakes

1. **Two location ID forms — they are not interchangeable.**
   - `int locationId` (e.g. `12345`) — used by REST (`/Automation/...`) and by DeviceHub / NotificationHub SignalR.
   - `HiloID` URN (e.g. `urn:hilo:crm:0000000-xxxxxx:0`) — used by the GraphQL digital-twin and ChallengeHub.
   - `Client.ListLocations(ctx)` returns both on each `Location`. Cache the mapping.

2. **Twin vs DeviceHub — different shapes for the same hardware.**
   - Twin (`GetLocation`, GraphQL) → typed `Container.Devices` of `BasicThermostat`, `BasicLight`, etc. Has `HiloID`, `PhysicalAddress`, current temperatures/setpoints/state. Snapshot only.
   - DeviceHub (`SubscribeDeviceList`) → flat `HubDevice` with integer `ID`, `Name`, `GroupID` (room), `Identifier` (== `PhysicalAddress` from twin, case-insensitive), `Type`, `SupportedAttributesList`. Live snapshot + deltas + add/delete.
   - **Match across the two by uppercasing `Identifier` ⇄ `PhysicalAddress`.**

3. **Rooms = Groups.** No "room" type in code. Use `GET /Automation/v1/api/Locations/{locID}/Groups` (room metadata) and `/Groups/{groupID}/Devices` (room→device list). `HubDevice.GroupID` from DeviceHub also carries it.

4. **Writes block until completion, with a known caveat.** `Set*` (e.g. `SetThermostatSetpoint`) blocks until the operation reaches a terminal status. On some backends neither completion channel fires — in that case the call returns a non-nil `*Operation` *and* a wrapped `ErrOperationStatusUnknown`. The PUT did succeed; only confirmation is missing. Always branch with `errors.Is(err, hilo.ErrOperationStatusUnknown)` — see `references/go-api.md`. For fire-and-forget, use the `*NoWait` variants.

5. **SDK scope.** Read-only REST/GraphQL, SignalR subscriptions, and device writes are all landed. Don't propose endpoints that aren't in the SDK without first checking with `grep -n "func (c \*Client)" *.go`. Use `hilo get <path>` / `hilo gql <q>` as escape hatches for unmodeled REST/GraphQL paths.

## Authentication setup

```sh
# .env in cwd:
HILO_EMAIL=you@example.com
HILO_PASSWORD=...
# Optional but useful for write CLI commands:
HILO_LOCATION=12345
```

Tokens persist at `~/.config/hilo/tokens.json` and refresh automatically. `./bin/hilo logout` wipes them. The client ID and APIM key are public values shipped with the Android app.

## Stdlib-only is deliberate

The only direct dep is `nhooyr.io/websocket` (for SignalR). Don't add others without strong justification — the user has called this out specifically.

## When to read the references

| Task | Read |
|------|------|
| Pick the right CLI command, flags, output shape | `references/cli.md` |
| Pick the right Go method, understand its return shape, find the right type | `references/go-api.md` |
| Joining data across endpoints (rooms+temps, watch+control, etc.) | `references/recipes.md` |
| Confused about a wire-level detail or domain term | `references/concepts.md` |

Read them lazily — don't dump all four into context. Read the one that matches the task.

## Verification before claiming done

This SDK is verified against a live account. After non-trivial Go edits, run `make lint` and `make test`. After CLI/recipe changes, exercise the actual command end-to-end and show its output. Don't claim "this should work" — show that it did.
