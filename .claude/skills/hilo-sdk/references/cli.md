# Hilo CLI reference

The `hilo` binary at `./bin/hilo` (built with `go build -o ./bin/hilo ./cmd/hilo`) is the user-facing surface for the SDK. It loads `.env` from the current working directory, caches tokens at `~/.config/hilo/tokens.json`, and prints JSON to stdout.

## Read commands

| Command | Args | Output | Notes |
|---|---|---|---|
| `whoami` | — | id_token claims | First call triggers form-scrape login if no cached token |
| `logout` | — | — | Wipes the token store |
| `minversion` | — | min app version | Unauthenticated |
| `notification-alert` | — | server alert payload | Unauthenticated |
| `locations` | — | `[]Location` | Use this first; both `id` (int) and `locationHiloId` (URN) are returned |
| `location` | `<locId>` | one `Location` | REST representation |
| `twin` | `<locHiloId>` | full GraphQL device tree | Pass the URN, not the int. Typed `Container` with `__typename` dispatched device union |
| `feature-flags` | `<locId>` | enabled feature flags | |
| `weather` | `<locId>` | current weather | |
| `history` | `<locId> [date YYYY-MM-DD] [Day]` | energy history | |
| `events` | `<locId> <season>` | Hilo Challenge events for a year (e.g. `2026`) | |
| `notifications` | `<locId>` | notification feed | |
| `scenes` | `<locId>` | scenes at a location | |
| `automations` | `<locId>` | automation rules | |
| `watch` | `<locId>` | live device telemetry stream (^C) | Prints snapshot, then per-attribute updates |
| `get` | `<path>` | raw GET on `api.hiloenergie.com` | Escape hatch for unmodeled REST endpoints |
| `gql` | `<query>` (or `-` for stdin) | raw GraphQL on platform digital-twin | Escape hatch for unmodeled GraphQL |

## Write commands

| Command | Args | Effect |
|---|---|---|
| `set <devID> setpoint <celsius>` | float | Thermostat target temp |
| `set <devID> mode <auto\|manual\|off>` | enum | Thermostat mode |
| `set <devID> on` / `off` | — | Switch / light on/off |
| `set <devID> level <0..100>` | int | Light/dimmer level |
| `set <devID> waterheater <off\|auto\|manual\|bypass>` | enum | Water-heater CCRMode |
| `scene <locId> <sceneId>` | — | Activate a scene |
| `scene-create <locId> <name>` | — | New scene |
| `scene-update <locId> <sceneId> <new-name>` | — | Rename |
| `scene-delete <locId> <sceneId>` | — | Delete |
| `optout <locId> <eventId> <devId>` | — | Opt one device out of one Hilo event |
| `prefs <locId> <thermostat\|other> <opt-in\|opt-out>` | — | Default opt-out preference |
| `rename <devID> <new-name>` | — | Rename device (uses `HILO_LOCATION` env) |
| `favorite <devID>` | — | Toggle `isFavorite` |

`set` and `rename` use `HILO_LOCATION` from env to avoid passing the location ID every call.

## Useful escape-hatch endpoints

These are not modeled by a top-level CLI command; reach them via `hilo get <path>`:

| Path | What |
|---|---|
| `/Automation/v1/api/Locations/{locId}/Groups` | Rooms (groups) at a location |
| `/Automation/v1/api/Locations/{locId}/Groups/{groupId}/Devices` | Devices in one room |
| `/Automation/v1/api/Locations/{locId}/Devices/{devId}` | Device REST detail |
| `/Automation/v1/api/Locations/{locId}/Devices/{devId}/Attributes` | Device attribute snapshot |

For arbitrary GraphQL queries, `hilo gql -` reads from stdin so multi-line queries work cleanly:

```sh
./bin/hilo gql - <<'EOF'
query($id: String!) {
  getLocation(id: $id) { hiloId lastUpdate devices { __typename ... on IBasicDevice { hiloId } } }
}
EOF
```

The CLI sends a default `variables` map; for parameterized queries you may need to embed values directly or use the Go API.

## Common patterns

**Find a thermostat's int device ID** (needed for `set`):
```sh
./bin/hilo get '/Automation/v1/api/Locations/<locId>/Groups' \
  | jq -r '.[] | "\(.id)\t\(.name)"'
./bin/hilo get '/Automation/v1/api/Locations/<locId>/Groups/<groupId>/Devices' \
  | jq -r '.[] | select(.type=="Thermostat") | "\(.id)\t\(.name)\t\(.identifier)"'
```

**Get current ambient temps from twin**:
```sh
./bin/hilo twin '<locHiloId>' \
  | jq '.devices[] | select(.__typename=="BasicThermostat") | {hiloId, t: .ambientTemperature.value}'
```

**Stream a few seconds of telemetry**:
```sh
timeout 10 ./bin/hilo watch <locId>
```
