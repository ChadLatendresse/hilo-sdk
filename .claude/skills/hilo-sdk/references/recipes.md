# Recipes — common task patterns

These are end-to-end patterns for tasks that require joining data from multiple endpoints. Most of them aren't expressible as one CLI call. Try the CLI's escape hatches (`hilo get <path>` piped to `jq`, or `hilo gql -`) first; reach for a small Go program when shell pipelines stop being a good fit (typed iteration over the twin device union, long-running streams, write loops).

Conventions used below:

- `LOC` is the integer location ID (`12345` in examples).
- `LOC_HID` is the corresponding `HiloID` URN.
- Always start from `ListLocations` and look both up — never hardcode.

## Recipe 1: list every room and its current temperature

The data needs three sources:

1. `GET /Automation/v1/api/Locations/{LOC}/Groups` — room metadata (id, name, type).
2. `GET /Automation/v1/api/Locations/{LOC}/Groups/{groupId}/Devices` — devices in each room (gives `identifier` = Zigbee MAC and `type`).
3. `GetLocation(LOC_HID)` — twin GraphQL with current `ambientTemperature` per device, keyed by `physicalAddress`.

Match `Identifier` (uppercased) ⇄ `PhysicalAddress` (uppercased) when joining the two sources.

### Sketch (Go)

```go
// 1. Rooms.
var groups []struct { ID int; Name, Type string }
_ = json.Unmarshal(mustGet(c, ctx, "/Automation/v1/api/Locations/"+locStr+"/Groups"), &groups)

// 2. Devices per room — gives identifier (== twin physicalAddress).
type roomDev struct{ ID int; Name, Identifier, Type string }
roomDevs := map[int][]roomDev{}
for _, g := range groups {
    var devs []roomDev
    _ = json.Unmarshal(mustGet(c, ctx, fmt.Sprintf("/Automation/v1/api/Locations/%d/Groups/%d/Devices", loc.ID, g.ID)), &devs)
    roomDevs[g.ID] = devs
}

// 3. Twin: ambient temp keyed by uppercased physicalAddress.
twin, _ := c.GetLocation(ctx, loc.LocationHiloID)
temps := map[string]float64{}
for _, d := range twin.Devices {
    if t, ok := d.(*hilo.BasicThermostat); ok {
        temps[strings.ToUpper(t.PhysicalAddress)] = t.AmbientTemperature.Celsius()
    }
}

// 4. Print: room → thermostat → temperature.
for _, g := range groups {
    fmt.Println(g.Name, "—", g.Type)
    for _, d := range roomDevs[g.ID] {
        if d.Type == "Thermostat" {
            fmt.Printf("  %-12s %.1f °C\n", d.Name, temps[strings.ToUpper(d.Identifier)])
        }
    }
}
```

`mustGet` is a one-line helper around `c.Get(ctx, path, &raw) -> raw`. Skip room mapping entirely if you only want temperatures:

```sh
./bin/hilo twin '<LOC_HID>' \
  | jq '[.devices[] | select(.__typename=="BasicThermostat") | {hiloId, t: .ambientTemperature.value, sp: .ambientTempSetpoint.value}]'
```

## Recipe 2: watch live telemetry, react to one device

```go
list, _ := c.SubscribeDeviceList(ctx, locID)
vals, _ := c.SubscribeDeviceValues(ctx, locID)

names := map[int]string{}
for upd := range list.Updates() {
    for _, d := range upd.Devices {
        names[d.ID] = d.Name
    }
    if upd.Kind == hilo.DeviceListSnapshot {
        break // got initial mapping; keep listening on `vals`
    }
}

for upd := range vals.Updates() {
    for _, v := range upd.Values {
        fmt.Printf("%s.%s = %s\n", names[v.DeviceID], v.AttributeType, string(v.Value))
    }
}
```

`DeviceAttrValue.Value` is `json.RawMessage` because the wire shape varies by attribute. Decode to the right Go type per attribute (numeric for `TargetTemperature`/`Power`, string for `ThermostatMode`, etc.).

## Recipe 3: set every thermostat to one temperature, batched

Use `SetBatchAttributes` to issue one wire call across multiple devices:

```go
writes := make([]hilo.AttributeWrite, 0, len(thermoIDs))
for _, id := range thermoIDs {
    writes = append(writes, hilo.AttributeWrite{
        DeviceID:      id,
        AttributeType: hilo.AttrTargetTemperature,
        Value:         hilo.NewTemperature(20.5).Celsius(),
    })
}
ops, err := c.SetBatchAttributes(ctx, locID, writes)
```

Branch on `errors.Is(err, hilo.ErrOperationStatusUnknown)` for the "PUT applied, completion unobserved" case (see `references/go-api.md`).

For fire-and-forget, swap in `SetBatchAttributesNoWait`.

## Recipe 4: opt every thermostat out of an active Hilo Challenge

```go
events, _ := c.HiloEvents(ctx, strconv.Itoa(locID), 2026)
// pick the active eventID from `events`
list, _ := c.SubscribeDeviceList(ctx, locID)
upd := <-list.Updates() // first push is the snapshot
for _, d := range upd.Devices {
    if d.Type == "Thermostat" {
        if err := c.OptOutDevice(ctx, locID, eventID, d.ID); err != nil {
            log.Printf("opt-out %d: %v", d.ID, err)
        }
    }
}
```

For the persistent default, `SetLocationPreferences(ctx, locID, prefs)` instead of per-event opt-out.

## Recipe 5: activate a scene by name (CLI)

```sh
SCENES=$(./bin/hilo scenes "$LOC")
SCENE_ID=$(jq -r '.[] | select(.name=="Movie night") | .id' <<<"$SCENES")
./bin/hilo scene "$LOC" "$SCENE_ID"
```

## Recipe 6: smart-meter power right now

```sh
./bin/hilo twin '<LOC_HID>' \
  | jq '.devices[] | select(.__typename=="BasicSmartMeter") | .power'
```

In Go, type-switch the `Container.Devices` for `*hilo.BasicSmartMeter` and call `.Power.Watts()`.

## Recipe 7: rename a thermostat

```sh
HILO_LOCATION=$LOC ./bin/hilo rename <devID> "Office"
```

Or in Go:

```go
list, _ := c.SubscribeDeviceList(ctx, locID)
snap := <-list.Updates()
for _, d := range snap.Devices {
    if d.ID == targetID {
        d.Name = "Office"
        _, err := c.UpdateDevice(ctx, locID, d)
        // ...
    }
}
```

`UpdateDevice` is generic — pass the modified `HubDevice` and the server returns the canonical record.

## Anti-recipes — patterns to avoid

- **Don't** use the integer location ID where a `HiloID` URN is required (or vice-versa). The error you'll see is a 404 or a GraphQL `"location not found"`.
- **Don't** treat `ErrOperationStatusUnknown` as a write failure. The PUT applied; only completion confirmation is missing. Check with `errors.Is`.
- **Don't** hold a `Stream` past `ctx` cancellation expecting more updates — channels close.
- **Don't** assume `DevicesValuesReceived` will fire for a write you just made — use `Set*` (which races GraphQL + DeviceHub completion) instead of subscribing manually.
- **Don't** add new dependencies casually. The stdlib-only constraint is intentional.
