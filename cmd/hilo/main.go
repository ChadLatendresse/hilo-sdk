// Command hilo is a small CLI for the unofficial Hilo Energie Go client.
// Authentication is .env-driven: HILO_EMAIL + HILO_PASSWORD trigger an OAuth
// 2.0 ROPC password grant against Hilo's B2C tenant on first use.
//
//	hilo whoami                 decode id_token claims for the saved session
//	hilo logout                 wipe the saved token
//	hilo minversion             /status/MinVersion (unauth)
//	hilo notification-alert     /status/notification-alert (unauth)
//	hilo account                /Clientele/api/UserInfo
//	hilo locations              list locations
//	hilo devices <locId>        list devices in a location
//	hilo get <path>             GET arbitrary path on api.hiloenergie.com
//	hilo gql <query>            POST GraphQL query to digital-twin
package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/ChadLatendresse/hilo-sdk"
)

func main() {
	// Load .env from cwd before reading env vars; existing env wins over .env.
	if err := hilo.LoadDotEnv(".env"); err != nil {
		fmt.Fprintln(os.Stderr, "warning: .env:", err)
	}

	if len(os.Args) < 2 {
		usage()
	}
	cmd := os.Args[1]
	args := os.Args[2:]

	c := hilo.NewClient()
	ctx := context.Background()

	switch cmd {
	case "whoami":
		tok, err := c.AccessToken(ctx)
		fatal(err)
		fmt.Println("access_token (first 32):", tok[:min(32, len(tok))]+"...")
		if t, err := (&hilo.FileStore{Path: hilo.DefaultStorePath()}).Load(); err == nil && t.IDToken != "" {
			parts := strings.SplitN(t.IDToken, ".", 3)
			if len(parts) == 3 {
				payload, _ := base64.RawURLEncoding.DecodeString(parts[1])
				var claims map[string]any
				_ = json.Unmarshal(payload, &claims)
				prettyPrint(claims)
			}
		}

	case "logout":
		path := hilo.DefaultStorePath()
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			fatal(err)
		}
		fmt.Fprintln(os.Stderr, "removed", path)

	case "minversion":
		v, err := c.MinVersion(ctx)
		fatal(err)
		fmt.Println(v)

	case "notification-alert":
		raw, err := c.NotificationAlert(ctx)
		fatal(err)
		printRaw(raw)

	case "locations":
		locs, err := c.ListLocations(ctx)
		fatal(err)
		b, _ := json.MarshalIndent(locs, "", "  ")
		fmt.Println(string(b))

	case "location":
		if len(args) < 1 {
			fatal(fmt.Errorf("usage: hilo location <locationId>"))
		}
		loc, err := c.GetLocationREST(ctx, args[0])
		fatal(err)
		b, _ := json.MarshalIndent(loc, "", "  ")
		fmt.Println(string(b))

	case "twin":
		if len(args) < 1 {
			fatal(fmt.Errorf("usage: hilo twin <locationHiloId>"))
		}
		cont, err := c.GetLocation(ctx, hilo.HiloID(args[0]))
		fatal(err)
		b, _ := json.MarshalIndent(cont, "", "  ")
		fmt.Println(string(b))

	case "watch":
		if len(os.Args) < 3 {
			fmt.Fprintln(os.Stderr, "usage: hilo watch <locationId>")
			os.Exit(2)
		}
		locID, err := strconv.Atoi(os.Args[2])
		if err != nil {
			fmt.Fprintf(os.Stderr, "invalid locationId %q: %v\n", os.Args[2], err)
			os.Exit(2)
		}
		runWatch(c, locID)

	case "set":
		if len(os.Args) < 4 {
			fmt.Fprintln(os.Stderr, "usage: hilo set <devID> <attr> [value]")
			fmt.Fprintln(os.Stderr, "  attr: setpoint <celsius> | mode <auto|manual|off>")
			fmt.Fprintln(os.Stderr, "        on | off | level <0..100>")
			fmt.Fprintln(os.Stderr, "        waterheater <off|auto|manual|bypass>")
			fmt.Fprintln(os.Stderr, "  HILO_LOCATION env var must be set")
			os.Exit(2)
		}
		runSet(c, os.Args[2:])

	case "scene":
		if len(os.Args) < 4 {
			fmt.Fprintln(os.Stderr, "usage: hilo scene <locationId> <sceneId>")
			os.Exit(2)
		}
		runScene(c, os.Args[2], os.Args[3])

	case "optout":
		if len(os.Args) < 5 {
			fmt.Fprintln(os.Stderr, "usage: hilo optout <locationId> <eventId> <deviceId>")
			os.Exit(2)
		}
		runOptout(c, os.Args[2], os.Args[3], os.Args[4])

	case "prefs":
		if len(os.Args) < 5 {
			fmt.Fprintln(os.Stderr, "usage: hilo prefs <locationId> <thermostat|other> <opt-in|opt-out>")
			os.Exit(2)
		}
		runPrefs(c, os.Args[2], os.Args[3], os.Args[4])

	case "rename":
		if len(os.Args) < 4 {
			fmt.Fprintln(os.Stderr, "usage: hilo rename <devID> <new-name>")
			fmt.Fprintln(os.Stderr, "  HILO_LOCATION env var must be set")
			os.Exit(2)
		}
		runRename(c, os.Args[2], os.Args[3])

	case "favorite":
		if len(os.Args) < 3 {
			fmt.Fprintln(os.Stderr, "usage: hilo favorite <devID>")
			fmt.Fprintln(os.Stderr, "  HILO_LOCATION env var must be set; toggles isFavorite")
			os.Exit(2)
		}
		runFavorite(c, os.Args[2])

	case "scene-create":
		if len(os.Args) < 4 {
			fmt.Fprintln(os.Stderr, "usage: hilo scene-create <locationId> <name>")
			os.Exit(2)
		}
		runSceneCreate(c, os.Args[2], os.Args[3])

	case "scene-update":
		if len(os.Args) < 5 {
			fmt.Fprintln(os.Stderr, "usage: hilo scene-update <locationId> <sceneId> <new-name>")
			os.Exit(2)
		}
		runSceneUpdate(c, os.Args[2], os.Args[3], os.Args[4])

	case "scene-delete":
		if len(os.Args) < 4 {
			fmt.Fprintln(os.Stderr, "usage: hilo scene-delete <locationId> <sceneId>")
			os.Exit(2)
		}
		runSceneDelete(c, os.Args[2], os.Args[3])

	case "feature-flags":
		if len(args) < 1 {
			fatal(fmt.Errorf("usage: hilo feature-flags <locationId>"))
		}
		f, err := c.LocationFeatureFlags(ctx, args[0])
		fatal(err)
		printRaw(f.Raw)

	case "weather":
		if len(args) < 1 {
			fatal(fmt.Errorf("usage: hilo weather <locationId>"))
		}
		raw, err := c.LocationWeather(ctx, args[0])
		fatal(err)
		printRaw(raw)

	case "history":
		if len(args) < 1 {
			fatal(fmt.Errorf("usage: hilo history <locationId> [date YYYY-MM-DD] [timescale=Day]"))
		}
		date := time.Now()
		if len(args) >= 2 {
			d, err := time.Parse("2006-01-02", args[1])
			if err != nil {
				fatal(fmt.Errorf("date must be YYYY-MM-DD; got %q", args[1]))
			}
			date = d
		}
		timescale := "Day"
		if len(args) >= 3 {
			timescale = args[2]
		}
		raw, err := c.EnergyHistory(ctx, args[0], timescale, date)
		fatal(err)
		printRaw(raw)

	case "events":
		if len(args) < 2 {
			fatal(fmt.Errorf("usage: hilo events <locationId> <season>   (season is the year, e.g. 2026)"))
		}
		season, err := strconv.Atoi(args[1])
		if err != nil {
			fatal(fmt.Errorf("season must be an integer year (e.g. 2026); got %q", args[1]))
		}
		raw, err := c.HiloEvents(ctx, args[0], season)
		fatal(err)
		printRaw(raw)

	case "notifications":
		if len(args) < 1 {
			fatal(fmt.Errorf("usage: hilo notifications <locationId>"))
		}
		raw, err := c.LocationNotifications(ctx, args[0])
		fatal(err)
		printRaw(raw)

	case "scenes":
		if len(args) < 1 {
			fatal(fmt.Errorf("usage: hilo scenes <locationId>"))
		}
		raw, err := c.Scenes(ctx, args[0])
		fatal(err)
		printRaw(raw)

	case "automations":
		if len(args) < 1 {
			fatal(fmt.Errorf("usage: hilo automations <locationId>"))
		}
		raw, err := c.Automations(ctx, args[0])
		fatal(err)
		printRaw(raw)

	case "get":
		if len(args) < 1 {
			fatal(fmt.Errorf("usage: hilo get <path>"))
		}
		var raw json.RawMessage
		fatal(c.Get(ctx, args[0], &raw))
		printRaw(raw)

	case "gql":
		var query string
		if len(args) >= 1 && args[0] != "-" {
			query = args[0]
		} else {
			b, err := io.ReadAll(os.Stdin)
			fatal(err)
			query = string(b)
		}
		resp, err := c.GraphQL(ctx, hilo.GraphQLRequest{Query: query})
		if err != nil && resp == nil {
			fatal(err)
		}
		printRaw(resp.Data)
		if len(resp.Errors) > 0 {
			fmt.Fprintln(os.Stderr, "errors:")
			for _, e := range resp.Errors {
				fmt.Fprintln(os.Stderr, " -", e.Message)
			}
			os.Exit(2)
		}

	default:
		usage()
	}
}

func runWatch(c *hilo.Client, locID int) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	listStream, err := c.SubscribeDeviceList(ctx, locID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "subscribe list: %v\n", err)
		os.Exit(1)
	}

	valsStream, err := c.SubscribeDeviceValues(ctx, locID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "subscribe values: %v\n", err)
		os.Exit(1)
	}

	// Cache device names by id so attribute pushes can show a label.
	var nameMu sync.Mutex
	names := map[int]string{}
	getName := func(id int) string {
		nameMu.Lock()
		defer nameMu.Unlock()
		if n, ok := names[id]; ok {
			return n
		}
		return fmt.Sprintf("device#%d", id)
	}
	setNames := func(devs []hilo.HubDevice) {
		nameMu.Lock()
		defer nameMu.Unlock()
		for _, d := range devs {
			names[d.ID] = d.Name
		}
	}

	go func() {
		for s := range listStream.State() {
			fmt.Fprintf(os.Stderr, "[device-list state] %s\n", s)
		}
	}()
	go func() {
		for s := range valsStream.State() {
			fmt.Fprintf(os.Stderr, "[device-values state] %s\n", s)
		}
	}()

	go func() {
		for upd := range listStream.Updates() {
			setNames(upd.Devices)
			ts := time.Now().Format("15:04:05")
			switch upd.Kind {
			case hilo.DeviceListSnapshot:
				fmt.Printf("%s snapshot: %d devices\n", ts, len(upd.Devices))
				for _, d := range upd.Devices {
					fmt.Printf("  %s [%s/%s] id=%d\n", d.Name, d.Type, d.Category, d.ID)
				}
			case hilo.DeviceListDelta:
				fmt.Printf("%s list delta: %d devices\n", ts, len(upd.Devices))
			case hilo.DeviceListAdded:
				if len(upd.Devices) > 0 {
					d := upd.Devices[0]
					fmt.Printf("%s added: %s [%s] id=%d\n", ts, d.Name, d.Type, d.ID)
				}
			case hilo.DeviceListDeleted:
				if len(upd.Devices) > 0 {
					d := upd.Devices[0]
					fmt.Printf("%s deleted: %s id=%d\n", ts, d.Name, d.ID)
				}
			}
		}
	}()

	for upd := range valsStream.Updates() {
		ts := time.Now().Format("15:04:05")
		for _, v := range upd.Values {
			fmt.Printf("%s %s.%s = %s\n", ts, getName(v.DeviceID), v.AttributeType, string(v.Value))
		}
	}
	if err := valsStream.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "values stream ended: %v\n", err)
	}
}

func runSet(c *hilo.Client, args []string) {
	devID, err := strconv.Atoi(args[0])
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid devID %q: %v\n", args[0], err)
		os.Exit(2)
	}
	locStr := os.Getenv("HILO_LOCATION")
	if locStr == "" {
		fmt.Fprintln(os.Stderr, "HILO_LOCATION env var required for `hilo set`")
		os.Exit(2)
	}
	locID, err := strconv.Atoi(locStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid HILO_LOCATION %q: %v\n", locStr, err)
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	attr := args[1]
	var op *hilo.Operation
	switch attr {
	case "setpoint":
		if len(args) < 3 {
			fmt.Fprintln(os.Stderr, "usage: hilo set <devID> setpoint <celsius>")
			os.Exit(2)
		}
		c64, perr := strconv.ParseFloat(args[2], 64)
		if perr != nil {
			fmt.Fprintf(os.Stderr, "invalid celsius: %v\n", perr)
			os.Exit(2)
		}
		op, err = c.SetThermostatSetpoint(ctx, locID, devID, hilo.NewTemperature(c64))
	case "mode":
		if len(args) < 3 {
			fmt.Fprintln(os.Stderr, "usage: hilo set <devID> mode <auto|manual|off>")
			os.Exit(2)
		}
		var mode hilo.ThermostatMode
		switch strings.ToLower(args[2]) {
		case "auto":
			mode = hilo.ThermostatModeAuto
		case "manual":
			mode = hilo.ThermostatModeManual
		case "off":
			mode = hilo.ThermostatModeOff
		default:
			fmt.Fprintf(os.Stderr, "invalid mode %q\n", args[2])
			os.Exit(2)
		}
		op, err = c.SetThermostatMode(ctx, locID, devID, mode)
	case "on":
		op, err = c.SetSwitchState(ctx, locID, devID, true)
	case "off":
		op, err = c.SetSwitchState(ctx, locID, devID, false)
	case "level":
		if len(args) < 3 {
			fmt.Fprintln(os.Stderr, "usage: hilo set <devID> level <0..100>")
			os.Exit(2)
		}
		l, perr := strconv.Atoi(args[2])
		if perr != nil {
			fmt.Fprintf(os.Stderr, "invalid level: %v\n", perr)
			os.Exit(2)
		}
		op, err = c.SetLightLevel(ctx, locID, devID, l)
	case "waterheater":
		if len(args) < 3 {
			fmt.Fprintln(os.Stderr, "usage: hilo set <devID> waterheater <off|auto|manual|bypass>")
			os.Exit(2)
		}
		var mode hilo.CCRMode
		switch strings.ToLower(args[2]) {
		case "off":
			mode = hilo.CCRModeOff
		case "auto":
			mode = hilo.CCRModeAuto
		case "manual":
			mode = hilo.CCRModeManual
		case "bypass":
			mode = hilo.CCRModeAutoBypass
		default:
			fmt.Fprintf(os.Stderr, "invalid waterheater mode %q\n", args[2])
			os.Exit(2)
		}
		op, err = c.SetWaterHeaterMode(ctx, locID, devID, mode)
	default:
		fmt.Fprintf(os.Stderr, "unknown attr %q\n", attr)
		os.Exit(2)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "set failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("op=%s status=%s reason=%s\n", op.OperationID, op.Status, op.StatusReason)
	if op.Status != hilo.OperationStatusSucceeded {
		os.Exit(1)
	}
}

func runScene(c *hilo.Client, locArg, sceneArg string) {
	locID, err := strconv.Atoi(locArg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid locationId: %v\n", err)
		os.Exit(2)
	}
	sceneID, err := strconv.Atoi(sceneArg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid sceneId: %v\n", err)
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := c.ActivateScene(ctx, locID, sceneID); err != nil {
		fmt.Fprintf(os.Stderr, "ActivateScene: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("scene activated")
}

func runOptout(c *hilo.Client, locArg, eventID, devArg string) {
	locID, err := strconv.Atoi(locArg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid locationId: %v\n", err)
		os.Exit(2)
	}
	devID, err := strconv.Atoi(devArg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid devID: %v\n", err)
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := c.OptOutDevice(ctx, locID, eventID, devID); err != nil {
		fmt.Fprintf(os.Stderr, "OptOutDevice: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("opted out")
}

func runPrefs(c *hilo.Client, locArg, kindArg, valueArg string) {
	locID, err := strconv.Atoi(locArg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid locationId: %v\n", err)
		os.Exit(2)
	}
	var prefType string
	switch strings.ToLower(kindArg) {
	case "thermostat":
		prefType = "Thermostat"
	case "other":
		prefType = "OtherDevices"
	default:
		fmt.Fprintf(os.Stderr, "kind must be 'thermostat' or 'other'\n")
		os.Exit(2)
	}
	var optOut bool
	switch strings.ToLower(valueArg) {
	case "opt-out":
		optOut = true
	case "opt-in":
		optOut = false
	default:
		fmt.Fprintf(os.Stderr, "value must be 'opt-in' or 'opt-out'\n")
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := c.SetLocationPreferences(ctx, locID, hilo.LocationPreferences{
		PreferenceType: prefType,
		OptOut:         optOut,
	}); err != nil {
		fmt.Fprintf(os.Stderr, "SetLocationPreferences: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("preferences set")
}

func runRename(c *hilo.Client, devArg, newName string) {
	devID, err := strconv.Atoi(devArg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid devID: %v\n", err)
		os.Exit(2)
	}
	locStr := os.Getenv("HILO_LOCATION")
	if locStr == "" {
		fmt.Fprintln(os.Stderr, "HILO_LOCATION env var required")
		os.Exit(2)
	}
	locID, err := strconv.Atoi(locStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid HILO_LOCATION: %v\n", err)
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	fmt.Fprintf(os.Stderr, "Rename device %d to %q? [y/N] ", devID, newName)
	var ans string
	_, _ = fmt.Scanln(&ans)
	if strings.ToLower(strings.TrimSpace(ans)) != "y" {
		fmt.Fprintln(os.Stderr, "aborted")
		os.Exit(1)
	}

	dev := hilo.HubDevice{ID: devID, Name: newName}
	out, err := c.UpdateDevice(ctx, locID, dev)
	if err != nil {
		fmt.Fprintf(os.Stderr, "UpdateDevice: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("renamed: id=%d name=%q\n", out.ID, out.Name)
}

func runFavorite(c *hilo.Client, devArg string) {
	devID, err := strconv.Atoi(devArg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid devID: %v\n", err)
		os.Exit(2)
	}
	locStr := os.Getenv("HILO_LOCATION")
	if locStr == "" {
		fmt.Fprintln(os.Stderr, "HILO_LOCATION env var required")
		os.Exit(2)
	}
	locID, err := strconv.Atoi(locStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid HILO_LOCATION: %v\n", err)
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := c.ToggleDeviceFavorite(ctx, locID, devID); err != nil {
		fmt.Fprintf(os.Stderr, "ToggleDeviceFavorite: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("favorite toggled")
}

func runSceneCreate(c *hilo.Client, locArg, name string) {
	locID, err := strconv.Atoi(locArg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid locationId: %v\n", err)
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	out, err := c.CreateScene(ctx, locID, hilo.Scene{Name: name})
	if err != nil {
		fmt.Fprintf(os.Stderr, "CreateScene: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("created: id=%d name=%q\n", out.ID, out.Name)
}

func runSceneUpdate(c *hilo.Client, locArg, sceneArg, newName string) {
	locID, err := strconv.Atoi(locArg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid locationId: %v\n", err)
		os.Exit(2)
	}
	sceneID, err := strconv.Atoi(sceneArg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid sceneId: %v\n", err)
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	out, err := c.UpdateScene(ctx, locID, hilo.Scene{ID: sceneID, Name: newName})
	if err != nil {
		fmt.Fprintf(os.Stderr, "UpdateScene: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("updated: id=%d name=%q\n", out.ID, out.Name)
}

func runSceneDelete(c *hilo.Client, locArg, sceneArg string) {
	locID, err := strconv.Atoi(locArg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid locationId: %v\n", err)
		os.Exit(2)
	}
	sceneID, err := strconv.Atoi(sceneArg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid sceneId: %v\n", err)
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	fmt.Fprintf(os.Stderr, "Delete scene %d at location %d? [y/N] ", sceneID, locID)
	var ans string
	_, _ = fmt.Scanln(&ans)
	if strings.ToLower(strings.TrimSpace(ans)) != "y" {
		fmt.Fprintln(os.Stderr, "aborted")
		os.Exit(1)
	}

	if err := c.DeleteScene(ctx, locID, sceneID); err != nil {
		fmt.Fprintf(os.Stderr, "DeleteScene: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("scene deleted")
}

func usage() {
	fmt.Fprint(os.Stderr, `hilo - unofficial Hilo Energie API client

Setup:
  Create a .env in the cwd with:
      HILO_EMAIL=you@example.com
      HILO_PASSWORD=<your-password>

Read commands:
  hilo whoami                    decode id_token claims for the saved session
  hilo logout                    wipe the saved token
  hilo minversion                /status/MinVersion (unauth)
  hilo notification-alert        /status/notification-alert (unauth)
  hilo locations                 list locations
  hilo location <locId>          fetch one location's REST detail
  hilo twin <locHiloId>          full GraphQL device tree (typed)
  hilo feature-flags <locId>     enabled feature flags for a location
  hilo weather <locId>           current weather for a location
  hilo history <locId> [date] [timescale]   energy history; date YYYY-MM-DD, timescale=Day
  hilo events <locId> <season>   Hilo Challenge events for an integer year (e.g. 2026)
  hilo notifications <locId>     notification feed for a location
  hilo scenes <locId>            scenes at a location
  hilo automations <locId>       automation rules at a location
  hilo watch <locationId>        live device telemetry stream (^C to stop)
  hilo get <path>                GET arbitrary path on api.hiloenergie.com
  hilo gql <query>               POST GraphQL to platform digital-twin (use - for stdin)

Write commands:
  hilo set <devID> ...                            set device attribute (uses HILO_LOCATION env)
  hilo scene <locID> <sceneID>                    activate a scene
  hilo optout <locID> <eventID> <devID>           opt one device out of one Hilo event
  hilo prefs <locID> <thermostat|other> <opt-in|opt-out>  set default opt-out preference
  hilo rename <devID> <new-name>           rename a device (uses HILO_LOCATION env)
  hilo favorite <devID>                    toggle isFavorite
  hilo scene-create <locID> <name>         create a scene
  hilo scene-update <locID> <sceneID> <new-name>  rename a scene
  hilo scene-delete <locID> <sceneID>      delete a scene

Tokens are cached at ~/.config/hilo/tokens.json and refreshed automatically.
`)
	os.Exit(2)
}

func fatal(err error) {
	if err == nil {
		return
	}
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}

func printRaw(raw json.RawMessage) {
	if len(raw) == 0 {
		return
	}
	var v any
	if json.Unmarshal(raw, &v) == nil {
		b, _ := json.MarshalIndent(v, "", "  ")
		fmt.Println(string(b))
		return
	}
	fmt.Println(string(raw))
}

func prettyPrint(v any) {
	b, _ := json.MarshalIndent(v, "", "  ")
	fmt.Println(string(b))
}
