package hilo

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// ErrOperationStatusUnknown is returned by Set* methods when the underlying
// PUT succeeded (server issued an operationId) but the SDK could not confirm
// the operation reached a terminal status before ctx expired. The operation
// likely applied server-side; the caller can retry, await separately, or
// accept the uncertainty.
//
// Returned alongside a non-nil *Operation containing OperationID and
// Status=OperationStatusReport (the in-flight status from the bundle's
// vocabulary).
//
// Use errors.Is to detect:
//
//	op, err := c.SetThermostatSetpoint(ctx, locID, devID, target)
//	if errors.Is(err, ErrOperationStatusUnknown) {
//	    // PUT applied; status couldn't be confirmed within ctx deadline.
//	    // op.OperationID is set; op.Status is OperationStatusReport.
//	} else if err != nil {
//	    // Genuine REST or transport error; PUT did NOT apply.
//	}
var ErrOperationStatusUnknown = errors.New("hilo: operation status unverified before ctx deadline")

// SetAttribute writes a single attribute to one device and blocks until the
// operation reaches a terminal status. Returns the resulting *Operation —
// caller checks op.Status to distinguish Succeeded from Rejected/Failed.
//
// The error return is non-nil for transport, REST, or ctx errors. Server-side
// rejection (op.Status != Succeeded) is surfaced via op, not err.
func (c *Client) SetAttribute(ctx context.Context, locID, devID int, attr AttributeType, value any) (*Operation, error) {
	body := map[string]any{string(attr): value}
	var resp []struct {
		OperationID string `json:"operationId"`
	}
	path := fmt.Sprintf("/Automation/v1/api/Locations/%d/Devices/%d/Attributes", locID, devID)
	if err := c.Put(ctx, path, body, &resp); err != nil {
		return nil, fmt.Errorf("SetAttribute PUT: %w", err)
	}
	if len(resp) == 0 || resp[0].OperationID == "" {
		return nil, fmt.Errorf("SetAttribute: server returned no operationId for %s on device %d", attr, devID)
	}
	return c.awaitOp(ctx, locID, devID, attr, resp[0].OperationID)
}

// ensureOpSubscription lazily opens the internal SubscribeDeviceValues for
// the given location, refcount-tracked. Multiple in-flight writes share one
// subscription per location. Safe for concurrent callers.
//
// Uses context.Background() for the inner subscription's lifetime so
// individual user contexts don't tear it down. The cancel runs when the
// last release brings the refcount to zero.
func (c *Client) ensureOpSubscription(locID int) error {
	c.opMu.Lock()
	if sub, ok := c.opSubs[locID]; ok {
		sub.refs++
		c.opMu.Unlock()
		return nil
	}
	c.opMu.Unlock()

	subCtx, cancel := context.WithCancel(context.Background())
	stream, err := c.SubscribeDeviceValues(subCtx, locID)
	if err != nil {
		cancel()
		return err
	}

	sub := &opSub{
		cancel: cancel,
		refs:   1,
		done:   make(chan struct{}),
	}

	c.opMu.Lock()
	if existing, ok := c.opSubs[locID]; ok {
		// Lost a race; tear ours down and reuse theirs.
		c.opMu.Unlock()
		cancel()
		// Drain the stream to allow our orphan goroutine to exit cleanly.
		go func() {
			for range stream.Updates() {
			}
		}()
		c.opMu.Lock()
		existing.refs++
		c.opMu.Unlock()
		return nil
	}
	c.opSubs[locID] = sub
	c.opMu.Unlock()

	go func() {
		defer close(sub.done)
		for upd := range stream.Updates() {
			c.opDispatch(upd.Values)
		}
		// Stream closed — terminal. Notify all awaiters.
		streamErr := stream.Err()
		if streamErr == nil {
			streamErr = fmt.Errorf("DeviceHub stream closed")
		}
		sub.mu.Lock()
		errChs := append([]chan error{}, sub.errChs...)
		sub.mu.Unlock()
		for _, ch := range errChs {
			select {
			case ch <- streamErr:
			default:
			}
		}
	}()
	return nil
}

// releaseOpSubscription decrements the refcount and tears the subscription
// down when it reaches zero.
func (c *Client) releaseOpSubscription(locID int) {
	c.opMu.Lock()
	sub := c.opSubs[locID]
	if sub == nil {
		c.opMu.Unlock()
		return
	}
	sub.refs--
	closing := sub.refs == 0
	if closing {
		delete(c.opSubs, locID)
	}
	c.opMu.Unlock()
	if closing {
		sub.cancel()
		<-sub.done
	}
}

// opDispatch is called by the internal subscription goroutine for every
// DeviceValuesUpdate. For each value carrying a non-empty OperationID, it
// delivers an Operation to a registered awaiter (if any) and always writes
// the echo to the 64-entry ring buffer so a not-yet-registered awaitOp can
// replay it (plugs the race window).
func (c *Client) opDispatch(values []DeviceAttrValue) {
	for _, v := range values {
		if v.OperationID == "" {
			continue
		}
		key := opKey{
			DeviceID:      v.DeviceID,
			AttributeType: AttributeType(v.AttributeType),
			OperationID:   v.OperationID,
		}
		op := &Operation{
			OperationID:  v.OperationID,
			Status:       OperationStatusSucceeded,
			StatusReason: OperationStatusReasonNone,
		}

		c.opMu.Lock()
		// 1) Best-effort delivery to a registered awaiter — UNLESS the awaiter
		// has GraphQL as its authoritative source. Real status comes from
		// gqlOpHandler in that case; always-Succeeded echoes would mask
		// real REJECTED/FAILED outcomes.
		if entry, ok := c.opPending[key]; ok && !entry.preferGraphQL {
			select {
			case entry.ch <- op:
			default:
			}
		}
		// 2) Always write to the ring so a not-yet-registered awaiter can replay.
		c.opEchoBuf[c.opEchoIdx] = opEcho{Key: key, At: time.Now(), Op: op}
		c.opEchoIdx = (c.opEchoIdx + 1) % len(c.opEchoBuf)
		c.opMu.Unlock()
	}
}

// AttributeType is the wire-level name of a device attribute. The Hilo
// backend keys writes by this exact string. Unrecognised attribute types
// still work (the type is just string), but constants are provided for
// the common ones observed in the REST bundle and DeviceHub's device list.
type AttributeType string

const (
	AttrTargetTemperature      AttributeType = "TargetTemperature"
	AttrThermostatMode         AttributeType = "ThermostatMode"
	AttrThermostatAllowedModes AttributeType = "ThermostatAllowedModes"
	AttrPower                  AttributeType = "Power"
	AttrLevel                  AttributeType = "Level"
	AttrHue                    AttributeType = "Hue"
	AttrSaturation             AttributeType = "Saturation"
	AttrColorTemperature       AttributeType = "ColorTemperature"
	AttrCCRMode                AttributeType = "CCRMode"
	AttrGdState                AttributeType = "GdState"
	AttrGrapState              AttributeType = "GrapState"
	AttrMaxTempSetpoint        AttributeType = "MaxTempSetpoint"
	AttrMinTempSetpoint        AttributeType = "MinTempSetpoint"
)

// AttributeWrite is one entry in a SetBatchAttributes call.
type AttributeWrite struct {
	DeviceID      int           `json:"deviceId"`
	AttributeType AttributeType `json:"attributeName"`
	Value         any           `json:"value"`
}

// opKey identifies one in-flight operation in the Client's pending registry.
type opKey struct {
	DeviceID      int
	AttributeType AttributeType
	OperationID   string
}

// opPendingEntry holds an in-flight awaiter's result channel plus a flag
// for whether the GraphQL Subscription path is the authoritative source.
// When preferGraphQL is true, opDispatch MUST NOT deliver always-Succeeded
// echoes to ch — only gqlOpHandler is allowed to deliver.
type opPendingEntry struct {
	ch            chan *Operation
	preferGraphQL bool
}

// opEcho is one entry in the recent-echoes ring buffer. Keeps the awaitOp
// race window deterministic: an echo arriving between the PUT response
// and the channel registration is captured here and replayed when the
// register-side scans.
type opEcho struct {
	Key opKey
	At  time.Time
	Op  *Operation
}

// opSub tracks one location's internal SubscribeDeviceValues subscription
// used to correlate DeviceHub Values echoes back to in-flight writes.
type opSub struct {
	cancel context.CancelFunc
	refs   int
	done   chan struct{}

	mu     sync.Mutex // guards errChs below
	errChs []chan error
}

// SetAttributes writes multiple attributes to one device in a single PUT
// and blocks until ALL operations reach terminal status. Returns ops in
// the same iteration order as the input map (Go map iteration is
// unordered, so callers should not depend on a specific output order).
func (c *Client) SetAttributes(ctx context.Context, locID, devID int, attrs map[AttributeType]any) ([]Operation, error) {
	if len(attrs) == 0 {
		return nil, fmt.Errorf("SetAttributes: no attributes")
	}
	// Build a sorted key slice so iteration matches json.Marshal's sorted
	// key output. The server returns opIds in the same order as the JSON
	// keys it received; pairing requires we use that same order here.
	keys := make([]AttributeType, 0, len(attrs))
	for k := range attrs {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	body := map[string]any{}
	for _, k := range keys {
		body[string(k)] = attrs[k]
	}
	var resp []struct {
		OperationID string `json:"operationId"`
	}
	path := fmt.Sprintf("/Automation/v1/api/Locations/%d/Devices/%d/Attributes", locID, devID)
	if err := c.Put(ctx, path, body, &resp); err != nil {
		return nil, fmt.Errorf("SetAttributes PUT: %w", err)
	}
	if len(resp) != len(attrs) {
		return nil, fmt.Errorf("SetAttributes: server returned %d operationIds for %d attributes", len(resp), len(attrs))
	}
	ops := make([]Operation, 0, len(attrs))
	for i, k := range keys {
		op, err := c.awaitOp(ctx, locID, devID, k, resp[i].OperationID)
		if err != nil {
			return ops, err
		}
		ops = append(ops, *op)
	}
	return ops, nil
}

// SetBatchAttributes writes attributes across multiple devices in one PUT
// to the BatchAttributes endpoint. Awaits all operations sequentially.
func (c *Client) SetBatchAttributes(ctx context.Context, locID int, writes []AttributeWrite) ([]Operation, error) {
	if len(writes) == 0 {
		return nil, fmt.Errorf("SetBatchAttributes: no writes")
	}
	var resp []struct {
		OperationID string `json:"operationId"`
	}
	path := fmt.Sprintf("/Automation/v1/api/Locations/%d/Devices/BatchAttributes", locID)
	if err := c.Put(ctx, path, writes, &resp); err != nil {
		return nil, fmt.Errorf("SetBatchAttributes PUT: %w", err)
	}
	if len(resp) != len(writes) {
		return nil, fmt.Errorf("SetBatchAttributes: server returned %d operationIds for %d writes", len(resp), len(writes))
	}
	ops := make([]Operation, 0, len(writes))
	for i, w := range writes {
		op, err := c.awaitOp(ctx, locID, w.DeviceID, w.AttributeType, resp[i].OperationID)
		if err != nil {
			return ops, err
		}
		ops = append(ops, *op)
	}
	return ops, nil
}

// SetThermostatSetpoint sets a thermostat's TargetTemperature.
func (c *Client) SetThermostatSetpoint(ctx context.Context, locID, devID int, target Temperature) (*Operation, error) {
	return c.SetAttribute(ctx, locID, devID, AttrTargetTemperature, target.Celsius())
}

// SetThermostatMode changes a thermostat's operating mode (e.g. Auto, Manual, Off).
func (c *Client) SetThermostatMode(ctx context.Context, locID, devID int, mode ThermostatMode) (*Operation, error) {
	return c.SetAttribute(ctx, locID, devID, AttrThermostatMode, string(mode))
}

// SetSwitchState toggles a switch on or off via its Power attribute.
func (c *Client) SetSwitchState(ctx context.Context, locID, devID int, on bool) (*Operation, error) {
	power := 0
	if on {
		power = 100
	}
	return c.SetAttribute(ctx, locID, devID, AttrPower, power)
}

// SetLightState toggles a light on or off via its Power attribute.
// (Lights and switches use the same Power attribute on the wire.)
func (c *Client) SetLightState(ctx context.Context, locID, devID int, on bool) (*Operation, error) {
	power := 0
	if on {
		power = 100
	}
	return c.SetAttribute(ctx, locID, devID, AttrPower, power)
}

// SetLightLevel sets a dimmable light to a brightness in [0, 100].
// Returns an error if level is out of range — call site bug, not server.
func (c *Client) SetLightLevel(ctx context.Context, locID, devID int, level int) (*Operation, error) {
	if level < 0 || level > 100 {
		return nil, fmt.Errorf("SetLightLevel: level=%d out of range [0, 100]", level)
	}
	return c.SetAttribute(ctx, locID, devID, AttrLevel, level)
}

// SetLightColor sets hue [0, 360) and saturation [0, 100]. Returns an
// error if either is out of range.
func (c *Client) SetLightColor(ctx context.Context, locID, devID int, hue, saturation int) (*Operation, error) {
	if hue < 0 || hue >= 360 {
		return nil, fmt.Errorf("SetLightColor: hue=%d out of range [0, 360)", hue)
	}
	if saturation < 0 || saturation > 100 {
		return nil, fmt.Errorf("SetLightColor: saturation=%d out of range [0, 100]", saturation)
	}
	// Multi-attribute write so both Hue and Saturation land in one round-trip.
	ops, err := c.SetAttributes(ctx, locID, devID, map[AttributeType]any{
		AttrHue:        hue,
		AttrSaturation: saturation,
	})
	if err != nil {
		return nil, err
	}
	if len(ops) == 0 {
		return nil, fmt.Errorf("SetLightColor: no operations returned")
	}
	// Surface the first non-success status if any; otherwise the first op.
	for _, op := range ops {
		if op.Status != OperationStatusSucceeded {
			op := op // capture range var for the address-of below
			return &op, nil
		}
	}
	op := ops[0]
	return &op, nil
}

// SetWaterHeaterMode sets a Hilo water-heater controller's CCRMode.
// Use one of CCRModeOff, CCRModeAuto, CCRModeAutoBypass, CCRModeManual
// from enums.go.
func (c *Client) SetWaterHeaterMode(ctx context.Context, locID, devID int, mode CCRMode) (*Operation, error) {
	return c.SetAttribute(ctx, locID, devID, AttrCCRMode, string(mode))
}

// registerOpTransportErr registers a buffered channel to receive the terminal
// transport error if the location's internal subscription goes Disconnected.
// Returns a receive-only handle and a cancel function that removes the
// channel from the sub's list.
func (c *Client) registerOpTransportErr(locID int) (<-chan error, func()) {
	ch := make(chan error, 1)
	c.opMu.Lock()
	sub := c.opSubs[locID]
	c.opMu.Unlock()
	if sub == nil {
		return ch, func() {}
	}
	sub.mu.Lock()
	sub.errChs = append(sub.errChs, ch)
	sub.mu.Unlock()
	return ch, func() {
		sub.mu.Lock()
		defer sub.mu.Unlock()
		for i, c := range sub.errChs {
			if c == ch {
				sub.errChs = append(sub.errChs[:i], sub.errChs[i+1:]...)
				return
			}
		}
	}
}

// awaitOp registers a channel in opPending keyed on (devID, attr, opID)
// and waits for opDispatch or the GraphQL subscription to deliver an
// Operation, ctx to cancel, or the internal subscription to fail.
// Cleans up the registration on exit.
//
// When the GraphQL subscription opens successfully, it is the authoritative
// source for status (real REJECTED/FAILED/etc.). The ring buffer replay is
// skipped in that path because the ring only holds always-Succeeded echoes
// from the DeviceHub Values echo. When GraphQL is unavailable, falls back
// to the DeviceHub echo path (ring buffer + opDispatch).
//
// Spec ordering: ensureGraphQLSubscription → ensureOpSubscription →
// register in opPending → ring scan → select. No waiter is registered
// if either ensure fails.
func (c *Client) awaitOp(ctx context.Context, locID, devID int, attr AttributeType, opID string) (*Operation, error) {
	// Prefer GraphQL Subscription path for explicit Operation Status.
	gqlActive := false
	if err := c.ensureGraphQLSubscription(locID); err == nil {
		gqlActive = true
		defer c.releaseGraphQLSubscription(locID)
	} else if c.Logger != nil {
		c.Logger("hilo: GraphQL subscription unavailable, falling back to DeviceHub Values: %v", err)
	}

	if err := c.ensureOpSubscription(locID); err != nil {
		return nil, fmt.Errorf("ensureOpSubscription: %w", err)
	}
	defer c.releaseOpSubscription(locID)

	key := opKey{DeviceID: devID, AttributeType: attr, OperationID: opID}
	ch := make(chan *Operation, 1)

	c.opMu.Lock()
	c.opPending[key] = &opPendingEntry{ch: ch, preferGraphQL: gqlActive}

	// Scan the ring for a recent matching echo (race-window plug).
	// When GraphQL is the authoritative source we skip the ring's
	// always-Succeeded echoes and wait for the real status.
	const maxAge = 30 * time.Second
	now := time.Now()
	if !gqlActive {
		for _, e := range c.opEchoBuf {
			if e.Op == nil {
				continue
			}
			if e.Key == key && now.Sub(e.At) < maxAge {
				delete(c.opPending, key)
				c.opMu.Unlock()
				return e.Op, nil
			}
		}
	}
	c.opMu.Unlock()

	defer func() {
		c.opMu.Lock()
		delete(c.opPending, key)
		c.opMu.Unlock()
	}()

	errCh, cancelErr := c.registerOpTransportErr(locID)
	defer cancelErr()

	select {
	case op := <-ch:
		return op, nil
	case <-ctx.Done():
		if c.Logger != nil {
			c.Logger("hilo: awaitOp(dev=%d attr=%s op=%s) cancelled before terminal status: %v", devID, attr, opID, ctx.Err())
		}
		return &Operation{
			OperationID:  opID,
			Status:       OperationStatusReport,
			StatusReason: OperationStatusReasonNone,
		}, fmt.Errorf("%w: %v", ErrOperationStatusUnknown, ctx.Err())
	case err := <-errCh:
		return nil, fmt.Errorf("DeviceHub transport: %w", err)
	}
}

// SetAttributeNoWait writes a single attribute and returns immediately
// after the PUT — does NOT await operation completion. Use when you don't
// need to confirm the write or want to await separately. Returns the
// server-issued operationId.
func (c *Client) SetAttributeNoWait(ctx context.Context, locID, devID int, attr AttributeType, value any) (string, error) {
	body := map[string]any{string(attr): value}
	var resp []struct {
		OperationID string `json:"operationId"`
	}
	path := fmt.Sprintf("/Automation/v1/api/Locations/%d/Devices/%d/Attributes", locID, devID)
	if err := c.Put(ctx, path, body, &resp); err != nil {
		return "", fmt.Errorf("SetAttributeNoWait PUT: %w", err)
	}
	if len(resp) == 0 || resp[0].OperationID == "" {
		return "", fmt.Errorf("SetAttributeNoWait: server returned no operationId for %s on device %d", attr, devID)
	}
	return resp[0].OperationID, nil
}

// SetAttributesNoWait writes multiple attributes to one device and returns
// immediately. Returns operationIds in the same iteration order as the
// keys returned from sorted iteration of the input map (matches
// json.Marshal's sorted-key serialization, so positional pairing with
// SetAttributes' sequential awaits is consistent).
func (c *Client) SetAttributesNoWait(ctx context.Context, locID, devID int, attrs map[AttributeType]any) ([]string, error) {
	if len(attrs) == 0 {
		return nil, fmt.Errorf("SetAttributesNoWait: no attributes")
	}
	keys := make([]AttributeType, 0, len(attrs))
	for k := range attrs {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	body := map[string]any{}
	for _, k := range keys {
		body[string(k)] = attrs[k]
	}
	var resp []struct {
		OperationID string `json:"operationId"`
	}
	path := fmt.Sprintf("/Automation/v1/api/Locations/%d/Devices/%d/Attributes", locID, devID)
	if err := c.Put(ctx, path, body, &resp); err != nil {
		return nil, fmt.Errorf("SetAttributesNoWait PUT: %w", err)
	}
	if len(resp) != len(attrs) {
		return nil, fmt.Errorf("SetAttributesNoWait: server returned %d operationIds for %d attributes", len(resp), len(attrs))
	}
	opIDs := make([]string, len(resp))
	for i, r := range resp {
		opIDs[i] = r.OperationID
	}
	return opIDs, nil
}

// SetBatchAttributesNoWait writes attributes across multiple devices and
// returns immediately with operationIds in the same order as the input
// writes.
func (c *Client) SetBatchAttributesNoWait(ctx context.Context, locID int, writes []AttributeWrite) ([]string, error) {
	if len(writes) == 0 {
		return nil, fmt.Errorf("SetBatchAttributesNoWait: no writes")
	}
	var resp []struct {
		OperationID string `json:"operationId"`
	}
	path := fmt.Sprintf("/Automation/v1/api/Locations/%d/Devices/BatchAttributes", locID)
	if err := c.Put(ctx, path, writes, &resp); err != nil {
		return nil, fmt.Errorf("SetBatchAttributesNoWait PUT: %w", err)
	}
	if len(resp) != len(writes) {
		return nil, fmt.Errorf("SetBatchAttributesNoWait: server returned %d operationIds for %d writes", len(resp), len(writes))
	}
	opIDs := make([]string, len(resp))
	for i, r := range resp {
		opIDs[i] = r.OperationID
	}
	return opIDs, nil
}
