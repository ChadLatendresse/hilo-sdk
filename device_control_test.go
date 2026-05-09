package hilo

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestAttributeTypeConstants(t *testing.T) {
	cases := map[AttributeType]string{
		AttrTargetTemperature: "TargetTemperature",
		AttrThermostatMode:    "ThermostatMode",
		AttrPower:             "Power",
		AttrLevel:             "Level",
		AttrCCRMode:           "CCRMode",
	}
	for got, want := range cases {
		if string(got) != want {
			t.Errorf("AttributeType const = %q, want %q", string(got), want)
		}
	}
}

func TestClientHasOperationRegistryFields(t *testing.T) {
	c := NewClient()
	c.opMu.Lock()
	if c.opPending == nil {
		t.Error("opPending not initialised")
	}
	if c.opSubs == nil {
		t.Error("opSubs not initialised")
	}
	c.opMu.Unlock()
}

func TestEnsureOpSubscriptionRefcount(t *testing.T) {
	c := NewClient()
	fake := newFakeTransport()
	c.dialHubFn = func(ctx context.Context, k hubKind) (*hubConn, error) {
		h := newHubConn(k, "u", fake)
		go h.observeAndPropagate()
		return h, nil
	}
	fake.replyInvoke("SubscribeToLocation", nil, nil)
	fake.replyInvoke("UnsubscribeFromLocation", nil, nil)
	fake.setState(StateConnected)

	if err := c.ensureOpSubscription(12345); err != nil {
		t.Fatal(err)
	}
	c.opMu.Lock()
	if c.opSubs[12345] == nil {
		c.opMu.Unlock()
		t.Fatal("opSubs[12345] not registered")
	}
	if c.opSubs[12345].refs != 1 {
		c.opMu.Unlock()
		t.Errorf("refs=%d, want 1", c.opSubs[12345].refs)
	}
	c.opMu.Unlock()

	// Second ensure bumps refs without re-dialing.
	if err := c.ensureOpSubscription(12345); err != nil {
		t.Fatal(err)
	}
	c.opMu.Lock()
	if c.opSubs[12345].refs != 2 {
		c.opMu.Unlock()
		t.Errorf("after second ensure: refs=%d, want 2", c.opSubs[12345].refs)
	}
	c.opMu.Unlock()

	// Release once: still alive.
	c.releaseOpSubscription(12345)
	c.opMu.Lock()
	if c.opSubs[12345] == nil {
		c.opMu.Unlock()
		t.Fatal("opSubs[12345] cleared after first release")
	}
	c.opMu.Unlock()

	// Release twice: gone.
	c.releaseOpSubscription(12345)
	c.opMu.Lock()
	if c.opSubs[12345] != nil {
		c.opMu.Unlock()
		t.Error("opSubs[12345] still alive after second release")
	}
	c.opMu.Unlock()
}

func TestOpDispatchRoutesByOpKey(t *testing.T) {
	c := NewClient()
	ch := make(chan *Operation, 1)
	key := opKey{DeviceID: 67890, AttributeType: AttrTargetTemperature, OperationID: "op-42"}
	c.opMu.Lock()
	c.opPending[key] = &opPendingEntry{ch: ch, preferGraphQL: false}
	c.opMu.Unlock()

	c.opDispatch([]DeviceAttrValue{
		{DeviceID: 9999, AttributeType: "Power", OperationID: "op-other"}, // no match
		{DeviceID: 67890, AttributeType: "TargetTemperature", OperationID: "op-42"},
	})

	select {
	case op := <-ch:
		if op == nil {
			t.Fatal("nil op")
		}
		if op.OperationID != "op-42" {
			t.Errorf("OperationID=%q", op.OperationID)
		}
		if op.Status != OperationStatusSucceeded {
			t.Errorf("Status=%v, want Succeeded", op.Status)
		}
	case <-time.After(time.Second):
		t.Fatal("matching echo did not deliver")
	}
}

func TestOpDispatchIgnoresEmptyOperationID(t *testing.T) {
	c := NewClient()
	ch := make(chan *Operation, 1)
	key := opKey{DeviceID: 1, AttributeType: AttrPower, OperationID: ""}
	c.opMu.Lock()
	c.opPending[key] = &opPendingEntry{ch: ch, preferGraphQL: false}
	c.opMu.Unlock()

	c.opDispatch([]DeviceAttrValue{
		{DeviceID: 1, AttributeType: "Power", OperationID: ""},
	})

	select {
	case <-ch:
		t.Fatal("dispatch fired on empty OperationID")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestAwaitOpDeliversWhenEchoArrives(t *testing.T) {
	c := NewClient()
	fake := newFakeTransport()
	c.dialHubFn = func(ctx context.Context, k hubKind) (*hubConn, error) {
		h := newHubConn(k, "u", fake)
		go h.observeAndPropagate()
		return h, nil
	}
	fake.replyInvoke("SubscribeToLocation", nil, nil)
	fake.replyInvoke("UnsubscribeFromLocation", nil, nil)
	fake.setState(StateConnected)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resultCh := make(chan *Operation, 1)
	errCh := make(chan error, 1)
	go func() {
		op, err := c.awaitOp(ctx, 12345, 67890, AttrTargetTemperature, "op-77")
		resultCh <- op
		errCh <- err
	}()

	// Wait for awaitOp to register before firing the echo.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		c.opMu.Lock()
		_, ok := c.opPending[opKey{67890, AttrTargetTemperature, "op-77"}]
		c.opMu.Unlock()
		if ok {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}

	c.opDispatch([]DeviceAttrValue{
		{DeviceID: 67890, AttributeType: "TargetTemperature", OperationID: "op-77"},
	})

	op := <-resultCh
	err := <-errCh
	if err != nil {
		t.Fatal(err)
	}
	if op.Status != OperationStatusSucceeded {
		t.Errorf("Status=%v", op.Status)
	}

	c.opMu.Lock()
	if _, ok := c.opPending[opKey{67890, AttrTargetTemperature, "op-77"}]; ok {
		c.opMu.Unlock()
		t.Error("opPending entry not cleaned up after delivery")
	}
	c.opMu.Unlock()
}

func TestAwaitOpReturnsCtxErrOnTimeout(t *testing.T) {
	c := NewClient()
	fake := newFakeTransport()
	c.dialHubFn = func(ctx context.Context, k hubKind) (*hubConn, error) {
		h := newHubConn(k, "u", fake)
		go h.observeAndPropagate()
		return h, nil
	}
	fake.replyInvoke("SubscribeToLocation", nil, nil)
	fake.replyInvoke("UnsubscribeFromLocation", nil, nil)
	fake.setState(StateConnected)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	op, err := c.awaitOp(ctx, 12345, 1, AttrPower, "op-never")
	if err == nil {
		t.Fatal("expected ctx error, got nil")
	}
	if !errors.Is(err, ErrOperationStatusUnknown) {
		t.Errorf("err=%v, want ErrOperationStatusUnknown wrap", err)
	}
	if op == nil {
		t.Fatal("expected non-nil *Operation, got nil")
	}
	if op.OperationID != "op-never" {
		t.Errorf("op.OperationID=%q, want op-never", op.OperationID)
	}
	if op.Status != OperationStatusReport {
		t.Errorf("op.Status=%v, want OperationStatusReport", op.Status)
	}

	c.opMu.Lock()
	if _, ok := c.opPending[opKey{1, AttrPower, "op-never"}]; ok {
		c.opMu.Unlock()
		t.Error("opPending entry leaked after ctx timeout")
	}
	c.opMu.Unlock()
}

// withTestServer points the Client at an httptest.Server's URL for REST
// calls. Returns a teardown func.
//
// The server intercepts two paths that awaitOp now hits before the actual
// device-control PUT:
//   - GET /Automation/v1/api/Locations — locationHiloID fallback; returns
//     an empty array so ensureGraphQLSubscription fails gracefully and the
//     DeviceHub echo path is used (which is what these tests exercise).
//   - POST /api/digital-twin/v3/graphql — returns 404 so
//     openGraphQLSubscription fails fast without a timeout.
//
// All other requests are forwarded to h.
func withTestServer(t *testing.T, c *Client, h http.HandlerFunc) func() {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/Automation/v1/api/Locations", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/Automation/v1/api/Locations" {
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`[]`))
			return
		}
		h(w, r)
	})
	mux.HandleFunc("/api/digital-twin/v3/graphql", func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	})
	mux.HandleFunc("/", h)
	srv := httptest.NewServer(mux)
	c.APIBase = srv.URL
	c.PlatformBase = srv.URL
	// Bypass token negotiation by injecting a token directly.
	c.mu.Lock()
	c.token = &TokenSet{AccessToken: "test-token", ExpiresAt: time.Now().Add(time.Hour)}
	c.mu.Unlock()
	return func() { srv.Close() }
}

func readAll(r io.Reader) ([]byte, error) { return io.ReadAll(r) }

func TestSetAttributePUTAndAwaitsEcho(t *testing.T) {
	c := NewClient()

	gotPath := ""
	gotBody := []byte(nil)
	teardown := withTestServer(t, c, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "PUT" {
			t.Errorf("method=%s, want PUT", r.Method)
		}
		gotPath = r.URL.Path
		var err error
		gotBody, err = readAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`[{"operationId":"op-set-1"}]`))
	})
	defer teardown()

	fake := newFakeTransport()
	c.dialHubFn = func(ctx context.Context, k hubKind) (*hubConn, error) {
		h := newHubConn(k, "u", fake)
		go h.observeAndPropagate()
		return h, nil
	}
	fake.replyInvoke("SubscribeToLocation", nil, nil)
	fake.replyInvoke("UnsubscribeFromLocation", nil, nil)
	fake.setState(StateConnected)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	type result struct {
		op  *Operation
		err error
	}
	out := make(chan result, 1)
	go func() {
		op, err := c.SetAttribute(ctx, 12345, 67890, AttrTargetTemperature, 21.5)
		out <- result{op, err}
	}()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		c.opMu.Lock()
		_, ok := c.opPending[opKey{67890, AttrTargetTemperature, "op-set-1"}]
		c.opMu.Unlock()
		if ok {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	c.opDispatch([]DeviceAttrValue{
		{DeviceID: 67890, AttributeType: "TargetTemperature", OperationID: "op-set-1"},
	})

	r := <-out
	if r.err != nil {
		t.Fatal(r.err)
	}
	if r.op.Status != OperationStatusSucceeded {
		t.Errorf("Status=%v", r.op.Status)
	}

	wantPath := "/Automation/v1/api/Locations/12345/Devices/67890/Attributes"
	if gotPath != wantPath {
		t.Errorf("path=%q, want %q", gotPath, wantPath)
	}
	if !bytes.Contains(gotBody, []byte(`"TargetTemperature":21.5`)) {
		t.Errorf("body did not contain TargetTemperature:21.5: %s", gotBody)
	}
	_ = strings.TrimSpace
}

func TestSetAttributesMultiAttr(t *testing.T) {
	c := NewClient()
	teardown := withTestServer(t, c, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`[{"operationId":"op-a"},{"operationId":"op-b"}]`))
	})
	defer teardown()

	fake := newFakeTransport()
	c.dialHubFn = func(ctx context.Context, k hubKind) (*hubConn, error) {
		h := newHubConn(k, "u", fake)
		go h.observeAndPropagate()
		return h, nil
	}
	fake.replyInvoke("SubscribeToLocation", nil, nil)
	fake.replyInvoke("UnsubscribeFromLocation", nil, nil)
	fake.setState(StateConnected)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	type result struct {
		ops []Operation
		err error
	}
	out := make(chan result, 1)
	go func() {
		ops, err := c.SetAttributes(ctx, 12345, 67890, map[AttributeType]any{
			AttrTargetTemperature: 21.5,
			AttrThermostatMode:    "Manual",
		})
		out <- result{ops, err}
	}()

	// Wait until BOTH op IDs are registered, since SetAttributes awaits sequentially:
	// it waits on op-a first, so we need to dispatch op-a, but op-b's registration
	// happens only AFTER op-a is delivered. So we must dispatch op-a, then wait
	// for op-b registration, then dispatch op-b.
	deadline := time.Now().Add(2 * time.Second)
	dispatchA := false
	dispatchB := false
	for time.Now().Before(deadline) {
		c.opMu.Lock()
		_, hasA := c.opPending[opKey{67890, AttrTargetTemperature, "op-a"}]
		_, hasB := c.opPending[opKey{67890, AttrThermostatMode, "op-b"}]
		c.opMu.Unlock()
		if hasA && !dispatchA {
			c.opDispatch([]DeviceAttrValue{{DeviceID: 67890, AttributeType: "TargetTemperature", OperationID: "op-a"}})
			dispatchA = true
		}
		if hasB && !dispatchB {
			c.opDispatch([]DeviceAttrValue{{DeviceID: 67890, AttributeType: "ThermostatMode", OperationID: "op-b"}})
			dispatchB = true
		}
		if dispatchA && dispatchB {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}

	r := <-out
	if r.err != nil {
		t.Fatal(r.err)
	}
	if len(r.ops) != 2 {
		t.Fatalf("got %d ops, want 2", len(r.ops))
	}
	for _, op := range r.ops {
		if op.Status != OperationStatusSucceeded {
			t.Errorf("op %s: Status=%v", op.OperationID, op.Status)
		}
	}
}

func TestSetThermostatSetpointPassesCorrectAttribute(t *testing.T) {
	c := NewClient()
	teardown := withTestServer(t, c, func(w http.ResponseWriter, r *http.Request) {
		body, _ := readAll(r.Body)
		// Body should contain {"TargetTemperature":21.5} (the Temperature
		// type's Celsius() method returns the float).
		if !strings.Contains(string(body), `"TargetTemperature":21.5`) {
			t.Errorf("body=%s", body)
		}
		w.Write([]byte(`[{"operationId":"op-set-1"}]`))
	})
	defer teardown()

	fake := newFakeTransport()
	c.dialHubFn = func(ctx context.Context, k hubKind) (*hubConn, error) {
		h := newHubConn(k, "u", fake)
		go h.observeAndPropagate()
		return h, nil
	}
	fake.replyInvoke("SubscribeToLocation", nil, nil)
	fake.replyInvoke("UnsubscribeFromLocation", nil, nil)
	fake.setState(StateConnected)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	type result struct {
		op  *Operation
		err error
	}
	out := make(chan result, 1)
	go func() {
		op, err := c.SetThermostatSetpoint(ctx, 12345, 67890, NewTemperature(21.5))
		out <- result{op, err}
	}()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		c.opMu.Lock()
		_, ok := c.opPending[opKey{67890, AttrTargetTemperature, "op-set-1"}]
		c.opMu.Unlock()
		if ok {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	c.opDispatch([]DeviceAttrValue{
		{DeviceID: 67890, AttributeType: "TargetTemperature", OperationID: "op-set-1"},
	})

	r := <-out
	if r.err != nil {
		t.Fatal(r.err)
	}
	if r.op.Status != OperationStatusSucceeded {
		t.Errorf("Status=%v", r.op.Status)
	}
}

func TestSetLightLevelValidatesRange(t *testing.T) {
	c := NewClient()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if _, err := c.SetLightLevel(ctx, 12345, 1, -1); err == nil {
		t.Error("expected error for level=-1")
	}
	if _, err := c.SetLightLevel(ctx, 12345, 1, 101); err == nil {
		t.Error("expected error for level=101")
	}
	if _, err := c.SetLightLevel(ctx, 12345, 1, 200); err == nil {
		t.Error("expected error for level=200")
	}
}

func TestSetLightColorValidatesRanges(t *testing.T) {
	c := NewClient()
	ctx := context.Background()
	if _, err := c.SetLightColor(ctx, 1, 1, -1, 50); err == nil {
		t.Error("expected error for hue=-1")
	}
	if _, err := c.SetLightColor(ctx, 1, 1, 360, 50); err == nil {
		t.Error("expected error for hue=360")
	}
	if _, err := c.SetLightColor(ctx, 1, 1, 0, 101); err == nil {
		t.Error("expected error for saturation=101")
	}
}

func TestSetLightColorSurfacesAnyNonSuccessStatus(t *testing.T) {
	c := NewClient()
	teardown := withTestServer(t, c, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`[{"operationId":"op-hue"},{"operationId":"op-sat"}]`))
	})
	defer teardown()

	fake := newFakeTransport()
	c.dialHubFn = func(ctx context.Context, k hubKind) (*hubConn, error) {
		h := newHubConn(k, "u", fake)
		go h.observeAndPropagate()
		return h, nil
	}
	fake.replyInvoke("SubscribeToLocation", nil, nil)
	fake.replyInvoke("UnsubscribeFromLocation", nil, nil)
	fake.setState(StateConnected)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	type result struct {
		op  *Operation
		err error
	}
	out := make(chan result, 1)
	go func() {
		op, err := c.SetLightColor(ctx, 12345, 1, 180, 50)
		out <- result{op, err}
	}()

	// Sorted-key order means Hue ("Hue") < Saturation ("Saturation"), so
	// resp[0].OperationID = "op-hue", resp[1].OperationID = "op-sat".
	// We dispatch ONLY for Hue with Succeeded; for Saturation, we synthesize
	// a "rejected" by injecting an Operation directly into the channel.
	deadline := time.Now().Add(time.Second)
	dispatchedHue := false
	injectedSat := false
	for time.Now().Before(deadline) {
		c.opMu.Lock()
		_, hasHue := c.opPending[opKey{1, AttrHue, "op-hue"}]
		satEntry, hasSat := c.opPending[opKey{1, AttrSaturation, "op-sat"}]
		c.opMu.Unlock()
		if hasHue && !dispatchedHue {
			c.opDispatch([]DeviceAttrValue{{DeviceID: 1, AttributeType: "Hue", OperationID: "op-hue"}})
			dispatchedHue = true
		}
		if hasSat && !injectedSat {
			// Synthesize a Rejected status by sending directly into the
			// channel. This bypasses opDispatch (which always emits
			// Succeeded).
			satEntry.ch <- &Operation{OperationID: "op-sat", Status: OperationStatusRejected, StatusReason: OperationStatusReasonInvalidArgument}
			injectedSat = true
		}
		if dispatchedHue && injectedSat {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}

	r := <-out
	if r.err != nil {
		t.Fatal(r.err)
	}
	// The returned *Operation should reflect the rejection, not the success.
	if r.op.Status == OperationStatusSucceeded {
		t.Errorf("SetLightColor returned Succeeded but Saturation was Rejected; got op=%+v", r.op)
	}
	if r.op.Status != OperationStatusRejected {
		t.Errorf("Status=%v, want Rejected", r.op.Status)
	}
}

func TestNewTemperatureIsCelsius(t *testing.T) {
	temp := NewTemperature(21.5)
	if temp.Value != 21.5 {
		t.Errorf("Value=%v", temp.Value)
	}
	if temp.Kind != TemperatureKindDegreeCelsius {
		t.Errorf("Kind=%v", temp.Kind)
	}
	if temp.Celsius() != 21.5 {
		t.Errorf("Celsius()=%v", temp.Celsius())
	}
}

func TestAwaitOpRecentEchoesReplay(t *testing.T) {
	c := NewClient()
	fake := newFakeTransport()
	c.dialHubFn = func(ctx context.Context, k hubKind) (*hubConn, error) {
		h := newHubConn(k, "u", fake)
		go h.observeAndPropagate()
		return h, nil
	}
	fake.replyInvoke("SubscribeToLocation", nil, nil)
	fake.replyInvoke("UnsubscribeFromLocation", nil, nil)
	fake.setState(StateConnected)

	// Fire the echo BEFORE awaitOp registers — exercises the race window.
	c.opDispatch([]DeviceAttrValue{
		{DeviceID: 42, AttributeType: "TargetTemperature", OperationID: "op-pre"},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	op, err := c.awaitOp(ctx, 12345, 42, AttrTargetTemperature, "op-pre")
	if err != nil {
		t.Fatalf("awaitOp returned err=%v (ring buffer should have replayed)", err)
	}
	if op == nil || op.Status != OperationStatusSucceeded {
		t.Errorf("op=%+v, want Succeeded", op)
	}
}

func TestRingBufferDiscardsOldEntries(t *testing.T) {
	c := NewClient()
	for i := 0; i < 100; i++ {
		c.opDispatch([]DeviceAttrValue{
			{DeviceID: i, AttributeType: "Power", OperationID: fmt.Sprintf("op-%d", i)},
		})
	}
	c.opMu.Lock()
	defer c.opMu.Unlock()
	found := 0
	for _, e := range c.opEchoBuf {
		if e.Op != nil {
			found++
		}
	}
	if found != 64 {
		t.Errorf("ring contains %d entries, want 64 after 100 writes", found)
	}
}

func TestRingBufferIgnoresStaleEchoes(t *testing.T) {
	c := NewClient()
	c.opMu.Lock()
	c.opEchoBuf[0] = opEcho{
		Key: opKey{DeviceID: 1, AttributeType: AttrPower, OperationID: "op-stale"},
		At:  time.Now().Add(-5 * time.Minute),
		Op:  &Operation{OperationID: "op-stale", Status: OperationStatusSucceeded},
	}
	c.opEchoIdx = 1
	c.opMu.Unlock()

	fake := newFakeTransport()
	c.dialHubFn = func(ctx context.Context, k hubKind) (*hubConn, error) {
		h := newHubConn(k, "u", fake)
		go h.observeAndPropagate()
		return h, nil
	}
	fake.replyInvoke("SubscribeToLocation", nil, nil)
	fake.replyInvoke("UnsubscribeFromLocation", nil, nil)
	fake.setState(StateConnected)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := c.awaitOp(ctx, 12345, 1, AttrPower, "op-stale")
	if err == nil {
		t.Error("expected ctx timeout — stale ring entry should not have replayed")
	}
}

func TestAwaitOpReturnsTransportErrorPromptly(t *testing.T) {
	c := NewClient()
	fake := newFakeTransport()
	c.dialHubFn = func(ctx context.Context, k hubKind) (*hubConn, error) {
		h := newHubConn(k, "u", fake)
		go h.observeAndPropagate()
		return h, nil
	}
	fake.replyInvoke("SubscribeToLocation", nil, nil)
	fake.replyInvoke("UnsubscribeFromLocation", nil, nil)
	fake.setState(StateConnected)

	// Use a long ctx; we want to verify awaitOp returns BEFORE ctx fires.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resultCh := make(chan error, 1)
	go func() {
		_, err := c.awaitOp(ctx, 12345, 42, AttrPower, "op-network-died")
		resultCh <- err
	}()

	// Wait for awaitOp to register, then simulate the transport going terminal.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		c.opMu.Lock()
		_, ok := c.opPending[opKey{42, AttrPower, "op-network-died"}]
		c.opMu.Unlock()
		if ok {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	fake.setState(StateDisconnected)

	// awaitOp should return within ~500ms with a transport error wrap.
	select {
	case err := <-resultCh:
		if err == nil {
			t.Fatal("expected transport error, got nil")
		}
		if !strings.Contains(err.Error(), "DeviceHub") && !strings.Contains(err.Error(), "transport") {
			t.Errorf("err=%v, want a transport-error wrap", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("awaitOp did not return within 2s of StateDisconnected")
	}
}

func TestSetBatchAttributes(t *testing.T) {
	c := NewClient()
	gotBody := []byte(nil)
	teardown := withTestServer(t, c, func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/Devices/BatchAttributes") {
			t.Errorf("path=%s", r.URL.Path)
		}
		gotBody, _ = readAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`[{"operationId":"op-x"},{"operationId":"op-y"}]`))
	})
	defer teardown()

	fake := newFakeTransport()
	c.dialHubFn = func(ctx context.Context, k hubKind) (*hubConn, error) {
		h := newHubConn(k, "u", fake)
		go h.observeAndPropagate()
		return h, nil
	}
	fake.replyInvoke("SubscribeToLocation", nil, nil)
	fake.replyInvoke("UnsubscribeFromLocation", nil, nil)
	fake.setState(StateConnected)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	type result struct {
		ops []Operation
		err error
	}
	out := make(chan result, 1)
	go func() {
		ops, err := c.SetBatchAttributes(ctx, 12345, []AttributeWrite{
			{DeviceID: 1, AttributeType: AttrPower, Value: 100},
			{DeviceID: 2, AttributeType: AttrLevel, Value: 50},
		})
		out <- result{ops, err}
	}()

	// Same sequential-await pattern as SetAttributes.
	deadline := time.Now().Add(2 * time.Second)
	dispatchX := false
	dispatchY := false
	for time.Now().Before(deadline) {
		c.opMu.Lock()
		_, hasX := c.opPending[opKey{1, AttrPower, "op-x"}]
		_, hasY := c.opPending[opKey{2, AttrLevel, "op-y"}]
		c.opMu.Unlock()
		if hasX && !dispatchX {
			c.opDispatch([]DeviceAttrValue{{DeviceID: 1, AttributeType: "Power", OperationID: "op-x"}})
			dispatchX = true
		}
		if hasY && !dispatchY {
			c.opDispatch([]DeviceAttrValue{{DeviceID: 2, AttributeType: "Level", OperationID: "op-y"}})
			dispatchY = true
		}
		if dispatchX && dispatchY {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}

	r := <-out
	if r.err != nil {
		t.Fatal(r.err)
	}
	if len(r.ops) != 2 {
		t.Fatalf("got %d ops, want 2", len(r.ops))
	}
	if !strings.Contains(string(gotBody), `"deviceId":1`) || !strings.Contains(string(gotBody), `"attributeName":"Power"`) {
		t.Errorf("body missing expected fields: %s", gotBody)
	}
}

func TestConcurrentSameDeviceAttributeWrites(t *testing.T) {
	c := NewClient()
	fake := newFakeTransport()
	c.dialHubFn = func(ctx context.Context, k hubKind) (*hubConn, error) {
		h := newHubConn(k, "u", fake)
		go h.observeAndPropagate()
		return h, nil
	}
	fake.replyInvoke("SubscribeToLocation", nil, nil)
	fake.replyInvoke("UnsubscribeFromLocation", nil, nil)
	fake.setState(StateConnected)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	type result struct {
		op  *Operation
		err error
	}
	out1 := make(chan result, 1)
	out2 := make(chan result, 1)

	go func() {
		op, err := c.awaitOp(ctx, 12345, 42, AttrPower, "op-A")
		out1 <- result{op, err}
	}()
	go func() {
		op, err := c.awaitOp(ctx, 12345, 42, AttrPower, "op-B")
		out2 <- result{op, err}
	}()

	// Wait for both to register.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		c.opMu.Lock()
		_, hasA := c.opPending[opKey{42, AttrPower, "op-A"}]
		_, hasB := c.opPending[opKey{42, AttrPower, "op-B"}]
		c.opMu.Unlock()
		if hasA && hasB {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}

	// Dispatch both echoes in one call.
	c.opDispatch([]DeviceAttrValue{
		{DeviceID: 42, AttributeType: "Power", OperationID: "op-A"},
		{DeviceID: 42, AttributeType: "Power", OperationID: "op-B"},
	})

	r1 := <-out1
	r2 := <-out2

	if r1.err != nil || r1.op == nil {
		t.Errorf("await A: err=%v op=%v", r1.err, r1.op)
	} else if r1.op.OperationID != "op-A" {
		t.Errorf("await A: opID=%s, want op-A", r1.op.OperationID)
	}
	if r2.err != nil || r2.op == nil {
		t.Errorf("await B: err=%v op=%v", r2.err, r2.op)
	} else if r2.op.OperationID != "op-B" {
		t.Errorf("await B: opID=%s, want op-B", r2.op.OperationID)
	}
}

func TestGraphQLDeliveryWinsOverDeviceHubEcho(t *testing.T) {
	// Reproduce the dual-source race: when GraphQL is active, a
	// DeviceHub Values echo MUST NOT pre-empt the real-status delivery.

	c := NewClient()

	// Start a multipart server that holds the connection open and never
	// fires (we'll synthesise the GraphQL delivery directly via the
	// handler closure).
	mux := http.NewServeMux()
	mux.HandleFunc("/Automation/v1/api/Locations", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`[{"id":12345,"locationHiloId":"urn:hilo:crm:test:0","name":"Test"}]`))
	})
	mux.HandleFunc("/api/digital-twin/v3/graphql", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", `multipart/mixed; boundary=graphql; subscriptionSpec=1.0`)
		w.WriteHeader(http.StatusOK)
		fl, _ := w.(http.Flusher)
		if fl != nil {
			fl.Flush()
		}
		<-r.Context().Done()
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c.PlatformBase = srv.URL
	c.APIBase = srv.URL
	c.mu.Lock()
	c.token = &TokenSet{AccessToken: "test-token", ExpiresAt: time.Now().Add(time.Hour)}
	c.mu.Unlock()

	c.PrimeLocationHiloID(12345, "urn:hilo:crm:test:0")

	fake := newFakeTransport()
	c.dialHubFn = func(ctx context.Context, k hubKind) (*hubConn, error) {
		h := newHubConn(k, "u", fake)
		go h.observeAndPropagate()
		return h, nil
	}
	fake.replyInvoke("SubscribeToLocation", nil, nil)
	fake.replyInvoke("UnsubscribeFromLocation", nil, nil)
	fake.setState(StateConnected)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	resultCh := make(chan *Operation, 1)
	errCh := make(chan error, 1)
	go func() {
		op, err := c.awaitOp(ctx, 12345, 42, AttrPower, "op-rejected")
		resultCh <- op
		errCh <- err
	}()

	// Wait for awaitOp to register.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		c.opMu.Lock()
		_, ok := c.opPending[opKey{42, AttrPower, "op-rejected"}]
		c.opMu.Unlock()
		if ok {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}

	// First, fire a DeviceHub echo (simulates the SignalR fast path).
	// This SHOULD NOT deliver to the awaiting channel — GraphQL is
	// authoritative.
	c.opDispatch([]DeviceAttrValue{
		{DeviceID: 42, AttributeType: "Power", OperationID: "op-rejected"},
	})

	// Now simulate the GraphQL handler delivering the real REJECTED status.
	c.gqlOpHandler(12345)([]byte(`{"data":{"onAnyDeviceUpdated":{"operationId":"op-rejected","device":{"id":42},"status":"REJECTED","statusReason":"INVALID_ARGUMENT"}}}`))

	op := <-resultCh
	err := <-errCh
	if err != nil {
		t.Fatal(err)
	}
	if op.Status == OperationStatusSucceeded {
		t.Fatal("DeviceHub echo masked the GraphQL REJECTED status (dual-source race)")
	}
	if op.Status != OperationStatusRejected {
		t.Errorf("Status=%v, want Rejected", op.Status)
	}
}

func TestGraphQLSubscriptionDeliversRealStatus(t *testing.T) {
	gqlSrv := multipartStreamServer(t, []string{
		// Server reports real REJECTED status for op-fail
		`{"data":{"onAnyDeviceUpdated":{"operationId":"op-fail","device":{"id":42},"status":"REJECTED","statusReason":"INVALID_ARGUMENT"}}}`,
	}, true)
	defer gqlSrv.Close()

	c := NewClient()
	c.PlatformBase = gqlSrv.URL
	c.APIBase = gqlSrv.URL // ListLocations also lives under APIBase
	c.mu.Lock()
	c.token = &TokenSet{AccessToken: "test-token", ExpiresAt: time.Now().Add(time.Hour)}
	c.mu.Unlock()

	// We need ListLocations to return the location so locationHiloID lookup
	// works. The multipartStreamServer responds to ANY POST. Override it
	// with a custom server that handles BOTH the location list (GET on
	// /Automation/v1/api/Locations) AND the multipart subscription (POST
	// on /api/digital-twin/v3/graphql).
	mux := http.NewServeMux()
	mux.HandleFunc("/Automation/v1/api/Locations", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`[{"id":12345,"locationHiloId":"urn:hilo:crm:test:0","name":"Test"}]`))
	})
	mux.HandleFunc("/api/digital-twin/v3/graphql", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", `multipart/mixed; boundary=graphql; subscriptionSpec=1.0`)
		w.WriteHeader(http.StatusOK)
		fl, _ := w.(http.Flusher)
		fmt.Fprintf(w, "--graphql\r\nContent-Type: application/json\r\n\r\n%s\r\n",
			`{"data":{"onAnyDeviceUpdated":{"operationId":"op-fail","device":{"id":42},"status":"REJECTED","statusReason":"INVALID_ARGUMENT"}}}`)
		// Write the closing boundary so the multipart reader can complete
		// reading the first (and only) part without blocking.
		fmt.Fprint(w, "--graphql--\r\n")
		if fl != nil {
			fl.Flush()
		}
		<-r.Context().Done()
	})
	combinedSrv := httptest.NewServer(mux)
	defer combinedSrv.Close()
	c.PlatformBase = combinedSrv.URL
	c.APIBase = combinedSrv.URL

	// Prime the HiloID cache so ensureGraphQLSubscription can resolve the
	// location without a ListLocations round-trip.
	c.PrimeLocationHiloID(12345, "urn:hilo:crm:test:0")

	// DeviceHub fake (used as fallback if GraphQL unhealthy).
	fake := newFakeTransport()
	c.dialHubFn = func(ctx context.Context, k hubKind) (*hubConn, error) {
		h := newHubConn(k, "u", fake)
		go h.observeAndPropagate()
		return h, nil
	}
	fake.replyInvoke("SubscribeToLocation", nil, nil)
	fake.replyInvoke("UnsubscribeFromLocation", nil, nil)
	fake.setState(StateConnected)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	resultCh := make(chan *Operation, 1)
	errCh := make(chan error, 1)
	go func() {
		op, err := c.awaitOp(ctx, 12345, 42, AttrPower, "op-fail")
		resultCh <- op
		errCh <- err
	}()

	op := <-resultCh
	err := <-errCh
	if err != nil {
		t.Fatal(err)
	}
	if op.Status != OperationStatusRejected {
		t.Errorf("Status=%v, want Rejected (from GraphQL stream)", op.Status)
	}
	if op.StatusReason != OperationStatusReasonInvalidArgument {
		t.Errorf("StatusReason=%v, want InvalidArgument", op.StatusReason)
	}
}

func TestSetAttributeNoWaitReturnsImmediately(t *testing.T) {
	c := NewClient()
	gotPath := ""
	gotBody := []byte(nil)
	teardown := withTestServer(t, c, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotBody, _ = readAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`[{"operationId":"op-fire-and-forget"}]`))
	})
	defer teardown()

	// No fake transport setup — NoWait should not need it.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	opID, err := c.SetAttributeNoWait(ctx, 12345, 67890, AttrTargetTemperature, 21.5)
	if err != nil {
		t.Fatal(err)
	}
	if opID != "op-fire-and-forget" {
		t.Errorf("opID=%q", opID)
	}
	if !strings.HasSuffix(gotPath, "/Devices/67890/Attributes") {
		t.Errorf("path=%s", gotPath)
	}
	if !strings.Contains(string(gotBody), `"TargetTemperature":21.5`) {
		t.Errorf("body=%s", gotBody)
	}
}

func TestSetAttributesNoWaitReturnsAllOpIDs(t *testing.T) {
	c := NewClient()
	teardown := withTestServer(t, c, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`[{"operationId":"op-a"},{"operationId":"op-b"}]`))
	})
	defer teardown()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	opIDs, err := c.SetAttributesNoWait(ctx, 12345, 67890, map[AttributeType]any{
		AttrTargetTemperature: 21.5,
		AttrThermostatMode:    "Manual",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(opIDs) != 2 {
		t.Fatalf("got %d opIDs, want 2: %v", len(opIDs), opIDs)
	}
	if opIDs[0] != "op-a" || opIDs[1] != "op-b" {
		t.Errorf("opIDs=%v, want [op-a op-b]", opIDs)
	}
}

func TestSetBatchAttributesNoWaitReturnsAllOpIDs(t *testing.T) {
	c := NewClient()
	gotPath := ""
	teardown := withTestServer(t, c, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`[{"operationId":"op-x"},{"operationId":"op-y"}]`))
	})
	defer teardown()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	opIDs, err := c.SetBatchAttributesNoWait(ctx, 12345, []AttributeWrite{
		{DeviceID: 1, AttributeType: AttrPower, Value: 100},
		{DeviceID: 2, AttributeType: AttrLevel, Value: 50},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(gotPath, "/Devices/BatchAttributes") {
		t.Errorf("path=%s", gotPath)
	}
	if len(opIDs) != 2 || opIDs[0] != "op-x" || opIDs[1] != "op-y" {
		t.Errorf("opIDs=%v", opIDs)
	}
}

func TestSetAttributeAwaitTimeoutReturnsErrUnknown(t *testing.T) {
	c := NewClient()
	teardown := withTestServer(t, c, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`[{"operationId":"op-no-echo"}]`))
	})
	defer teardown()

	fake := newFakeTransport()
	c.dialHubFn = func(ctx context.Context, k hubKind) (*hubConn, error) {
		h := newHubConn(k, "u", fake)
		go h.observeAndPropagate()
		return h, nil
	}
	fake.replyInvoke("SubscribeToLocation", nil, nil)
	fake.replyInvoke("UnsubscribeFromLocation", nil, nil)
	fake.setState(StateConnected)

	// No echo will arrive — short ctx forces timeout.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	op, err := c.SetAttribute(ctx, 12345, 67890, AttrTargetTemperature, 21.5)
	if !errors.Is(err, ErrOperationStatusUnknown) {
		t.Errorf("err=%v, want wrap of ErrOperationStatusUnknown", err)
	}
	if op == nil {
		t.Fatal("expected non-nil *Operation, got nil")
	}
	if op.OperationID != "op-no-echo" {
		t.Errorf("op.OperationID=%q, want op-no-echo", op.OperationID)
	}
	if op.Status != OperationStatusReport {
		t.Errorf("op.Status=%v, want OperationStatusReport", op.Status)
	}
}
