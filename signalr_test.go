package hilo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestStreamDeliversUpdate(t *testing.T) {
	s := newStream[int](4)
	s.deliver(42)
	select {
	case v := <-s.Updates():
		if v != 42 {
			t.Fatalf("got %d, want 42", v)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for update")
	}
}

func TestStreamDropsOldestWhenBufferFull(t *testing.T) {
	s := newStream[int](2)
	s.deliver(1)
	s.deliver(2)
	s.deliver(3) // should drop 1
	want := []int{2, 3}
	got := []int{}
	for i := 0; i < 2; i++ {
		select {
		case v := <-s.Updates():
			got = append(got, v)
		case <-time.After(time.Second):
			t.Fatalf("timeout, got so far: %v", got)
		}
	}
	if got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("got %v, want %v", got, want)
	}
	if d := s.Dropped(); d != 1 {
		t.Fatalf("Dropped()=%d, want 1", d)
	}
}

func TestStreamCloseSetsErr(t *testing.T) {
	s := newStream[int](4)
	wantErr := errors.New("boom")
	s.close(wantErr)
	if _, ok := <-s.Updates(); ok {
		t.Fatal("Updates should be closed after close")
	}
	if _, ok := <-s.State(); ok {
		t.Fatal("State should be closed after close")
	}
	if !errors.Is(s.Err(), wantErr) {
		t.Fatalf("Err()=%v, want %v", s.Err(), wantErr)
	}
}

func TestStreamCloseIsIdempotent(t *testing.T) {
	s := newStream[int](4)
	s.close(nil)
	s.close(errors.New("late")) // should not panic, should not overwrite
	if s.Err() != nil {
		t.Fatalf("Err()=%v after close(nil)+close(err), want nil (first close wins)", s.Err())
	}
}

func TestStreamStatePushesCoalesced(t *testing.T) {
	s := newStream[int](4)
	s.pushState(StateConnecting)
	s.pushState(StateConnected)
	got := []ConnState{}
loop:
	for i := 0; i < 2; i++ {
		select {
		case st := <-s.State():
			got = append(got, st)
		case <-time.After(100 * time.Millisecond):
			break loop
		}
	}
	if len(got) == 0 {
		t.Fatal("State channel had no events")
	}
	// The last value MUST be Connected; earlier values may have been dropped.
	if got[len(got)-1] != StateConnected {
		t.Fatalf("last state = %v, want StateConnected", got[len(got)-1])
	}
}

func TestStreamConcurrentDeliverAndClose(t *testing.T) {
	s := newStream[int](16)
	var wg sync.WaitGroup
	var delivered atomic.Int64
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				s.deliver(j)
				delivered.Add(1)
			}
		}()
	}
	go func() {
		time.Sleep(10 * time.Millisecond)
		s.close(nil)
	}()
	wg.Wait()
	// Drain
	for range s.Updates() {
	}
}

var _ srTransport = (*fakeTransport)(nil)

type fakeTransport struct {
	mu          sync.Mutex
	state       ConnState
	stateLog    []ConnState // all state transitions; replayed to late-registering observers
	observers   []chan<- ConnState
	invokeReply map[string]invokeResult
	invokeLog   []invokeCall
	err         error
}

type invokeCall struct {
	method string
	args   []any
}

func newFakeTransport() *fakeTransport {
	return &fakeTransport{
		state:       StateConnecting,
		invokeReply: map[string]invokeResult{},
	}
}

func (f *fakeTransport) Start() {}
func (f *fakeTransport) Stop()  {}

func (f *fakeTransport) State() ConnState {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.state
}

func (f *fakeTransport) Observe(ch chan<- ConnState) func() {
	f.mu.Lock()
	replay := append([]ConnState{}, f.stateLog...)
	f.observers = append(f.observers, ch)
	f.mu.Unlock()
	// Replay past transitions so late-registering observers see the full history.
	for _, s := range replay {
		select {
		case ch <- s:
		default:
		}
	}
	return func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		for i, c := range f.observers {
			if c == ch {
				f.observers = append(f.observers[:i], f.observers[i+1:]...)
				return
			}
		}
	}
}

func (f *fakeTransport) Invoke(method string, args ...any) <-chan invokeResult {
	ch := make(chan invokeResult, 1)
	f.mu.Lock()
	f.invokeLog = append(f.invokeLog, invokeCall{method, args})
	res, ok := f.invokeReply[method]
	f.mu.Unlock()
	if ok {
		ch <- res
	}
	close(ch)
	return ch
}

func (f *fakeTransport) Send(method string, args ...any) <-chan error {
	ch := make(chan error, 1)
	f.mu.Lock()
	f.invokeLog = append(f.invokeLog, invokeCall{method, args})
	f.mu.Unlock()
	close(ch)
	return ch
}

func (f *fakeTransport) Err() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.err
}

// Test helpers (non-interface methods).

func (f *fakeTransport) setState(s ConnState) {
	f.mu.Lock()
	f.state = s
	f.stateLog = append(f.stateLog, s)
	obs := append([]chan<- ConnState{}, f.observers...)
	f.mu.Unlock()
	for _, ch := range obs {
		select {
		case ch <- s:
		default:
		}
	}
}

func (f *fakeTransport) replyInvoke(method string, value any, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.invokeReply[method] = invokeResult{value: value, err: err}
}

func (f *fakeTransport) observe(ch chan<- ConnState) { f.Observe(ch) }
func (f *fakeTransport) invoke(method string, args ...any) <-chan invokeResult {
	return f.Invoke(method, args...)
}

func TestFakeTransportObserveStateAndInvoke(t *testing.T) {
	f := newFakeTransport()
	stateCh := make(chan ConnState, 4)
	f.observe(stateCh)
	f.setState(StateConnected)
	select {
	case s := <-stateCh:
		if s != StateConnected {
			t.Fatalf("got %v, want StateConnected", s)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}

	// Simulate a server response for an Invoke.
	f.replyInvoke("SubscribeToEventList", nil, nil)
	resCh := f.invoke("SubscribeToEventList", "loc1")
	select {
	case res := <-resCh:
		if res.err != nil {
			t.Fatalf("invoke err: %v", res.err)
		}
	case <-time.After(time.Second):
		t.Fatal("invoke timeout")
	}
}

func TestHubConnDispatchToMatchingSink(t *testing.T) {
	f := newFakeTransport()
	h := newHubConn(hubDevice, "https://example/DeviceHub", f)

	got := make(chan string, 4)
	sink := h.addSink("Method1", nil, func(args []json.RawMessage) {
		got <- string(args[0])
	})
	defer h.removeSinks(sink)

	h.dispatch("Method1", []json.RawMessage{json.RawMessage(`"hello"`)})

	select {
	case v := <-got:
		if v != `"hello"` {
			t.Fatalf("got %q, want %q", v, `"hello"`)
		}
	case <-time.After(time.Second):
		t.Fatal("sink did not receive")
	}
}

func TestHubConnDispatchHonorsFilter(t *testing.T) {
	f := newFakeTransport()
	h := newHubConn(hubDevice, "u", f)

	got := make(chan struct{}, 4)
	sink := h.addSink("M",
		func(args []json.RawMessage) bool {
			var s string
			_ = json.Unmarshal(args[0], &s)
			return s == "want"
		},
		func(args []json.RawMessage) { got <- struct{}{} },
	)
	defer h.removeSinks(sink)

	h.dispatch("M", []json.RawMessage{json.RawMessage(`"skip"`)})
	h.dispatch("M", []json.RawMessage{json.RawMessage(`"want"`)})

	select {
	case <-got:
	case <-time.After(time.Second):
		t.Fatal("filtered sink did not receive matching arg")
	}
	select {
	case <-got:
		t.Fatal("filtered sink received non-matching arg")
	case <-time.After(20 * time.Millisecond):
	}
}

func TestHubConnRemoveSinks(t *testing.T) {
	f := newFakeTransport()
	h := newHubConn(hubDevice, "u", f)
	got := make(chan struct{}, 4)
	sink := h.addSink("M", nil, func([]json.RawMessage) { got <- struct{}{} })
	h.removeSinks(sink)
	h.dispatch("M", nil)
	select {
	case <-got:
		t.Fatal("removed sink still received")
	case <-time.After(20 * time.Millisecond):
	}
}

func TestHubConnResubscribesOnReconnect(t *testing.T) {
	f := newFakeTransport()
	h := newHubConn(hubChallenge, "u", f)
	defer h.close()

	rejoined := make(chan string, 4)
	sink := h.addSinkWithRejoin("EventListInitialValuesReceived",
		nil,
		func([]json.RawMessage) {},
		func() error {
			rejoined <- "list"
			return nil
		},
	)
	defer h.removeSinks(sink)

	go h.observeAndPropagate()

	// Simulate the lifecycle: Connected → Reconnecting → Connected.
	f.setState(StateConnected)
	f.setState(StateReconnecting)
	f.setState(StateConnected)

	select {
	case v := <-rejoined:
		if v != "list" {
			t.Fatalf("got %q", v)
		}
	case <-time.After(time.Second):
		t.Fatal("rejoin not invoked")
	}
}

func TestSignalRClientInvocationCorrelation(t *testing.T) {
	// We don't have a real WS here, so wire up the bare minimum manually:
	// build a *signalRClient with a sendCh we control, call Invoke, then
	// hand-craft a Completion frame and verify the channel resolves.
	innerCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := &signalRClient{
		ctx:      innerCtx,
		cancel:   cancel,
		handlers: map[string]func([]json.RawMessage){},
		pending:  map[string]chan invokeResult{},
		state:    StateConnected,
		sendCh:   make(chan []byte, 4),
	}

	out := c.Invoke("Foo", "arg1")

	var sent []byte
	select {
	case sent = <-c.sendCh:
	case <-time.After(time.Second):
		t.Fatal("Invoke did not write to sendCh")
	}
	var msg struct {
		InvocationID string `json:"invocationId"`
		Target       string `json:"target"`
	}
	if err := json.Unmarshal(sent, &msg); err != nil {
		t.Fatal(err)
	}
	if msg.Target != "Foo" {
		t.Errorf("target=%s", msg.Target)
	}

	// Synthesize a server completion.
	completion := []byte(`{"type":3,"invocationId":"` + msg.InvocationID + `","result":42}`)
	c.handleFrame(completion)

	select {
	case res := <-out:
		if res.err != nil {
			t.Fatalf("err: %v", res.err)
		}
		if string(res.value.(json.RawMessage)) != "42" {
			t.Errorf("value=%v", res.value)
		}
	case <-time.After(time.Second):
		t.Fatal("invoke result not delivered")
	}
}

func TestAcquireReleaseHubSharesConnection(t *testing.T) {
	c := NewClient()
	dialed := 0
	c.dialHubFn = func(ctx context.Context, k hubKind) (*hubConn, error) {
		dialed++
		return newHubConn(k, "u", newFakeTransport()), nil
	}

	ctx := context.Background()
	h1, err := c.acquireHub(ctx, hubDevice)
	if err != nil {
		t.Fatal(err)
	}
	h2, err := c.acquireHub(ctx, hubDevice)
	if err != nil {
		t.Fatal(err)
	}
	if h1 != h2 {
		t.Errorf("acquireHub returned different hubConn instances")
	}
	if dialed != 1 {
		t.Errorf("dialed %d times, want 1", dialed)
	}

	c.releaseHub(hubDevice)
	if c.hubs[hubDevice] == nil {
		t.Errorf("hub closed after first release")
	}
	c.releaseHub(hubDevice)
	if c.hubs[hubDevice] != nil {
		t.Errorf("hub still alive after second release")
	}
}

func TestSubscriptionHelperLifecycle(t *testing.T) {
	c := NewClient()
	fake := newFakeTransport()
	c.dialHubFn = func(ctx context.Context, k hubKind) (*hubConn, error) {
		h := newHubConn(k, "u", fake)
		go h.observeAndPropagate()
		return h, nil
	}
	fake.replyInvoke("SubscribeToEventList", nil, nil)
	fake.replyInvoke("UnsubscribeFromEventList", nil, nil)
	fake.setState(StateConnected)

	ctx, cancel := context.WithCancel(context.Background())
	stream := newStream[string](4)
	sinks := []*subSink{}

	err := c.startSubscription(ctx, hubChallenge, stream, &sinks,
		func(h *hubConn) {
			sinks = append(sinks, h.addSinkWithRejoin(
				"EventListInitialValuesReceived",
				nil,
				func(args []json.RawMessage) {
					var s string
					_ = json.Unmarshal(args[0], &s)
					stream.deliver(s)
				},
				func() error { return nil },
			))
		},
		"SubscribeToEventList", []any{"loc1"},
		"UnsubscribeFromEventList", []any{"loc1"},
	)
	if err != nil {
		t.Fatal(err)
	}

	// Simulate a server push.
	c.hubs[hubChallenge].dispatch("EventListInitialValuesReceived",
		[]json.RawMessage{json.RawMessage(`"hello"`)})
	select {
	case v := <-stream.Updates():
		if v != "hello" {
			t.Fatalf("got %q", v)
		}
	case <-time.After(time.Second):
		t.Fatal("no update")
	}

	// Cancel and verify Unsubscribe was sent and stream closed.
	cancel()
	select {
	case <-stream.Updates():
	case <-time.After(time.Second):
		t.Fatal("Updates not closed after cancel")
	}

	// Verify the server-side Unsubscribe was invoked.
	found := false
	fake.mu.Lock()
	for _, ic := range fake.invokeLog {
		if ic.method == "UnsubscribeFromEventList" {
			found = true
			break
		}
	}
	fake.mu.Unlock()
	if !found {
		t.Fatal("UnsubscribeFromEventList not invoked")
	}
}

func TestDeviceListDecodeSnapshot(t *testing.T) {
	raw, err := os.ReadFile("testdata/signalr/device_hub_DeviceListInitialValuesReceived.json")
	if err != nil {
		t.Skipf("fixture not present: %v", err)
	}
	if isSyntheticFixture(raw) {
		t.Skip("synthetic fixture; capture a real one to enable this test")
	}
	var args []json.RawMessage
	if err := json.Unmarshal(raw, &args); err != nil {
		t.Fatal(err)
	}
	devices, err := decodeHubDevices(args)
	if err != nil {
		t.Fatal(err)
	}
	if len(devices) == 0 {
		t.Error("no devices decoded")
	}
	// First device should have the basic fields populated.
	d := devices[0]
	if d.ID == 0 {
		t.Errorf("ID=0")
	}
	if d.HiloID == "" {
		t.Errorf("HiloID empty")
	}
	if d.Type == "" {
		t.Errorf("Type empty")
	}
}

func TestDeviceListUpdateKindString(t *testing.T) {
	if got := DeviceListSnapshot.String(); got != "Snapshot" {
		t.Errorf("Snapshot.String() = %q", got)
	}
	if got := DeviceListDelta.String(); got != "Delta" {
		t.Errorf("Delta.String() = %q", got)
	}
	if got := DeviceListAdded.String(); got != "Added" {
		t.Errorf("Added.String() = %q", got)
	}
	if got := DeviceListDeleted.String(); got != "Deleted" {
		t.Errorf("Deleted.String() = %q", got)
	}
}

func isSyntheticFixture(raw []byte) bool {
	var probe struct {
		Synthetic bool `json:"_synthetic"`
	}
	_ = json.Unmarshal(raw, &probe)
	return probe.Synthetic
}

func TestMarkNotificationReadInvokesHub(t *testing.T) {
	c := NewClient()
	fake := newFakeTransport()
	c.dialHubFn = func(ctx context.Context, k hubKind) (*hubConn, error) {
		h := newHubConn(k, "u", fake)
		go h.observeAndPropagate()
		return h, nil
	}
	fake.replyInvoke("MarkAsRead", nil, nil)
	fake.setState(StateConnected)

	if err := c.MarkNotificationRead(context.Background(), "notif-1"); err != nil {
		t.Fatal(err)
	}
	found := false
	fake.mu.Lock()
	for _, ic := range fake.invokeLog {
		if ic.method == "MarkAsRead" && len(ic.args) == 1 && ic.args[0] == "notif-1" {
			found = true
		}
	}
	fake.mu.Unlock()
	if !found {
		t.Fatalf("MarkAsRead not invoked with notif-1: log=%v", fake.invokeLog)
	}
}

func TestMarkAllNotificationsReadInvokesHub(t *testing.T) {
	c := NewClient()
	fake := newFakeTransport()
	c.dialHubFn = func(ctx context.Context, k hubKind) (*hubConn, error) {
		h := newHubConn(k, "u", fake)
		go h.observeAndPropagate()
		return h, nil
	}
	fake.replyInvoke("MarkAllAsRead", nil, nil)
	fake.setState(StateConnected)

	if err := c.MarkAllNotificationsRead(context.Background()); err != nil {
		t.Fatal(err)
	}
	found := false
	fake.mu.Lock()
	for _, ic := range fake.invokeLog {
		if ic.method == "MarkAllAsRead" {
			found = true
		}
	}
	fake.mu.Unlock()
	if !found {
		t.Fatal("MarkAllAsRead not invoked")
	}
}

func TestNotificationEventKindString(t *testing.T) {
	if NotifSnapshot.String() != "Snapshot" {
		t.Errorf("NotifSnapshot.String() = %q", NotifSnapshot.String())
	}
	if NotifAdded.String() != "Added" {
		t.Errorf("NotifAdded.String() = %q", NotifAdded.String())
	}
	if NotifUpdated.String() != "Updated" {
		t.Errorf("NotifUpdated.String() = %q", NotifUpdated.String())
	}
}

func TestEventListDecodeSnapshot(t *testing.T) {
	raw, err := os.ReadFile("testdata/signalr/challenge_hub_EventListInitialValuesReceived.json")
	if err != nil {
		t.Skipf("fixture not present: %v", err)
	}
	if isSyntheticFixture(raw) {
		t.Skip("synthetic")
	}
	var args []json.RawMessage
	if err := json.Unmarshal(raw, &args); err != nil {
		t.Fatal(err)
	}
	upd, err := decodeEventList(args, EventListSnapshot, "")
	if err != nil {
		t.Fatal(err)
	}
	if upd.Kind != EventListSnapshot {
		t.Errorf("Kind=%v", upd.Kind)
	}
}

func TestEventListUpdateKindString(t *testing.T) {
	if EventListSnapshot.String() != "Snapshot" {
		t.Errorf("Snapshot.String()=%q", EventListSnapshot.String())
	}
	if EventListAdded.String() != "Added" {
		t.Errorf("Added.String()=%q", EventListAdded.String())
	}
}

func TestSubscribeEventListPassesObjectArg(t *testing.T) {
	c := NewClient()
	fake := newFakeTransport()
	c.dialHubFn = func(ctx context.Context, k hubKind) (*hubConn, error) {
		h := newHubConn(k, "u", fake)
		go h.observeAndPropagate()
		return h, nil
	}
	fake.replyInvoke("SubscribeToEventList", nil, nil)
	fake.replyInvoke("UnsubscribeFromEventList", nil, nil)
	fake.setState(StateConnected)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, err := c.SubscribeEventList(ctx, HiloID("urn:hilo:crm:1234"))
	if err != nil {
		t.Fatal(err)
	}

	// Verify the SubscribeToEventList Invoke was called with an object,
	// not a bare string.
	fake.mu.Lock()
	defer fake.mu.Unlock()
	for _, ic := range fake.invokeLog {
		if ic.method == "SubscribeToEventList" {
			if len(ic.args) != 1 {
				t.Fatalf("SubscribeToEventList: %d args, want 1", len(ic.args))
			}
			m, ok := ic.args[0].(map[string]string)
			if !ok {
				t.Fatalf("SubscribeToEventList arg not map[string]string: %T %v", ic.args[0], ic.args[0])
			}
			if m["locationHiloId"] != "urn:hilo:crm:1234" {
				t.Errorf("locationHiloId=%q", m["locationHiloId"])
			}
			return
		}
	}
	t.Fatal("SubscribeToEventList not invoked")
}

func TestEventCHDetailsDecodeWithoutConsumption(t *testing.T) {
	raw, err := os.ReadFile("testdata/signalr/challenge_hub_EventCHDetailsInitialValuesReceived.json")
	if err != nil {
		t.Skipf("fixture not present: %v", err)
	}
	if isSyntheticFixture(raw) {
		t.Skip("synthetic")
	}
	var args []json.RawMessage
	if err := json.Unmarshal(raw, &args); err != nil {
		t.Fatal(err)
	}
	upd, err := decodeEventCHDetails(args, true, false)
	if err != nil {
		t.Fatal(err)
	}
	if !upd.Initial {
		t.Error("Initial=false")
	}
	if upd.Consumption != nil {
		t.Error("expected no Consumption on Initial details")
	}
}

func TestSubscribeEventCHDetailsPassesObjectArgs(t *testing.T) {
	c := NewClient()
	fake := newFakeTransport()
	c.dialHubFn = func(ctx context.Context, k hubKind) (*hubConn, error) {
		h := newHubConn(k, "u", fake)
		go h.observeAndPropagate()
		return h, nil
	}
	fake.replyInvoke("SubscribeToEventCH", nil, nil)
	fake.replyInvoke("UnsubscribeFromEventCH", nil, nil)
	fake.setState(StateConnected)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, err := c.SubscribeEventCHDetails(ctx, HiloID("urn:hilo:crm:1234"), "evt-9")
	if err != nil {
		t.Fatal(err)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	for _, ic := range fake.invokeLog {
		if ic.method == "SubscribeToEventCH" {
			if len(ic.args) != 1 {
				t.Fatalf("%d args, want 1", len(ic.args))
			}
			m, ok := ic.args[0].(map[string]string)
			if !ok {
				t.Fatalf("arg type %T", ic.args[0])
			}
			if m["locationHiloId"] != "urn:hilo:crm:1234" || m["eventId"] != "evt-9" {
				t.Errorf("args=%v", m)
			}
			return
		}
	}
	t.Fatal("SubscribeToEventCH not invoked")
}

func TestRequestEventCHConsumption(t *testing.T) {
	c := NewClient()
	fake := newFakeTransport()
	c.dialHubFn = func(ctx context.Context, k hubKind) (*hubConn, error) {
		h := newHubConn(k, "u", fake)
		go h.observeAndPropagate()
		return h, nil
	}
	fake.replyInvoke("RequestEventCHConsumptionUpdate", nil, nil)
	fake.setState(StateConnected)

	if err := c.RequestEventCHConsumption(context.Background(), HiloID("urn:hilo:crm:1234"), "evt-9"); err != nil {
		t.Fatal(err)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	for _, ic := range fake.invokeLog {
		if ic.method == "RequestEventCHConsumptionUpdate" {
			m, ok := ic.args[0].(map[string]string)
			if !ok || m["locationHiloId"] != "urn:hilo:crm:1234" || m["eventId"] != "evt-9" {
				t.Errorf("bad args: %v", ic.args)
			}
			return
		}
	}
	t.Fatal("RequestEventCHConsumptionUpdate not invoked")
}

func TestEventFlexDetailsDecodeUpdate(t *testing.T) {
	raw, err := os.ReadFile("testdata/signalr/challenge_hub_EventFlexDetailsUpdatedValuesReceived.json")
	if err != nil {
		t.Skipf("fixture not present: %v", err)
	}
	if isSyntheticFixture(raw) {
		t.Skip("synthetic")
	}
	var args []json.RawMessage
	if err := json.Unmarshal(raw, &args); err != nil {
		t.Fatal(err)
	}
	upd, err := decodeEventFlexDetails(args, false, false)
	if err != nil {
		t.Fatal(err)
	}
	if upd.Initial {
		t.Error("Initial=true on Updated push")
	}
}

func TestSubscribeEventFlexDetailsPassesObjectArgs(t *testing.T) {
	c := NewClient()
	fake := newFakeTransport()
	c.dialHubFn = func(ctx context.Context, k hubKind) (*hubConn, error) {
		h := newHubConn(k, "u", fake)
		go h.observeAndPropagate()
		return h, nil
	}
	fake.replyInvoke("SubscribeToEventFlex", nil, nil)
	fake.replyInvoke("UnsubscribeFromEventFlex", nil, nil)
	fake.setState(StateConnected)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, err := c.SubscribeEventFlexDetails(ctx, HiloID("urn:hilo:crm:1234"), "flx-7")
	if err != nil {
		t.Fatal(err)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	for _, ic := range fake.invokeLog {
		if ic.method == "SubscribeToEventFlex" {
			m, ok := ic.args[0].(map[string]string)
			if !ok || m["locationHiloId"] != "urn:hilo:crm:1234" || m["eventId"] != "flx-7" {
				t.Errorf("bad args: %v", ic.args)
			}
			return
		}
	}
	t.Fatal("SubscribeToEventFlex not invoked")
}

func TestRequestEventFlexConsumption(t *testing.T) {
	c := NewClient()
	fake := newFakeTransport()
	c.dialHubFn = func(ctx context.Context, k hubKind) (*hubConn, error) {
		h := newHubConn(k, "u", fake)
		go h.observeAndPropagate()
		return h, nil
	}
	fake.replyInvoke("RequestEventFlexConsumptionUpdate", nil, nil)
	fake.setState(StateConnected)

	if err := c.RequestEventFlexConsumption(context.Background(), HiloID("urn:hilo:crm:1234"), "flx-7"); err != nil {
		t.Fatal(err)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	for _, ic := range fake.invokeLog {
		if ic.method == "RequestEventFlexConsumptionUpdate" {
			m, ok := ic.args[0].(map[string]string)
			if !ok || m["locationHiloId"] != "urn:hilo:crm:1234" || m["eventId"] != "flx-7" {
				t.Errorf("bad args: %v", ic.args)
			}
			return
		}
	}
	t.Fatal("RequestEventFlexConsumptionUpdate not invoked")
}

func TestReconnectingClientReplaysHandlers(t *testing.T) {
	dials := 0
	dialFn := func(ctx context.Context) (*signalRClient, error) {
		dials++
		// Construct a minimal *signalRClient suitable for unit testing,
		// without a real WebSocket. Use a shared sendCh and ctx.
		innerCtx, cancel := context.WithCancel(ctx)
		c := &signalRClient{
			ctx:      innerCtx,
			cancel:   cancel,
			handlers: map[string]func([]json.RawMessage){},
			pending:  map[string]chan invokeResult{},
			state:    StateConnected,
			sendCh:   make(chan []byte, 4),
		}
		return c, nil
	}
	rc, err := newReconnectingClient(context.Background(), dialFn, 3, 10*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Stop()

	called := make(chan struct{}, 4)
	rc.OnInvocation("Foo", func([]json.RawMessage) { called <- struct{}{} })

	// Fire a synthetic incoming Foo into the current underlying client.
	rc.mu.Lock()
	cur := rc.current
	rc.mu.Unlock()
	cur.handleFrame([]byte(`{"type":1,"target":"Foo","arguments":[]}`))
	select {
	case <-called:
	case <-time.After(time.Second):
		t.Fatal("first Foo not delivered")
	}

	// Force a disconnect to trigger reconnect.
	cur.markClosed(fmt.Errorf("simulated drop"))
	// Wait for reconnect to occur.
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	for {
		rc.mu.Lock()
		newCur := rc.current
		rc.mu.Unlock()
		if newCur != nil && newCur != cur {
			cur = newCur
			break
		}
		select {
		case <-deadline.C:
			t.Fatal("reconnect did not produce a new client")
		case <-time.After(20 * time.Millisecond):
		}
	}

	// Fire the same target on the new client; handler should still fire.
	cur.handleFrame([]byte(`{"type":1,"target":"Foo","arguments":[]}`))
	select {
	case <-called:
	case <-time.After(time.Second):
		t.Fatal("Foo handler not replayed on reconnect")
	}

	if dials < 2 {
		t.Errorf("expected at least 2 dials, got %d", dials)
	}
}

func TestNotificationSnapshotDeliversAllItems(t *testing.T) {
	c := NewClient()
	fake := newFakeTransport()
	c.dialHubFn = func(ctx context.Context, k hubKind) (*hubConn, error) {
		h := newHubConn(k, "u", fake)
		go h.observeAndPropagate()
		return h, nil
	}
	fake.setState(StateConnected)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream, err := c.SubscribeNotifications(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Push a synthetic three-item snapshot directly through the hub's dispatch.
	c.hubs[hubNotification].dispatch("NotificationsListReceived",
		[]json.RawMessage{json.RawMessage(`[{"id":"a"},{"id":"b"},{"id":"c"}]`)})

	got := []string{}
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	for len(got) < 3 {
		select {
		case ev := <-stream.Updates():
			if ev.Kind == NotifSnapshot {
				got = append(got, ev.Notification.ID)
			}
		case <-deadline.C:
			t.Fatalf("only got %d items: %v", len(got), got)
		}
	}
	if got[0] != "a" || got[1] != "b" || got[2] != "c" {
		t.Fatalf("got %v, want [a b c]", got)
	}
}

func TestStartSubscriptionClosesOnTerminalDisconnect(t *testing.T) {
	c := NewClient()
	fake := newFakeTransport()
	c.dialHubFn = func(ctx context.Context, k hubKind) (*hubConn, error) {
		h := newHubConn(k, "u", fake)
		go h.observeAndPropagate()
		return h, nil
	}
	fake.replyInvoke("SubscribeToEventList", nil, nil)
	fake.replyInvoke("UnsubscribeFromEventList", nil, nil)
	fake.setState(StateConnected)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream, err := c.SubscribeEventList(ctx, HiloID("urn:hilo:crm:1234"))
	if err != nil {
		t.Fatal(err)
	}

	// Simulate the transport going Disconnected without consumer cancel.
	fake.setState(StateDisconnected)

	// Updates() must close within a short window.
	select {
	case _, ok := <-stream.Updates():
		if ok {
			// might receive a queued state event first; allow drain.
			for range stream.Updates() {
			}
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Updates did not close within 2s of StateDisconnected")
	}

	// Err() should be set to a non-nil terminal error.
	if stream.Err() == nil {
		t.Error("Err() is nil after terminal disconnect")
	}
}
