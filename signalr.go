package hilo

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
)

// ConnState reports the transport state of a hub connection. Streams expose
// it via State() so consumers can detect possible gaps across reconnects.
type ConnState int

const (
	StateConnecting ConnState = iota
	StateConnected
	StateReconnecting
	StateDisconnected
)

func (s ConnState) String() string {
	switch s {
	case StateConnecting:
		return "Connecting"
	case StateConnected:
		return "Connected"
	case StateReconnecting:
		return "Reconnecting"
	case StateDisconnected:
		return "Disconnected"
	default:
		return "Unknown"
	}
}

// Stream is the typed event stream returned by every Subscribe* method.
// Updates is the domain channel; State carries transport state changes;
// Err returns the terminal error after both channels close.
//
// Lifecycle: a Stream is alive until the context passed to its
// originating Subscribe* call is cancelled, at which point both Updates
// and State channels close and Err returns ctx.Err() (or the underlying
// transport failure). Callers must drain Updates or cancel the context
// to avoid leaking the dispatch goroutine; once Updates closes there is
// nothing further to release.
//
// All accessor methods (Updates, State, Err, Dropped) are safe to call
// concurrently from multiple goroutines.
type Stream[T any] struct {
	updates chan T
	state   chan ConnState
	dropped atomic.Uint64
	err     atomic.Pointer[error]
	closed  atomic.Bool
	mu      sync.RWMutex
}

func newStream[T any](buf int) *Stream[T] {
	if buf < 1 {
		buf = 1
	}
	return &Stream[T]{
		updates: make(chan T, buf),
		state:   make(chan ConnState, 4),
	}
}

// Updates returns the typed event channel. Closed when the subscription ends.
func (s *Stream[T]) Updates() <-chan T { return s.updates }

// State returns coalesced connection-state changes.
func (s *Stream[T]) State() <-chan ConnState { return s.state }

// Err returns the terminal error after Updates closes (nil if none).
func (s *Stream[T]) Err() error {
	if p := s.err.Load(); p != nil {
		return *p
	}
	return nil
}

// Dropped returns the cumulative count of Updates events discarded under
// back-pressure (newest-wins).
func (s *Stream[T]) Dropped() uint64 { return s.dropped.Load() }

// deliver is called by hub dispatch to push a typed event. Newest-wins:
// if the buffer is full, drop the oldest and try again.
func (s *Stream[T]) deliver(v T) {
	if s.closed.Load() {
		return
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed.Load() {
		return
	}
	select {
	case s.updates <- v:
		return
	default:
	}
	// Drain one and retry.
	select {
	case <-s.updates:
		s.dropped.Add(1)
	default:
	}
	select {
	case s.updates <- v:
	default:
		// Producer beat us; treat the new value as dropped instead.
		s.dropped.Add(1)
	}
}

// pushState delivers a state change. Newest-wins on the State channel too.
func (s *Stream[T]) pushState(st ConnState) {
	if s.closed.Load() {
		return
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed.Load() {
		return
	}
	select {
	case s.state <- st:
		return
	default:
	}
	select {
	case <-s.state:
	default:
	}
	select {
	case s.state <- st:
	default:
	}
}

// close closes Updates and State. The first call wins for Err(); subsequent
// calls are no-ops.
func (s *Stream[T]) close(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed.Swap(true) {
		return
	}
	if err != nil {
		s.err.Store(&err)
	}
	close(s.updates)
	close(s.state)
}

// srTransport is the subset of SignalR client behavior the SDK's hub
// lifecycle uses. The production implementation is the hand-rolled
// *signalRClient that speaks the SignalR JSON protocol over
// github.com/coder/websocket. This interface lets hubConn-level tests use a
// fake without a real WebSocket.
type srTransport interface {
	Start()
	Stop()
	State() ConnState
	Observe(ch chan<- ConnState) func() // returns cancel fn
	Invoke(method string, args ...any) <-chan invokeResult
	Send(method string, args ...any) <-chan error
	Err() error
}

// invokeResult is the SignalR completion-message payload our SDK exposes:
// the server's Result (json.RawMessage left for caller decoding) or an
// error if the invocation failed client-side or server-side.
type invokeResult struct {
	value any
	err   error
}

// hubKind enumerates the three Hilo SignalR hubs.
type hubKind int

const (
	hubDevice hubKind = iota
	hubChallenge
	hubNotification
)

func (k hubKind) String() string {
	switch k {
	case hubDevice:
		return "device"
	case hubChallenge:
		return "challenge"
	case hubNotification:
		return "notification"
	default:
		return "unknown"
	}
}

// subSink is one (server-method, optional filter, deliver) registration. A
// single subscription registers multiple sinks, one per server-pushed method.
type subSink struct {
	method  string
	filter  func(args []json.RawMessage) bool
	deliver func(args []json.RawMessage)
	rejoin  func() error // optional; called after a Reconnecting → Connected transition
}

type dispatchItem struct {
	method string
	args   []json.RawMessage
}

// hubConn owns one logical hub: the underlying srTransport, the sink
// registry, and the demux goroutine.
type hubConn struct {
	kind   hubKind
	url    string
	client srTransport

	mu       sync.Mutex
	refs     int
	sinks    map[string][]*subSink
	stateSub []chan<- ConnState
	state    ConnState

	dispatchCh chan dispatchItem
	closeCh    chan struct{}
	doneCh     chan struct{}
}

func newHubConn(kind hubKind, url string, client srTransport) *hubConn {
	h := &hubConn{
		kind:       kind,
		url:        url,
		client:     client,
		sinks:      map[string][]*subSink{},
		state:      StateConnecting,
		dispatchCh: make(chan dispatchItem, 256),
		closeCh:    make(chan struct{}),
		doneCh:     make(chan struct{}),
	}
	go h.demux()
	return h
}

func (h *hubConn) addSink(method string, filter func([]json.RawMessage) bool, deliver func([]json.RawMessage)) *subSink {
	return h.addSinkWithRejoin(method, filter, deliver, nil)
}

func (h *hubConn) addSinkWithRejoin(method string, filter func([]json.RawMessage) bool, deliver func([]json.RawMessage), rejoin func() error) *subSink {
	s := &subSink{method: method, filter: filter, deliver: deliver, rejoin: rejoin}
	h.mu.Lock()
	h.sinks[method] = append(h.sinks[method], s)
	h.mu.Unlock()
	return s
}

func (h *hubConn) removeSinks(sinks ...*subSink) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, s := range sinks {
		list := h.sinks[s.method]
		for i, existing := range list {
			if existing == s {
				h.sinks[s.method] = append(list[:i], list[i+1:]...)
				break
			}
		}
		if len(h.sinks[s.method]) == 0 {
			delete(h.sinks, s.method)
		}
	}
}

// dispatch is called by the receiver methods (one per server-pushed name).
// It enqueues to the demux channel non-blockingly; if the queue is full we
// drop the oldest item to keep the receiver goroutine responsive.
func (h *hubConn) dispatch(method string, args []json.RawMessage) {
	item := dispatchItem{method: method, args: args}
	select {
	case h.dispatchCh <- item:
		return
	default:
	}
	drained := false
	select {
	case <-h.dispatchCh:
		drained = true
	default:
	}
	if !drained {
		// Demux is keeping up; nothing was full enough to need eviction.
		// Try one last time; if it still fails, drop our new item.
		select {
		case h.dispatchCh <- item:
		default:
		}
		return
	}
	select {
	case h.dispatchCh <- item:
	default:
	}
}

func (h *hubConn) demux() {
	defer close(h.doneCh)
	for {
		select {
		case <-h.closeCh:
			return
		case item := <-h.dispatchCh:
			h.mu.Lock()
			sinks := append([]*subSink{}, h.sinks[item.method]...)
			h.mu.Unlock()
			for _, s := range sinks {
				if s.filter != nil && !s.filter(item.args) {
					continue
				}
				s.deliver(item.args)
			}
		}
	}
}

func (h *hubConn) close() {
	select {
	case <-h.closeCh:
		return
	default:
	}
	close(h.closeCh)
	<-h.doneCh
	h.client.Stop()
}

// observeAndPropagate reads the underlying client's state changes, updates
// hubConn.state, fans out to stateSub, and runs each sink's rejoin callback
// after a Reconnecting → Connected transition.
func (h *hubConn) observeAndPropagate() {
	stateCh := make(chan ConnState, 8)
	cancel := h.client.Observe(stateCh)
	defer cancel()

	prev := h.state
	for {
		select {
		case <-h.closeCh:
			return
		case s, ok := <-stateCh:
			if !ok {
				return
			}
			h.mu.Lock()
			h.state = s
			subs := append([]chan<- ConnState{}, h.stateSub...)
			sinks := []*subSink{}
			for _, list := range h.sinks {
				sinks = append(sinks, list...)
			}
			h.mu.Unlock()

			for _, ch := range subs {
				select {
				case ch <- s:
				default:
				}
			}

			if prev == StateReconnecting && s == StateConnected {
				for _, sink := range sinks {
					if sink.rejoin != nil {
						_ = sink.rejoin() // best-effort; failures swallowed (next reconnect will try again)
					}
				}
			}
			prev = s

			if s == StateDisconnected {
				return
			}
		}
	}
}

// addStateSub registers a state listener (used by Stream.State()).
func (h *hubConn) addStateSub(ch chan<- ConnState) {
	h.mu.Lock()
	h.stateSub = append(h.stateSub, ch)
	h.mu.Unlock()
}

func (h *hubConn) removeStateSub(ch chan<- ConnState) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for i, c := range h.stateSub {
		if c == ch {
			h.stateSub = append(h.stateSub[:i], h.stateSub[i+1:]...)
			return
		}
	}
}

func (c *Client) dialHub(ctx context.Context, k hubKind) (*hubConn, error) {
	hubURL := c.hubURL(k)
	if hubURL == "" {
		return nil, fmt.Errorf("unknown hub kind %v", k)
	}
	gatewayHeaders := func() http.Header {
		h := http.Header{}
		tok, err := c.AccessToken(ctx)
		if err == nil {
			h.Set("Authorization", "Bearer "+tok)
		}
		h.Set("Ocp-Apim-Subscription-Key", c.SubscriptionKey)
		return h
	}
	dialOnce := func(dctx context.Context) (*signalRClient, error) {
		return dialSignalR(dctx, hubURL, gatewayHeaders, c.Logger)
	}
	rc, err := newReconnectingClient(ctx, dialOnce, 30, 1*time.Second)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", k, err)
	}
	h := newHubConn(k, hubURL, rc)

	// Bridge: server-pushed messages flow through the reconnectingClient's
	// handler registry (which replays on reconnect) into hubConn's dispatch
	// system.
	for _, method := range hubServerMethods(k) {
		method := method
		rc.OnInvocation(method, func(args []json.RawMessage) {
			h.dispatch(method, args)
		})
	}

	go h.observeAndPropagate()
	return h, nil
}

// hubURL returns the gateway URL for a hub kind.
func (c *Client) hubURL(k hubKind) string {
	switch k {
	case hubDevice:
		return c.APIBase + "/DeviceHub"
	case hubChallenge:
		return c.APIBase + "/ChallengeHub"
	case hubNotification:
		return c.PlatformBase + "/api/appcue/v1/NotificationHub"
	default:
		return ""
	}
}

// hubServerMethods returns the server-pushed method names this hub will
// dispatch. Mirrors the bundle's hub-factory definitions and live
// captures of the wire protocol. NotificationHub names are best-guess
// until a live notification is observed.
func hubServerMethods(k hubKind) []string {
	switch k {
	case hubDevice:
		return []string{
			"DeviceListInitialValuesReceived",
			"DeviceListUpdatedValuesReceived",
			"DeviceAdded",
			"DeviceDeleted",
			"DevicesValuesReceived",
		}
	case hubChallenge:
		return []string{
			"EventListInitialValuesReceived",
			"EventListUpdatedValuesReceived",
			"EventAdded",
			"EventCHDetailsInitialValuesReceived",
			"EventCHDetailsUpdatedValuesReceived",
			"EventCHConsumptionUpdatedValuesReceived",
			"EventFlexDetailsInitialValuesReceived",
			"EventFlexDetailsUpdatedValuesReceived",
			"EventFlexConsumptionUpdatedValuesReceived",
			"Heartbeat",
		}
	case hubNotification:
		return []string{
			"NotificationReceived",
			"NotificationsListReceived",
			"NotificationUpdated",
		}
	}
	return nil
}

// acquireHub returns the hub for kind k, opening it on first use and
// bumping a refcount otherwise. Safe for concurrent callers.
func (c *Client) acquireHub(ctx context.Context, k hubKind) (*hubConn, error) {
	c.hubsMu.Lock()
	if h := c.hubs[k]; h != nil {
		h.mu.Lock()
		h.refs++
		h.mu.Unlock()
		c.hubsMu.Unlock()
		return h, nil
	}
	c.hubsMu.Unlock()

	h, err := c.dialHubFn(ctx, k)
	if err != nil {
		return nil, err
	}

	c.hubsMu.Lock()
	if existing := c.hubs[k]; existing != nil {
		// Lost a race; tear down ours and use theirs.
		c.hubsMu.Unlock()
		h.close()
		c.hubsMu.Lock()
		existing.mu.Lock()
		existing.refs++
		existing.mu.Unlock()
		c.hubsMu.Unlock()
		return existing, nil
	}
	h.refs = 1
	c.hubs[k] = h
	c.hubsMu.Unlock()
	return h, nil
}

// releaseHub decrements the refcount and closes the hub when it reaches zero.
func (c *Client) releaseHub(k hubKind) {
	c.hubsMu.Lock()
	h := c.hubs[k]
	if h == nil {
		c.hubsMu.Unlock()
		return
	}
	h.mu.Lock()
	h.refs--
	closing := h.refs == 0
	h.mu.Unlock()
	if closing {
		delete(c.hubs, k)
	}
	c.hubsMu.Unlock()
	if closing {
		h.close()
	}
}

// ---------------------------------------------------------------------------
// Production SignalR transport (*signalRClient)
// ---------------------------------------------------------------------------

// SignalR JSON-protocol message types.
// https://github.com/dotnet/aspnetcore/blob/main/src/SignalR/docs/specs/HubProtocol.md
const (
	srMsgInvocation       = 1
	srMsgStreamItem       = 2
	srMsgCompletion       = 3
	srMsgStreamInvocation = 4
	srMsgCancelInvocation = 5
	srMsgPing             = 6
	srMsgClose            = 7
)

const srRecordSep byte = 0x1e

// negotiateResp is the gateway/edge negotiate response. Hilo (Azure SignalR
// Service) uses a redirect-style negotiate: the first call returns
// {url, accessToken}; the second call against `url` returns
// {connectionId, connectionToken, availableTransports}.
type negotiateResp struct {
	NegotiateVersion    int    `json:"negotiateVersion"`
	URL                 string `json:"url,omitempty"`
	AccessToken         string `json:"accessToken,omitempty"`
	ConnectionID        string `json:"connectionId,omitempty"`
	ConnectionToken     string `json:"connectionToken,omitempty"`
	AvailableTransports []struct {
		Transport       string   `json:"transport"`
		TransferFormats []string `json:"transferFormats"`
	} `json:"availableTransports,omitempty"`
}

// signalRClient is the production hand-rolled SignalR JSON-protocol client.
// One instance per hub. Wraps a single github.com/coder/websocket connection.
// Satisfies srTransport.
type signalRClient struct {
	ws     *websocket.Conn
	ctx    context.Context
	cancel context.CancelFunc

	mu          sync.Mutex
	handlers    map[string]func(args []json.RawMessage)
	pending     map[string]chan invokeResult
	stateSubs   []chan<- ConnState
	state       ConnState
	invokeSeq   atomic.Uint64
	closed      atomic.Bool
	terminalErr atomic.Pointer[error]
	logger      func(format string, args ...any)

	sendCh chan []byte
}

// dialSignalR performs the two-step Azure SignalR negotiate, opens the
// WebSocket, completes the handshake, and returns a *signalRClient with
// background read/write/ping goroutines running. gatewayHeaders is the
// auth-headers function for the gateway negotiate (Bearer + Sub Key).
func dialSignalR(ctx context.Context, hubURL string, gatewayHeaders func() http.Header, logger func(string, ...any)) (*signalRClient, error) {
	nr1, err := signalrNegotiate(ctx, hubURL, gatewayHeaders())
	if err != nil {
		return nil, fmt.Errorf("gateway negotiate: %w", err)
	}
	if nr1.URL == "" || nr1.AccessToken == "" {
		return nil, fmt.Errorf("gateway negotiate: missing url/accessToken")
	}
	edgeAuth := http.Header{}
	edgeAuth.Set("Authorization", "Bearer "+nr1.AccessToken)
	nr2, err := signalrNegotiate(ctx, nr1.URL, edgeAuth)
	if err != nil {
		return nil, fmt.Errorf("edge negotiate: %w", err)
	}
	connToken := nr2.ConnectionToken
	if connToken == "" {
		connToken = nr2.ConnectionID
	}
	if connToken == "" {
		return nil, fmt.Errorf("edge negotiate: no connection token")
	}

	wsURL, err := url.Parse(nr1.URL)
	if err != nil {
		return nil, fmt.Errorf("parse edge url: %w", err)
	}
	switch wsURL.Scheme {
	case "https":
		wsURL.Scheme = "wss"
	case "http":
		wsURL.Scheme = "ws"
	}
	q := wsURL.Query()
	q.Set("id", connToken)
	wsURL.RawQuery = q.Encode()

	wsHeaders := http.Header{}
	wsHeaders.Set("Authorization", "Bearer "+nr1.AccessToken)
	ws, _, err := websocket.Dial(ctx, wsURL.String(), &websocket.DialOptions{
		HTTPHeader: wsHeaders,
	})
	if err != nil {
		return nil, fmt.Errorf("websocket dial %s: %w", wsURL.String(), err)
	}
	ws.SetReadLimit(2 * 1024 * 1024)

	if err := signalrWriteFrame(ctx, ws, []byte(`{"protocol":"json","version":1}`)); err != nil {
		ws.Close(websocket.StatusInternalError, "")
		return nil, fmt.Errorf("send handshake: %w", err)
	}
	hsCtx, hsCancel := context.WithTimeout(ctx, 10*time.Second)
	_, raw, err := ws.Read(hsCtx)
	hsCancel()
	if err != nil {
		ws.Close(websocket.StatusInternalError, "")
		return nil, fmt.Errorf("read handshake: %w", err)
	}
	for _, frame := range bytes.Split(raw, []byte{srRecordSep}) {
		if len(frame) == 0 {
			continue
		}
		var hr struct {
			Error string `json:"error"`
		}
		if err := json.Unmarshal(frame, &hr); err != nil {
			ws.Close(websocket.StatusInternalError, "")
			return nil, fmt.Errorf("decode handshake: %w (%s)", err, frame)
		}
		if hr.Error != "" {
			ws.Close(websocket.StatusInternalError, "")
			return nil, fmt.Errorf("handshake rejected: %s", hr.Error)
		}
		break
	}

	innerCtx, cancel := context.WithCancel(ctx)
	c := &signalRClient{
		ws:       ws,
		ctx:      innerCtx,
		cancel:   cancel,
		handlers: map[string]func([]json.RawMessage){},
		pending:  map[string]chan invokeResult{},
		state:    StateConnected,
		sendCh:   make(chan []byte, 64),
		logger:   logger,
	}
	go c.readLoop()
	go c.writeLoop()
	go c.pingLoop()
	return c, nil
}

func signalrNegotiate(ctx context.Context, baseURL string, headers http.Header) (*negotiateResp, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, err
	}
	u.Path = strings.TrimRight(u.Path, "/") + "/negotiate"
	q := u.Query()
	q.Set("negotiateVersion", "1")
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, "POST", u.String(), nil)
	if err != nil {
		return nil, err
	}
	for k, v := range headers {
		req.Header[k] = v
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("status %d: %s", resp.StatusCode, body)
	}
	var nr negotiateResp
	if err := json.NewDecoder(resp.Body).Decode(&nr); err != nil {
		return nil, err
	}
	return &nr, nil
}

func signalrWriteFrame(ctx context.Context, ws *websocket.Conn, payload []byte) error {
	buf := make([]byte, 0, len(payload)+1)
	buf = append(buf, payload...)
	buf = append(buf, srRecordSep)
	return ws.Write(ctx, websocket.MessageText, buf)
}

func (c *signalRClient) readLoop() {
	for {
		if c.closed.Load() {
			return
		}
		_, raw, err := c.ws.Read(c.ctx)
		if err != nil {
			c.markClosed(err)
			return
		}
		for _, frame := range bytes.Split(raw, []byte{srRecordSep}) {
			if len(frame) == 0 {
				continue
			}
			c.handleFrame(frame)
		}
	}
}

func (c *signalRClient) writeLoop() {
	for {
		select {
		case <-c.ctx.Done():
			return
		case msg, ok := <-c.sendCh:
			if !ok {
				return
			}
			if err := signalrWriteFrame(c.ctx, c.ws, msg); err != nil {
				c.markClosed(err)
				return
			}
		}
	}
}

func (c *signalRClient) pingLoop() {
	t := time.NewTicker(15 * time.Second)
	defer t.Stop()
	pingMsg := []byte(`{"type":6}`)
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-t.C:
			select {
			case c.sendCh <- pingMsg:
			case <-c.ctx.Done():
				return
			default:
			}
		}
	}
}

func (c *signalRClient) handleFrame(frame []byte) {
	var probe struct {
		Type         int               `json:"type"`
		Target       string            `json:"target"`
		InvocationID string            `json:"invocationId"`
		Arguments    []json.RawMessage `json:"arguments"`
		Result       json.RawMessage   `json:"result,omitempty"`
		Error        string            `json:"error,omitempty"`
	}
	if err := json.Unmarshal(frame, &probe); err != nil {
		if c.logger != nil {
			c.logger("signalr: decode message: %v / %s", err, frame)
		}
		return
	}
	switch probe.Type {
	case srMsgInvocation:
		c.mu.Lock()
		h := c.handlers[probe.Target]
		c.mu.Unlock()
		if h != nil {
			h(probe.Arguments)
		} else if c.logger != nil {
			c.logger("signalr: unhandled target=%s", probe.Target)
		}
	case srMsgCompletion:
		c.mu.Lock()
		ch, ok := c.pending[probe.InvocationID]
		if ok {
			delete(c.pending, probe.InvocationID)
		}
		c.mu.Unlock()
		if ok {
			res := invokeResult{value: probe.Result}
			if probe.Error != "" {
				res.err = fmt.Errorf("server: %s", probe.Error)
			}
			ch <- res
			close(ch)
		}
	case srMsgPing:
		// no-op; pings from server are keepalives.
	case srMsgClose:
		c.markClosed(fmt.Errorf("server close: %s", frame))
	}
}

func (c *signalRClient) markClosed(err error) {
	if c.closed.Swap(true) {
		return
	}
	if err != nil {
		c.terminalErr.Store(&err)
		if c.logger != nil {
			c.logger("signalr: closed: %v", err)
		}
	}
	c.setStateInternal(StateDisconnected)
	c.cancel()
	if c.ws != nil {
		c.ws.Close(websocket.StatusNormalClosure, "")
	}

	// Fail any pending invokes.
	c.mu.Lock()
	pending := c.pending
	c.pending = map[string]chan invokeResult{}
	c.mu.Unlock()
	for _, ch := range pending {
		ch <- invokeResult{err: fmt.Errorf("connection closed")}
		close(ch)
	}
}

func (c *signalRClient) setStateInternal(s ConnState) {
	c.mu.Lock()
	c.state = s
	subs := append([]chan<- ConnState{}, c.stateSubs...)
	c.mu.Unlock()
	for _, ch := range subs {
		select {
		case ch <- s:
		default:
		}
	}
}

// srTransport implementation.

func (c *signalRClient) Start() {
	// Already started in dialSignalR; this is a no-op for API compatibility.
}

func (c *signalRClient) Stop() {
	c.markClosed(nil)
}

func (c *signalRClient) State() ConnState {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state
}

func (c *signalRClient) Observe(ch chan<- ConnState) func() {
	c.mu.Lock()
	cur := c.state
	c.stateSubs = append(c.stateSubs, ch)
	c.mu.Unlock()

	// Push current state immediately so newly-registered observers don't
	// miss the connect event that already happened.
	select {
	case ch <- cur:
	default:
	}

	return func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		for i, sub := range c.stateSubs {
			if sub == ch {
				c.stateSubs = append(c.stateSubs[:i], c.stateSubs[i+1:]...)
				return
			}
		}
	}
}

func (c *signalRClient) Invoke(method string, args ...any) <-chan invokeResult {
	out := make(chan invokeResult, 1)

	if c.closed.Load() {
		out <- invokeResult{err: fmt.Errorf("signalr: closed")}
		close(out)
		return out
	}

	id := strconv.FormatUint(c.invokeSeq.Add(1), 10)
	msg := struct {
		Type         int    `json:"type"`
		InvocationID string `json:"invocationId"`
		Target       string `json:"target"`
		Arguments    []any  `json:"arguments"`
	}{srMsgInvocation, id, method, args}
	b, err := json.Marshal(msg)
	if err != nil {
		out <- invokeResult{err: err}
		close(out)
		return out
	}

	c.mu.Lock()
	c.pending[id] = out
	c.mu.Unlock()

	select {
	case c.sendCh <- b:
	case <-c.ctx.Done():
		c.mu.Lock()
		_, stillOwned := c.pending[id]
		delete(c.pending, id)
		c.mu.Unlock()
		if stillOwned {
			out <- invokeResult{err: c.ctx.Err()}
			close(out)
		}
	}
	return out
}

func (c *signalRClient) Send(method string, args ...any) <-chan error {
	out := make(chan error, 1)
	if c.closed.Load() {
		out <- fmt.Errorf("signalr: closed")
		close(out)
		return out
	}
	msg := struct {
		Type      int    `json:"type"`
		Target    string `json:"target"`
		Arguments []any  `json:"arguments"`
	}{srMsgInvocation, method, args}
	b, err := json.Marshal(msg)
	if err != nil {
		out <- err
		close(out)
		return out
	}
	select {
	case c.sendCh <- b:
		close(out)
	case <-c.ctx.Done():
		out <- c.ctx.Err()
		close(out)
	}
	return out
}

func (c *signalRClient) Err() error {
	if p := c.terminalErr.Load(); p != nil {
		return *p
	}
	return nil
}

// OnInvocation registers a handler for a specific server-pushed method
// (type-1 frames with target == method). hubConn uses this to route
// incoming pushes into its own dispatch system.
func (c *signalRClient) OnInvocation(method string, handler func(args []json.RawMessage)) {
	c.mu.Lock()
	c.handlers[method] = handler
	c.mu.Unlock()
}

// Compile-time interface check.
var _ srTransport = (*signalRClient)(nil)

// ---------------------------------------------------------------------------
// reconnectingClient — exponential-backoff auto-reconnect wrapper
// ---------------------------------------------------------------------------

// reconnectingClient wraps a *signalRClient with auto-reconnect (exponential
// backoff up to 60s, capped attempts). On terminal disconnect of the inner
// client it emits StateReconnecting, re-dials, replays OnInvocation
// registrations, and emits StateConnected.
//
// Lifecycle: created by Client.dialHub; owns its own ctx via WithCancel.
// Stop (or parent-ctx cancellation) tears down the active inner
// signalRClient, terminates the reconnect loop, and releases all
// per-stream goroutines.
type reconnectingClient struct {
	ctx       context.Context
	cancel    context.CancelFunc
	dialFn    func(ctx context.Context) (*signalRClient, error)
	maxTries  int
	baseDelay time.Duration

	mu       sync.Mutex
	current  *signalRClient
	handlers map[string]func([]json.RawMessage)
	subs     []chan<- ConnState
	state    ConnState
	closed   atomic.Bool
}

func newReconnectingClient(ctx context.Context, dialFn func(context.Context) (*signalRClient, error), maxTries int, baseDelay time.Duration) (*reconnectingClient, error) {
	innerCtx, cancel := context.WithCancel(ctx)
	rc := &reconnectingClient{
		ctx:       innerCtx,
		cancel:    cancel,
		dialFn:    dialFn,
		maxTries:  maxTries,
		baseDelay: baseDelay,
		handlers:  map[string]func([]json.RawMessage){},
		state:     StateConnecting,
	}
	if err := rc.connect(); err != nil {
		cancel()
		return nil, err
	}
	go rc.watch()
	return rc, nil
}

func (rc *reconnectingClient) connect() error {
	sr, err := rc.dialFn(rc.ctx)
	if err != nil {
		return err
	}
	rc.mu.Lock()
	rc.current = sr
	for target, h := range rc.handlers {
		sr.OnInvocation(target, h)
	}
	rc.setStateLocked(StateConnected)
	rc.mu.Unlock()
	return nil
}

func (rc *reconnectingClient) watch() {
	for {
		if rc.closed.Load() {
			return
		}
		rc.mu.Lock()
		cur := rc.current
		rc.mu.Unlock()
		if cur == nil {
			return
		}
		// Wait for the current client to reach Disconnected.
		stateCh := make(chan ConnState, 4)
		cancelObs := cur.Observe(stateCh)
		for s := range stateCh {
			if s == StateDisconnected {
				break
			}
		}
		cancelObs()

		if rc.closed.Load() {
			return
		}

		// Reconnect loop with exponential backoff.
		rc.mu.Lock()
		rc.setStateLocked(StateReconnecting)
		rc.mu.Unlock()

		delay := rc.baseDelay
		gaveUp := true
		for i := 0; i < rc.maxTries; i++ {
			select {
			case <-rc.ctx.Done():
				rc.mu.Lock()
				rc.setStateLocked(StateDisconnected)
				rc.mu.Unlock()
				return
			case <-time.After(delay):
			}
			if err := rc.connect(); err == nil {
				gaveUp = false
				break
			}
			delay *= 2
			if delay > 60*time.Second {
				delay = 60 * time.Second
			}
		}
		if gaveUp {
			rc.mu.Lock()
			rc.setStateLocked(StateDisconnected)
			rc.mu.Unlock()
			return
		}
	}
}

func (rc *reconnectingClient) setStateLocked(s ConnState) {
	rc.state = s
	subs := append([]chan<- ConnState{}, rc.subs...)
	go func() {
		for _, ch := range subs {
			select {
			case ch <- s:
			default:
			}
		}
	}()
}

// srTransport interface methods forward to the current underlying client.

func (rc *reconnectingClient) Start() {}

func (rc *reconnectingClient) Stop() {
	if rc.closed.Swap(true) {
		return
	}
	rc.cancel()
	rc.mu.Lock()
	cur := rc.current
	rc.mu.Unlock()
	if cur != nil {
		cur.Stop()
	}
}

func (rc *reconnectingClient) State() ConnState {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	return rc.state
}

func (rc *reconnectingClient) Observe(ch chan<- ConnState) func() {
	rc.mu.Lock()
	cur := rc.state
	rc.subs = append(rc.subs, ch)
	rc.mu.Unlock()
	select {
	case ch <- cur:
	default:
	}
	return func() {
		rc.mu.Lock()
		defer rc.mu.Unlock()
		for i, sub := range rc.subs {
			if sub == ch {
				rc.subs = append(rc.subs[:i], rc.subs[i+1:]...)
				return
			}
		}
	}
}

func (rc *reconnectingClient) Invoke(method string, args ...any) <-chan invokeResult {
	rc.mu.Lock()
	cur := rc.current
	rc.mu.Unlock()
	if cur == nil {
		ch := make(chan invokeResult, 1)
		ch <- invokeResult{err: fmt.Errorf("reconnectingClient: not connected")}
		close(ch)
		return ch
	}
	return cur.Invoke(method, args...)
}

func (rc *reconnectingClient) Send(method string, args ...any) <-chan error {
	rc.mu.Lock()
	cur := rc.current
	rc.mu.Unlock()
	if cur == nil {
		ch := make(chan error, 1)
		ch <- fmt.Errorf("reconnectingClient: not connected")
		close(ch)
		return ch
	}
	return cur.Send(method, args...)
}

func (rc *reconnectingClient) Err() error {
	rc.mu.Lock()
	cur := rc.current
	rc.mu.Unlock()
	if cur == nil {
		return nil
	}
	return cur.Err()
}

func (rc *reconnectingClient) OnInvocation(target string, handler func([]json.RawMessage)) {
	rc.mu.Lock()
	rc.handlers[target] = handler
	cur := rc.current
	rc.mu.Unlock()
	if cur != nil {
		cur.OnInvocation(target, handler)
	}
}

var _ srTransport = (*reconnectingClient)(nil)

// startSubscription is the shared skeleton every Subscribe* uses.
//
//   - acquire the hub (opens the underlying connection on first use)
//   - call register so the caller can add typed sinks to the hub
//   - run the server-side subscribe Invoke; on failure tear down sinks/refs
//   - register a state subscriber so Stream.State() works
//   - launch a goroutine that runs unsubscribe + close on ctx.Done()
//
// stream is the *Stream[T] the caller will return; it must support
// pushState(ConnState) and close(error). The sinksPtr is dereferenced
// after register runs so the helper sees whatever sinks the register
// callback added.
func (c *Client) startSubscription(
	ctx context.Context,
	k hubKind,
	stream interface {
		pushState(ConnState)
		close(error)
	},
	sinksPtr *[]*subSink,
	register func(*hubConn),
	subMethod string, subArgs []any,
	unsubMethod string, unsubArgs []any,
) error {
	h, err := c.acquireHub(ctx, k)
	if err != nil {
		return err
	}
	register(h)

	// Server-side subscribe. Wait for the result so a bad locID surfaces here.
	if subMethod != "" {
		select {
		case res := <-h.client.Invoke(subMethod, subArgs...):
			if res.err != nil {
				h.removeSinks(*sinksPtr...)
				c.releaseHub(k)
				return fmt.Errorf("%s: %w", subMethod, res.err)
			}
		case <-ctx.Done():
			h.removeSinks(*sinksPtr...)
			c.releaseHub(k)
			return ctx.Err()
		}
	}

	// Wire stream state from hubConn state-sub.
	// A shared "terminate" channel lets EITHER ctx.Done() OR a terminal
	// StateDisconnected push trigger the cleanup path exactly once.
	stateCh := make(chan ConnState, 4)
	h.addStateSub(stateCh)
	stateDone := make(chan struct{})
	terminate := make(chan struct{}) // closed once on either cancel or terminal disconnect
	var termOnce sync.Once
	doTerminate := func() { termOnce.Do(func() { close(terminate) }) }

	go func() {
		defer close(stateDone)
		for {
			select {
			case s, ok := <-stateCh:
				if !ok {
					return
				}
				stream.pushState(s)
				if s == StateDisconnected {
					doTerminate()
					return
				}
			case <-terminate:
				return
			}
		}
	}()

	// Lifecycle: either ctx.Done() or terminal disconnect unsubscribes
	// server-side and closes the stream.
	go func() {
		select {
		case <-ctx.Done():
			doTerminate()
		case <-terminate:
			// state-drainer already triggered termination
		}
		// Best-effort server-side unsubscribe (only if transport still up).
		if unsubMethod != "" {
			invokeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			select {
			case <-h.client.Invoke(unsubMethod, unsubArgs...):
			case <-invokeCtx.Done():
			}
		}
		h.removeStateSub(stateCh)
		<-stateDone
		h.removeSinks(*sinksPtr...)

		// Determine the terminal error: ctx.Err takes priority if set, else
		// the transport's terminal error if any.
		var termErr error
		if cerr := ctx.Err(); cerr != nil {
			termErr = cerr
		} else if terr := h.client.Err(); terr != nil {
			termErr = terr
		} else {
			termErr = fmt.Errorf("transport disconnected")
		}
		stream.close(termErr)
		c.releaseHub(k)
	}()

	return nil
}
