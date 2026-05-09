package hilo

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
)

// gqlSubscriber is one active GraphQL multipart subscription. The HTTP
// response streams JSON parts separated by --graphql boundaries; the
// background reader goroutine deserializes each part and dispatches
// the raw payload to the registered handler.
//
// Lifecycle: spawned by openGraphQLSubscription with its own derived
// context. Cancelling that context (via Client.releaseGraphQLSubscription
// or parent ctx) closes the HTTP body, ends the reader goroutine, and
// signals done. Callers never construct gqlSubscriber directly.
type gqlSubscriber struct {
	ctx    context.Context
	cancel context.CancelFunc
	body   io.ReadCloser
	done   chan struct{}
}

// openGraphQLSubscription POSTs a subscription request to the digital-twin
// endpoint with a multipart Accept header, validates the response is
// multipart, and spins a goroutine that streams parts into the handler.
//
// handler is called once per JSON part with the raw bytes (caller decodes
// per its needs). When the stream ends or ctx cancels, the goroutine
// closes sub.done.
func (c *Client) openGraphQLSubscription(parentCtx context.Context, query string, vars map[string]any, handler func(payload []byte)) (*gqlSubscriber, error) {
	body := map[string]any{
		"query":     query,
		"variables": vars,
	}
	rawBody, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}

	tok, err := c.AccessToken(parentCtx)
	if err != nil {
		return nil, fmt.Errorf("AccessToken: %w", err)
	}

	url := c.PlatformBase + "/api/digital-twin/v3/graphql"
	req, err := http.NewRequestWithContext(parentCtx, "POST", url, bytes.NewReader(rawBody))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Ocp-Apim-Subscription-Key", c.SubscriptionKey)
	req.Header.Set("Accept", `multipart/mixed; boundary=graphql; subscriptionSpec=1.0, application/json`)

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return nil, fmt.Errorf("subscription status %d: %s", resp.StatusCode, bodyBytes)
	}

	mediaType, params, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if err != nil {
		resp.Body.Close()
		return nil, fmt.Errorf("parse Content-Type: %w", err)
	}
	if mediaType != "multipart/mixed" {
		resp.Body.Close()
		return nil, fmt.Errorf("expected multipart/mixed Content-Type, got %s", mediaType)
	}
	boundary := params["boundary"]
	if boundary == "" {
		resp.Body.Close()
		return nil, fmt.Errorf("missing multipart boundary in Content-Type")
	}

	innerCtx, cancel := context.WithCancel(parentCtx)
	sub := &gqlSubscriber{
		ctx:    innerCtx,
		cancel: cancel,
		body:   resp.Body,
		done:   make(chan struct{}),
	}

	go sub.readLoop(boundary, handler, c.Logger)
	return sub, nil
}

func (s *gqlSubscriber) readLoop(boundary string, handler func([]byte), logger func(string, ...any)) {
	defer close(s.done)
	defer s.body.Close()

	mr := multipart.NewReader(s.body, boundary)
	for {
		select {
		case <-s.ctx.Done():
			return
		default:
		}
		part, err := mr.NextPart()
		if err == io.EOF {
			return
		}
		if err != nil {
			if logger != nil && s.ctx.Err() == nil {
				logger("hilo: gqlSubscriber multipart read: %v", err)
			}
			return
		}
		payload, err := io.ReadAll(part)
		_ = part.Close()
		if err != nil {
			if logger != nil {
				logger("hilo: gqlSubscriber part read: %v", err)
			}
			continue
		}
		// Trim trailing whitespace (CRLF after JSON body).
		payload = bytes.TrimSpace(payload)
		if len(payload) == 0 {
			continue
		}
		handler(payload)
	}
}

// gqlSub is the per-location refcount-tracked GraphQL subscription used
// by awaitOp. Mirrors the opSub pattern used by DeviceHub.
type gqlSub struct {
	cancel context.CancelFunc
	refs   int
	done   chan struct{}
}

// ensureGraphQLSubscription lazy-opens the onAnyDeviceUpdated subscription
// for the given location, refcount-tracked. Returns nil if the
// subscription successfully opened (caller should rely on it for status);
// returns a non-nil error if the subscription couldn't open (caller falls
// back to the DeviceHub Values echo path).
func (c *Client) ensureGraphQLSubscription(locID int) error {
	c.gqlMu.Lock()
	if sub, ok := c.gqlSubs[locID]; ok {
		sub.refs++
		c.gqlMu.Unlock()
		return nil
	}
	c.gqlMu.Unlock()

	subCtx, cancel := context.WithCancel(context.Background())
	hiloID, err := c.locationHiloID(subCtx, locID)
	if err != nil {
		cancel()
		return fmt.Errorf("locationHiloID: %w", err)
	}

	query := `subscription onAnyDeviceUpdated($id: String!) {
		onAnyDeviceUpdated(locationId: $id) {
			operationId
			status
			statusReason
			device {
				hiloId
			}
		}
	}`
	vars := map[string]any{"id": string(hiloID)}

	gsub, err := c.openGraphQLSubscription(subCtx, query, vars, c.gqlOpHandler(locID))
	if err != nil {
		cancel()
		return fmt.Errorf("openGraphQLSubscription: %w", err)
	}

	sub := &gqlSub{
		cancel: cancel,
		refs:   1,
		done:   gsub.done,
	}

	c.gqlMu.Lock()
	if existing, ok := c.gqlSubs[locID]; ok {
		c.gqlMu.Unlock()
		cancel() // was: gsub.cancel() — cancel the outer context to clean the full chain
		c.gqlMu.Lock()
		existing.refs++
		c.gqlMu.Unlock()
		return nil
	}
	c.gqlSubs[locID] = sub
	c.gqlMu.Unlock()
	return nil
}

// releaseGraphQLSubscription decrements the refcount and tears the
// subscription down at zero.
func (c *Client) releaseGraphQLSubscription(locID int) {
	c.gqlMu.Lock()
	sub := c.gqlSubs[locID]
	if sub == nil {
		c.gqlMu.Unlock()
		return
	}
	sub.refs--
	closing := sub.refs == 0
	if closing {
		delete(c.gqlSubs, locID)
	}
	c.gqlMu.Unlock()
	if closing {
		sub.cancel()
		<-sub.done
	}
}

// gqlOpHandler returns a handler closure that decodes onAnyDeviceUpdated
// payloads and routes them into opPending. operationIds matched by the
// awaitOp registry receive the *real* Operation status from the wire.
func (c *Client) gqlOpHandler(locID int) func([]byte) {
	return func(raw []byte) {
		var msg struct {
			Data struct {
				OnAnyDeviceUpdated struct {
					OperationID  string                `json:"operationId"`
					Status       OperationStatus       `json:"status"`
					StatusReason OperationStatusReason `json:"statusReason"`
					Device       struct {
						HiloID HiloID `json:"hiloId"`
					} `json:"device"`
				} `json:"onAnyDeviceUpdated"`
			} `json:"data"`
		}
		if err := json.Unmarshal(raw, &msg); err != nil {
			if c.Logger != nil {
				c.Logger("hilo: gqlOpHandler decode: %v / %s", err, raw)
			}
			return
		}
		opID := msg.Data.OnAnyDeviceUpdated.OperationID
		if opID == "" {
			// Passive update (not from a write); ignore.
			return
		}
		op := &Operation{
			OperationID:  opID,
			Status:       msg.Data.OnAnyDeviceUpdated.Status,
			StatusReason: msg.Data.OnAnyDeviceUpdated.StatusReason,
		}

		// Match by OperationID alone. Scan opPending for any key whose
		// OperationID equals opID; deliver to all (the typical case is
		// exactly one match since opIDs are unique per write).
		c.opMu.Lock()
		var matched []chan<- *Operation
		for k, entry := range c.opPending {
			if k.OperationID == opID {
				matched = append(matched, entry.ch)
			}
		}
		c.opMu.Unlock()
		for _, ch := range matched {
			select {
			case ch <- op:
			default:
			}
		}
	}
}

// locationHiloID resolves the HiloID for an integer location ID.
// Cache-first: returns immediately if the ID is already cached.
// Falls back to ListLocations on a cache miss, populating the cache
// for all returned locations so subsequent calls are free. Callers
// can pre-populate via PrimeLocationHiloID or RefreshLocationHiloIDCache
// to avoid the fallback round-trip entirely.
func (c *Client) locationHiloID(ctx context.Context, locID int) (HiloID, error) {
	// Cache-first.
	c.gqlMu.Lock()
	id, ok := c.locHiloIDCache[locID]
	c.gqlMu.Unlock()
	if ok {
		return id, nil
	}
	// Fallback: hit the REST API once and populate cache for all locations.
	locs, err := c.ListLocations(ctx)
	if err != nil {
		return "", err
	}
	c.gqlMu.Lock()
	for _, l := range locs {
		c.locHiloIDCache[l.ID] = l.LocationHiloID
	}
	id, ok = c.locHiloIDCache[locID]
	c.gqlMu.Unlock()
	if !ok {
		return "", fmt.Errorf("location id=%d not found", locID)
	}
	return id, nil
}

// PrimeLocationHiloID pre-populates the location HiloID cache so that
// ensureGraphQLSubscription can open subscriptions without making a
// ListLocations REST call. Production callers that already have a
// Location (from a prior call to ListLocations) should call this to
// reduce latency.
func (c *Client) PrimeLocationHiloID(locID int, hiloID HiloID) {
	c.gqlMu.Lock()
	c.locHiloIDCache[locID] = hiloID
	c.gqlMu.Unlock()
}

// RefreshLocationHiloIDCache calls ListLocations and populates the
// cache for all returned locations. Call once at start-up (or after a
// location change) so that ensureGraphQLSubscription can resolve HiloIDs
// without a round-trip per write.
func (c *Client) RefreshLocationHiloIDCache(ctx context.Context) error {
	locs, err := c.ListLocations(ctx)
	if err != nil {
		return err
	}
	c.gqlMu.Lock()
	for _, l := range locs {
		c.locHiloIDCache[l.ID] = l.LocationHiloID
	}
	c.gqlMu.Unlock()
	return nil
}
