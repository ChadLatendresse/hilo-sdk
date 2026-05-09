package hilo

import (
	"context"
	"encoding/json"
	"fmt"
)

// HubEvent is a placeholder until a real fixture is captured during the
// heating season. The actual wire shape will be determined empirically.
type HubEvent struct {
	Raw json.RawMessage `json:"-"`
}

// EventListUpdateKind classifies one EventList push.
type EventListUpdateKind int

const (
	EventListSnapshot EventListUpdateKind = iota // EventListInitialValuesReceived
	EventListDelta                               // EventListUpdatedValuesReceived
	EventListAdded                               // EventAdded
)

func (k EventListUpdateKind) String() string {
	switch k {
	case EventListSnapshot:
		return "Snapshot"
	case EventListDelta:
		return "Delta"
	case EventListAdded:
		return "Added"
	default:
		return "Unknown"
	}
}

// EventListUpdate is one push from ChallengeHub's EventList subscription.
type EventListUpdate struct {
	Kind    EventListUpdateKind
	Events  []HubEvent
	EventID string // populated only when Kind == EventListAdded
}

// SubscribeEventList subscribes to the live list of Hilo Challenge events
// for one location. Snapshots arrive on (re)subscribe, Deltas on changes,
// Added when a new event is announced.
//
// Per the bundle's pattern, the SubscribeToEventList args are an object
// {locationHiloId: <id>} — sending a bare string fails server-side with a
// LocationKey deserialization error.
func (c *Client) SubscribeEventList(ctx context.Context, locID HiloID) (*Stream[EventListUpdate], error) {
	stream := newStream[EventListUpdate](32)
	sinks := []*subSink{}

	subArgs := []any{map[string]string{"locationHiloId": string(locID)}}
	register := func(h *hubConn) {
		rejoin := func() error {
			res := <-h.client.Invoke("SubscribeToEventList", subArgs...)
			return res.err
		}
		sinks = append(sinks,
			h.addSinkWithRejoin("EventListInitialValuesReceived", nil,
				func(args []json.RawMessage) {
					upd, err := decodeEventList(args, EventListSnapshot, "")
					if err != nil {
						return
					}
					stream.deliver(upd)
				}, rejoin),
			h.addSink("EventListUpdatedValuesReceived", nil,
				func(args []json.RawMessage) {
					upd, err := decodeEventList(args, EventListDelta, "")
					if err != nil {
						return
					}
					stream.deliver(upd)
				}),
			h.addSink("EventAdded", nil,
				func(args []json.RawMessage) {
					var id string
					if len(args) >= 2 {
						_ = json.Unmarshal(args[1], &id)
					}
					upd, err := decodeEventList(args, EventListAdded, id)
					if err != nil {
						return
					}
					stream.deliver(upd)
				}),
		)
	}

	if err := c.startSubscription(ctx, hubChallenge, stream, &sinks, register,
		"SubscribeToEventList", subArgs,
		"UnsubscribeFromEventList", subArgs); err != nil {
		return nil, err
	}
	return stream, nil
}

// decodeEventList accepts either an array (Initial/Updated) or a single
// event (Added) in args[0] and returns an EventListUpdate.
func decodeEventList(args []json.RawMessage, kind EventListUpdateKind, addedID string) (EventListUpdate, error) {
	if len(args) == 0 {
		return EventListUpdate{}, fmt.Errorf("decodeEventList: no args")
	}
	var events []HubEvent
	if err := json.Unmarshal(args[0], &events); err != nil {
		// EventAdded shape may carry a single event, not an array.
		var single HubEvent
		if err2 := json.Unmarshal(args[0], &single); err2 != nil {
			return EventListUpdate{}, fmt.Errorf("decodeEventList: %w / %w", err, err2)
		}
		events = []HubEvent{single}
	}
	return EventListUpdate{Kind: kind, Events: events, EventID: addedID}, nil
}

// EventCHDetails is a per-event detail object for a Hilo Challenge ("CH" =
// Critical Hours). The wire shape is best-guess until a real fixture is
// captured during the heating season; Raw exposes the full payload.
type EventCHDetails struct {
	EventID   string          `json:"eventId"`
	StartTime string          `json:"startTime,omitempty"`
	EndTime   string          `json:"endTime,omitempty"`
	Phase     string          `json:"phase,omitempty"`
	Raw       json.RawMessage `json:"-"`
}

// EventCHConsumption is a per-event live-consumption push. Wire shape unverified.
type EventCHConsumption struct {
	EventID    string          `json:"eventId"`
	Watts      float64         `json:"watts,omitempty"`
	Reduction  float64         `json:"reduction,omitempty"`
	ReceivedAt string          `json:"receivedAt,omitempty"`
	Raw        json.RawMessage `json:"-"`
}

// EventCHDetailsUpdate is one push from the EventCHDetails subscription.
// Initial=true marks the snapshot delivered after (re)subscribe;
// Consumption is non-nil only on consumption pushes (which arrive on the
// same subscription, merged into this stream).
type EventCHDetailsUpdate struct {
	Initial     bool
	Details     EventCHDetails
	Consumption *EventCHConsumption
}

// SubscribeEventCHDetails subscribes to per-event details and consumption
// updates for one CH event. Per the bundle, SubscribeToEventCH args are
// object-wrapped: {locationHiloId, eventId}.
func (c *Client) SubscribeEventCHDetails(ctx context.Context, locID HiloID, eventID string) (*Stream[EventCHDetailsUpdate], error) {
	stream := newStream[EventCHDetailsUpdate](32)
	sinks := []*subSink{}

	subArgs := []any{map[string]string{
		"locationHiloId": string(locID),
		"eventId":        eventID,
	}}

	matchEvent := func(args []json.RawMessage) bool {
		if len(args) == 0 {
			return false
		}
		var probe struct {
			EventID string `json:"eventId"`
		}
		_ = json.Unmarshal(args[0], &probe)
		return probe.EventID == eventID
	}

	register := func(h *hubConn) {
		rejoin := func() error {
			res := <-h.client.Invoke("SubscribeToEventCH", subArgs...)
			return res.err
		}
		sinks = append(sinks,
			h.addSinkWithRejoin("EventCHDetailsInitialValuesReceived", matchEvent,
				func(args []json.RawMessage) {
					upd, err := decodeEventCHDetails(args, true, false)
					if err != nil {
						return
					}
					stream.deliver(upd)
				}, rejoin),
			h.addSink("EventCHDetailsUpdatedValuesReceived", matchEvent,
				func(args []json.RawMessage) {
					upd, err := decodeEventCHDetails(args, false, false)
					if err != nil {
						return
					}
					stream.deliver(upd)
				}),
			h.addSink("EventCHConsumptionUpdatedValuesReceived", matchEvent,
				func(args []json.RawMessage) {
					upd, err := decodeEventCHDetails(args, false, true)
					if err != nil {
						return
					}
					stream.deliver(upd)
				}),
		)
	}

	if err := c.startSubscription(ctx, hubChallenge, stream, &sinks, register,
		"SubscribeToEventCH", subArgs,
		"UnsubscribeFromEventCH", subArgs); err != nil {
		return nil, err
	}
	return stream, nil
}

// RequestEventCHConsumption asks the server to push a fresh CH-event
// consumption update on the existing subscription. Args are
// {locationHiloId, eventId} per the bundle.
func (c *Client) RequestEventCHConsumption(ctx context.Context, locID HiloID, eventID string) error {
	return c.invokeOnHub(ctx, hubChallenge, "RequestEventCHConsumptionUpdate",
		map[string]string{"locationHiloId": string(locID), "eventId": eventID})
}

func decodeEventCHDetails(args []json.RawMessage, initial, isConsumption bool) (EventCHDetailsUpdate, error) {
	if len(args) == 0 {
		return EventCHDetailsUpdate{}, fmt.Errorf("decodeEventCHDetails: no args")
	}
	upd := EventCHDetailsUpdate{Initial: initial}
	if isConsumption {
		var cons EventCHConsumption
		if err := json.Unmarshal(args[0], &cons); err != nil {
			return upd, fmt.Errorf("decode consumption: %w", err)
		}
		cons.Raw = args[0]
		upd.Consumption = &cons
		return upd, nil
	}
	var det EventCHDetails
	if err := json.Unmarshal(args[0], &det); err != nil {
		return upd, fmt.Errorf("decode details: %w", err)
	}
	det.Raw = args[0]
	upd.Details = det
	return upd, nil
}

// EventFlexDetails / EventFlexConsumption mirror EventCH but for Flex events.
type EventFlexDetails struct {
	EventID string          `json:"eventId"`
	Raw     json.RawMessage `json:"-"`
}

type EventFlexConsumption struct {
	EventID string          `json:"eventId"`
	Raw     json.RawMessage `json:"-"`
}

type EventFlexDetailsUpdate struct {
	Initial     bool
	Details     EventFlexDetails
	Consumption *EventFlexConsumption
}

func (c *Client) SubscribeEventFlexDetails(ctx context.Context, locID HiloID, eventID string) (*Stream[EventFlexDetailsUpdate], error) {
	stream := newStream[EventFlexDetailsUpdate](32)
	sinks := []*subSink{}

	subArgs := []any{map[string]string{
		"locationHiloId": string(locID),
		"eventId":        eventID,
	}}

	matchEvent := func(args []json.RawMessage) bool {
		if len(args) == 0 {
			return false
		}
		var probe struct {
			EventID string `json:"eventId"`
		}
		_ = json.Unmarshal(args[0], &probe)
		return probe.EventID == eventID
	}

	register := func(h *hubConn) {
		rejoin := func() error {
			res := <-h.client.Invoke("SubscribeToEventFlex", subArgs...)
			return res.err
		}
		sinks = append(sinks,
			h.addSinkWithRejoin("EventFlexDetailsInitialValuesReceived", matchEvent,
				func(args []json.RawMessage) {
					upd, err := decodeEventFlexDetails(args, true, false)
					if err != nil {
						return
					}
					stream.deliver(upd)
				}, rejoin),
			h.addSink("EventFlexDetailsUpdatedValuesReceived", matchEvent,
				func(args []json.RawMessage) {
					upd, err := decodeEventFlexDetails(args, false, false)
					if err != nil {
						return
					}
					stream.deliver(upd)
				}),
			h.addSink("EventFlexConsumptionUpdatedValuesReceived", matchEvent,
				func(args []json.RawMessage) {
					upd, err := decodeEventFlexDetails(args, false, true)
					if err != nil {
						return
					}
					stream.deliver(upd)
				}),
		)
	}

	if err := c.startSubscription(ctx, hubChallenge, stream, &sinks, register,
		"SubscribeToEventFlex", subArgs,
		"UnsubscribeFromEventFlex", subArgs); err != nil {
		return nil, err
	}
	return stream, nil
}

// RequestEventFlexConsumption asks the server to push a fresh Flex-event
// consumption update on the existing subscription. Args are
// {locationHiloId, eventId} per the bundle.
func (c *Client) RequestEventFlexConsumption(ctx context.Context, locID HiloID, eventID string) error {
	return c.invokeOnHub(ctx, hubChallenge, "RequestEventFlexConsumptionUpdate",
		map[string]string{"locationHiloId": string(locID), "eventId": eventID})
}

func decodeEventFlexDetails(args []json.RawMessage, initial, isConsumption bool) (EventFlexDetailsUpdate, error) {
	if len(args) == 0 {
		return EventFlexDetailsUpdate{}, fmt.Errorf("decodeEventFlexDetails: no args")
	}
	upd := EventFlexDetailsUpdate{Initial: initial}
	if isConsumption {
		var cons EventFlexConsumption
		if err := json.Unmarshal(args[0], &cons); err != nil {
			return upd, fmt.Errorf("decode flex consumption: %w", err)
		}
		cons.Raw = args[0]
		upd.Consumption = &cons
		return upd, nil
	}
	var det EventFlexDetails
	if err := json.Unmarshal(args[0], &det); err != nil {
		return upd, fmt.Errorf("decode flex details: %w", err)
	}
	det.Raw = args[0]
	upd.Details = det
	return upd, nil
}
