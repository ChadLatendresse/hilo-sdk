package hilo

import (
	"context"
	"encoding/json"
	"fmt"
)

// HiloEvents returns Hilo Challenge events for a season at a location.
// Season is an integer year (e.g. 2026 for the 2025-2026 winter program).
//
// Wire path is /challenge/v1/api/locations/{loc}/rates/hilo/seasons/{season}/events.
// The previously-named /events/hilo/{x} endpoint takes an event ID, not a
// season — see HiloEvent.
func (c *Client) HiloEvents(ctx context.Context, locationID string, season int) (json.RawMessage, error) {
	var raw json.RawMessage
	path := fmt.Sprintf("/challenge/v1/api/locations/%s/rates/hilo/seasons/%d/events", locationID, season)
	err := c.Get(ctx, path, &raw)
	return raw, err
}

// HiloEvent returns the detail of one Hilo event by its event ID.
func (c *Client) HiloEvent(ctx context.Context, locationID, eventID string) (json.RawMessage, error) {
	var raw json.RawMessage
	err := c.Get(ctx, fmt.Sprintf("/challenge/v1/api/locations/%s/events/hilo/%s", locationID, eventID), &raw)
	return raw, err
}

// EventOptOutDetails returns the opt-out detail payload for one device in
// one event.
func (c *Client) EventOptOutDetails(ctx context.Context, locationID, eventID, deviceID string) (json.RawMessage, error) {
	var raw json.RawMessage
	path := fmt.Sprintf("/challenge/v1/api/locations/%s/device/%s/event/%s/optout/details",
		locationID, deviceID, eventID)
	err := c.Get(ctx, path, &raw)
	return raw, err
}

// FlexReductionStatusLimits returns the configured Flex-D reduction limits.
func (c *Client) FlexReductionStatusLimits(ctx context.Context) (json.RawMessage, error) {
	var raw json.RawMessage
	err := c.Get(ctx, "/challenge/v1/api/locations/events/flex/reductionstatuslimits", &raw)
	return raw, err
}
