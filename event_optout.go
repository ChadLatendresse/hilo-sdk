package hilo

import (
	"context"
	"encoding/json"
	"fmt"
)

// DeviceOptoutDetails describes one device's opt-out state for one
// Hilo Challenge event. Returned by GetDeviceOptoutDetails.
//
// The shape is best-guess — the GET endpoint only fires for active
// events (heating season). Raw exposes the full payload for fields
// that haven't been typed yet.
type DeviceOptoutDetails struct {
	DeviceID int             `json:"deviceId"`
	EventID  string          `json:"eventId"`
	OptedOut bool            `json:"optedOut,omitempty"`
	Raw      json.RawMessage `json:"-"`
}

// GetDeviceOptoutDetails returns the current opt-out state of a device
// for a specific Hilo Challenge event.
func (c *Client) GetDeviceOptoutDetails(ctx context.Context, locID, devID int, eventID string) (*DeviceOptoutDetails, error) {
	var raw json.RawMessage
	path := fmt.Sprintf("/challenge/v1/api/locations/%d/device/%d/event/%s/optout/details", locID, devID, eventID)
	if err := c.Get(ctx, path, &raw); err != nil {
		return nil, fmt.Errorf("GetDeviceOptoutDetails: %w", err)
	}
	var d DeviceOptoutDetails
	if err := json.Unmarshal(raw, &d); err != nil {
		return nil, fmt.Errorf("GetDeviceOptoutDetails decode: %w", err)
	}
	d.Raw = raw
	return &d, nil
}

// OptOutDevice opts a single device out of a single Hilo Challenge event.
// The server records the opt-out and the device will not participate in
// load reduction during the event window.
func (c *Client) OptOutDevice(ctx context.Context, locID int, eventID string, devID int) error {
	path := fmt.Sprintf("/GDService/v1/api/locations/%d/events/%s/Devices/%d/Optout", locID, eventID, devID)
	if err := c.Post(ctx, path, nil, nil); err != nil {
		return fmt.Errorf("OptOutDevice: %w", err)
	}
	return nil
}

// LocationPreferences is the body shape for SetLocationPreferences.
// The GET on /challenge/.../preferences returns a similar shape;
// PreferenceType is "Thermostat" or "OtherDevices" (verified via the REST
// integration during fix commit eea36c9).
//
// Additional fields that the live endpoint accepts will be added once a
// real GET response is captured during heating season; until then the
// Raw escape hatch is the way to set fields beyond OptOut.
type LocationPreferences struct {
	PreferenceType string          `json:"preferenceType"`
	OptOut         bool            `json:"optOut,omitempty"`
	Raw            json.RawMessage `json:"-"`
}

// SetLocationPreferences PUTs to /challenge/v1/api/locations/{loc}/preferences
// to set the location's default opt-out behavior for future Hilo Challenge
// events.
func (c *Client) SetLocationPreferences(ctx context.Context, locID int, prefs LocationPreferences) error {
	path := fmt.Sprintf("/challenge/v1/api/locations/%d/preferences", locID)
	if err := c.Put(ctx, path, prefs, nil); err != nil {
		return fmt.Errorf("SetLocationPreferences: %w", err)
	}
	return nil
}
