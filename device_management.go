package hilo

import (
	"context"
	"fmt"
)

// FavoriteUpdate is one entry in a SetDevicesFavorite call.
type FavoriteUpdate struct {
	DeviceID   int  `json:"deviceId"`
	IsFavorite bool `json:"isFavorite"`
}

// UpdateDevice applies metadata changes (Name, GroupID, etc.) to one device
// and returns the server's updated representation. Use the typed HubDevice
// from device_hub.go: read it via SubscribeDeviceList, mutate the fields you
// want, pass it here.
//
// Server-validated fields (id, hiloId) are preserved by the server regardless
// of what the caller passes — the URL path's deviceId is authoritative.
func (c *Client) UpdateDevice(ctx context.Context, locID int, dev HubDevice) (*HubDevice, error) {
	var out HubDevice
	path := fmt.Sprintf("/Automation/v1/api/Locations/%d/Devices/%d", locID, dev.ID)
	if err := c.Put(ctx, path, dev, &out); err != nil {
		return nil, fmt.Errorf("UpdateDevice: %w", err)
	}
	return &out, nil
}

// ToggleDeviceFavorite flips the isFavorite bit for one device. The server
// returns 200; the new state is "the opposite of whatever it was". For
// idempotent set-to-specific-value semantics, use SetDevicesFavorite.
func (c *Client) ToggleDeviceFavorite(ctx context.Context, locID, devID int) error {
	path := fmt.Sprintf("/Automation/v1/api/Locations/%d/Devices/%d/favorite", locID, devID)
	if err := c.Patch(ctx, path, nil, nil); err != nil {
		return fmt.Errorf("ToggleDeviceFavorite: %w", err)
	}
	return nil
}

// SetDevicesFavorite assigns isFavorite for multiple devices in one PATCH.
// Server applies all updates atomically.
func (c *Client) SetDevicesFavorite(ctx context.Context, locID int, updates []FavoriteUpdate) error {
	if len(updates) == 0 {
		return fmt.Errorf("SetDevicesFavorite: no updates")
	}
	path := fmt.Sprintf("/Automation/v1/api/Locations/%d/Devices/favorite", locID)
	if err := c.Patch(ctx, path, updates, nil); err != nil {
		return fmt.Errorf("SetDevicesFavorite: %w", err)
	}
	return nil
}
