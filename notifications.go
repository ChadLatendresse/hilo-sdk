package hilo

import (
	"context"
	"encoding/json"
	"fmt"
)

// LocationNotifications returns notifications scoped to a location.
//
// The path is intentionally doubled: the bundle's NOTIFICATION_SERVICE_URL
// constant resolves to "${API_BASE}/Notifications" and call sites then append
// "/Notifications/Locations/{id}", so the real wire path is
// /Notifications/Notifications/Locations/{id}. Hitting the un-doubled form
// returns 404.
func (c *Client) LocationNotifications(ctx context.Context, locationID string) (json.RawMessage, error) {
	var raw json.RawMessage
	err := c.Get(ctx, fmt.Sprintf("/Notifications/Notifications/Locations/%s", locationID), &raw)
	return raw, err
}

// MarkNotificationViewed and MarkDeviceNotificationViewed PUT to mark
// notifications as read, which would mutate server state. They live
// alongside the other write methods.
