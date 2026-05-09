package hilo

import (
	"context"
	"encoding/json"
)

// MinVersion fetches the minimum supported app version. Endpoint returns plain
// text, not JSON.
func (c *Client) MinVersion(ctx context.Context) (string, error) {
	var raw []byte
	if err := c.Get(ctx, "/status/MinVersion", &raw); err != nil {
		return "", err
	}
	return string(raw), nil
}

// NotificationAlert returns the current global notification banner (if any).
func (c *Client) NotificationAlert(ctx context.Context) (json.RawMessage, error) {
	var out json.RawMessage
	err := c.Get(ctx, "/status/notification-alert", &out)
	return out, err
}
