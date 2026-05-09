package hilo

import (
	"context"
	"encoding/json"
)

// AccountInfo returns the authenticated user's account record. Shape varies;
// raw payload is preserved.
type AccountInfo struct {
	Raw json.RawMessage
}

// Account fetches the authenticated user's record. The exact path is
// /Clientele/api/Account though the body shape isn't yet typed; surfaced as raw.
func (c *Client) Account(ctx context.Context) (*AccountInfo, error) {
	var raw json.RawMessage
	if err := c.Get(ctx, "/Clientele/api/Account", &raw); err != nil {
		return nil, err
	}
	return &AccountInfo{Raw: raw}, nil
}

// AccountPayments returns payment history.
func (c *Client) AccountPayments(ctx context.Context) (json.RawMessage, error) {
	var raw json.RawMessage
	err := c.Get(ctx, "/Clientele/api/account/payments", &raw)
	return raw, err
}
