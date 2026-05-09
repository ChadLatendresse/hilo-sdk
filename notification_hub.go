package hilo

import (
	"context"
	"encoding/json"
	"fmt"
)

// Notification is a user-facing alert pushed by NotificationHub.
// Wire shape is unverified (no live captures during the May window);
// fields are best-guess from the REST endpoint shape. Unknown fields
// are preserved in the raw message via json.RawMessage embedding if
// needed; for now we surface the common set and leave the rest opaque.
type Notification struct {
	ID        string `json:"id"`
	Type      string `json:"type,omitempty"`
	Title     string `json:"title,omitempty"`
	Body      string `json:"body,omitempty"`
	IsRead    bool   `json:"isRead,omitempty"`
	IsViewed  bool   `json:"isViewed,omitempty"`
	CreatedAt string `json:"createdAt,omitempty"`
}

// NotificationEventKind classifies one push from NotificationHub.
type NotificationEventKind int

const (
	NotifSnapshot NotificationEventKind = iota // initial list after subscribe
	NotifAdded                                 // new alert
	NotifUpdated                               // status change (read/viewed)
)

func (k NotificationEventKind) String() string {
	switch k {
	case NotifSnapshot:
		return "Snapshot"
	case NotifAdded:
		return "Added"
	case NotifUpdated:
		return "Updated"
	default:
		return "Unknown"
	}
}

// NotificationEvent is one push from NotificationHub.
type NotificationEvent struct {
	Kind         NotificationEventKind
	Notification Notification
}

// SubscribeNotifications subscribes to the user's notification stream.
// Each user has a single, account-wide stream; locID is not required.
//
// Server-pushed method names registered here are best-guess from the
// bundle (NotificationReceived, NotificationsListReceived,
// NotificationUpdated); they will be verified when a real notification
// fires. Unknown targets are logged via Client.Logger if set (see
// signalRClient.handleFrame).
func (c *Client) SubscribeNotifications(ctx context.Context) (*Stream[NotificationEvent], error) {
	stream := newStream[NotificationEvent](16)
	sinks := []*subSink{}

	register := func(h *hubConn) {
		sinks = append(sinks,
			h.addSink("NotificationsListReceived", nil, func(args []json.RawMessage) {
				if len(args) == 0 {
					return
				}
				var list []Notification
				if err := json.Unmarshal(args[0], &list); err != nil {
					return
				}
				if len(list) == 0 {
					stream.deliver(NotificationEvent{Kind: NotifSnapshot})
					return
				}
				for _, n := range list {
					stream.deliver(NotificationEvent{Kind: NotifSnapshot, Notification: n})
				}
			}),
			h.addSink("NotificationReceived", nil, func(args []json.RawMessage) {
				ev, err := decodeNotification(args, NotifAdded)
				if err != nil {
					return
				}
				stream.deliver(ev)
			}),
			h.addSink("NotificationUpdated", nil, func(args []json.RawMessage) {
				ev, err := decodeNotification(args, NotifUpdated)
				if err != nil {
					return
				}
				stream.deliver(ev)
			}),
		)
	}

	if err := c.startSubscription(ctx, hubNotification, stream, &sinks, register,
		"", nil, "", nil); err != nil {
		return nil, err
	}
	return stream, nil
}

// decodeNotification decodes a single-notification push (Added/Updated).
// args[0] must be a JSON object representing one Notification.
func decodeNotification(args []json.RawMessage, kind NotificationEventKind) (NotificationEvent, error) {
	if len(args) == 0 {
		return NotificationEvent{}, fmt.Errorf("decodeNotification: no args")
	}
	var n Notification
	if err := json.Unmarshal(args[0], &n); err != nil {
		return NotificationEvent{}, err
	}
	return NotificationEvent{Kind: kind, Notification: n}, nil
}

// invokeOnHub is a helper for one-shot client→server methods that don't
// require an active subscription. Acquires the hub, invokes, releases.
func (c *Client) invokeOnHub(ctx context.Context, k hubKind, method string, args ...any) error {
	h, err := c.acquireHub(ctx, k)
	if err != nil {
		return err
	}
	defer c.releaseHub(k)
	select {
	case res := <-h.client.Invoke(method, args...):
		return res.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// MarkNotificationRead marks one notification as read.
func (c *Client) MarkNotificationRead(ctx context.Context, id string) error {
	return c.invokeOnHub(ctx, hubNotification, "MarkAsRead", id)
}

// MarkAllNotificationsRead marks all of the user's notifications as read.
func (c *Client) MarkAllNotificationsRead(ctx context.Context) error {
	return c.invokeOnHub(ctx, hubNotification, "MarkAllAsRead")
}

// MarkNotificationViewed marks one notification as viewed.
func (c *Client) MarkNotificationViewed(ctx context.Context, id string) error {
	return c.invokeOnHub(ctx, hubNotification, "MarkAsViewed", id)
}

// MarkAllNotificationsViewed marks all of the user's notifications as viewed.
func (c *Client) MarkAllNotificationsViewed(ctx context.Context) error {
	return c.invokeOnHub(ctx, hubNotification, "MarkAllAsViewed")
}
