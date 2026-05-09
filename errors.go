package hilo

import (
	"errors"
	"fmt"
)

// APIError is returned by Do() for any non-2xx response.
type APIError struct {
	Status int
	Body   string
	URL    string
}

func (e *APIError) Error() string {
	body := e.Body
	if len(body) > 400 {
		body = body[:400] + "...(truncated)"
	}
	return fmt.Sprintf("HTTP %d %s: %s", e.Status, e.URL, body)
}

// Sentinel errors callers can match with errors.Is.
var (
	// ErrTokenExpired is returned when stored tokens have expired and re-auth
	// (refresh + ROPC fallback) failed.
	ErrTokenExpired = errors.New("hilo: token expired and re-auth failed")

	// ErrPolicyRejected is returned when a write operation reaches a terminal
	// status of REJECTED or FAILED.
	ErrPolicyRejected = errors.New("hilo: operation rejected by policy")

	// ErrEventOptedOut is returned when the user has already opted out of a
	// peak event for the device in question.
	ErrEventOptedOut = errors.New("hilo: device already opted out of event")

	// ErrSubscriptionDisconnected is returned when the GraphQL subscription
	// channel is closed by an unrecoverable error.
	ErrSubscriptionDisconnected = errors.New("hilo: subscription disconnected")
)
