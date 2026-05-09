package hilo

import (
	"errors"
	"testing"
)

func TestAPIErrorFormat(t *testing.T) {
	t.Parallel()
	e := &APIError{Status: 404, URL: "https://api.example.com/x", Body: "not found"}
	got := e.Error()
	if got == "" {
		t.Error("empty error")
	}
}

func TestErrorSentinelsWrap(t *testing.T) {
	t.Parallel()
	wrapped := errors.Join(ErrTokenExpired, errors.New("downstream"))
	if !errors.Is(wrapped, ErrTokenExpired) {
		t.Error("expected ErrTokenExpired via errors.Is")
	}
}
