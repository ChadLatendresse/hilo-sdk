package hilo

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHiloEventsCalls(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/challenge/v1/api/locations/10000/rates/hilo/seasons/2026/events" {
			t.Errorf("path=%s", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"events":[],"season":2026}`))
	}))
	t.Cleanup(srv.Close)
	c := NewClient()
	c.APIBase = srv.URL
	c.token = &TokenSet{AccessToken: "fake", RefreshToken: "fake", ExpiresAt: timeFutureMinutes(60)}
	c.Store = nullStore{}
	if _, err := c.HiloEvents(context.Background(), "10000", 2026); err != nil {
		t.Fatalf("err: %v", err)
	}
}
