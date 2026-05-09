package hilo

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestLocationNotificationsCalls(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/Notifications/Notifications/Locations/10000" {
			t.Errorf("path=%s", r.URL.Path)
		}
		_, _ = w.Write([]byte(`[]`))
	}))
	t.Cleanup(srv.Close)
	c := NewClient()
	c.APIBase = srv.URL
	c.token = &TokenSet{AccessToken: "fake", RefreshToken: "fake", ExpiresAt: timeFutureMinutes(60)}
	c.Store = nullStore{}
	if _, err := c.LocationNotifications(context.Background(), "10000"); err != nil {
		t.Fatalf("err: %v", err)
	}
}
