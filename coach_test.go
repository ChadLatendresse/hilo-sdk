package hilo

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestEnergyHistoryCalls(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/Automation/v1/api/Locations/10000/History/Energy2" {
			t.Errorf("path=%s", r.URL.Path)
		}
		if got, want := r.URL.Query().Get("timescale"), "Day"; got != want {
			t.Errorf("timescale=%q, want %q", got, want)
		}
		if got, want := r.URL.Query().Get("date"), "2026-05-01"; got != want {
			t.Errorf("date=%q, want %q", got, want)
		}
		_, _ = w.Write([]byte(`{}`))
	}))
	t.Cleanup(srv.Close)
	c := NewClient()
	c.APIBase = srv.URL
	c.token = &TokenSet{AccessToken: "fake", RefreshToken: "fake", ExpiresAt: timeFutureMinutes(60)}
	c.Store = nullStore{}
	d := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	if _, err := c.EnergyHistory(context.Background(), "10000", "Day", d); err != nil {
		t.Fatalf("err: %v", err)
	}
}
