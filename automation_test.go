package hilo

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// shared test helpers (visible to every *_test.go file in this package)

func timeFutureMinutes(m int) time.Time {
	return time.Now().Add(time.Duration(m) * time.Minute)
}

type nullStore struct{}

func (nullStore) Load() (*TokenSet, error) { return nil, ErrTokenExpired }
func (nullStore) Save(*TokenSet) error     { return nil }

func newFixtureClient(t *testing.T, path string, fixture string) *Client {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != path {
			t.Errorf("unexpected path: %s (want %s)", r.URL.Path, path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(loadFixture(t, fixture))
	}))
	t.Cleanup(srv.Close)
	c := NewClient()
	c.APIBase = srv.URL
	// Inject a fake token so AccessToken doesn't try to log in.
	c.token = &TokenSet{AccessToken: "fake", RefreshToken: "fake", ExpiresAt: timeFutureMinutes(60)}
	c.Store = nullStore{}
	return c
}

func TestListLocations(t *testing.T) {
	t.Parallel()
	c := newFixtureClient(t, "/Automation/v1/api/Locations", "locations.json")
	got, err := c.ListLocations(context.Background())
	if err != nil {
		t.Fatalf("ListLocations: %v", err)
	}
	if len(got) == 0 {
		t.Fatal("no locations")
	}
	if got[0].LocationHiloID == "" {
		t.Error("locationHiloId empty")
	}
}

func TestGetLocationREST(t *testing.T) {
	t.Parallel()
	c := newFixtureClient(t, "/Automation/v1/api/Locations/10000", "location.json")
	got, err := c.GetLocationREST(context.Background(), "10000")
	if err != nil {
		t.Fatalf("GetLocationREST: %v", err)
	}
	if got.ID == 0 {
		t.Error("id zero")
	}
}

func TestLocationFeatureFlagsCalls(t *testing.T) {
	t.Parallel()
	called := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		if r.URL.Path != "/Automation/v1/api/Locations/10000/FeatureFlags" {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"someFlag":true}`))
	}))
	t.Cleanup(srv.Close)
	c := NewClient()
	c.APIBase = srv.URL
	c.token = &TokenSet{AccessToken: "fake", RefreshToken: "fake", ExpiresAt: timeFutureMinutes(60)}
	c.Store = nullStore{}

	got, err := c.LocationFeatureFlags(context.Background(), "10000")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !called {
		t.Error("upstream not called")
	}
	if string(got.Raw) != `{"someFlag":true}` {
		t.Errorf("raw=%s", got.Raw)
	}
}
