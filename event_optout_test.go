package hilo

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestGetDeviceOptoutDetails(t *testing.T) {
	c := NewClient()
	teardown := withTestServer(t, c, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			t.Errorf("method=%s", r.Method)
		}
		want := "/challenge/v1/api/locations/12345/device/67890/event/evt-1/optout/details"
		if r.URL.Path != want {
			t.Errorf("path=%s, want %s", r.URL.Path, want)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"deviceId":67890,"eventId":"evt-1","optedOut":false}`))
	})
	defer teardown()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	d, err := c.GetDeviceOptoutDetails(ctx, 12345, 67890, "evt-1")
	if err != nil {
		t.Fatal(err)
	}
	if d.DeviceID != 67890 {
		t.Errorf("DeviceID=%d", d.DeviceID)
	}
	if d.EventID != "evt-1" {
		t.Errorf("EventID=%q", d.EventID)
	}
	if d.OptedOut {
		t.Error("OptedOut should be false from fixture")
	}
}

func TestOptOutDevicePost(t *testing.T) {
	c := NewClient()
	gotPath := ""
	teardown := withTestServer(t, c, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Errorf("method=%s", r.Method)
		}
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
	})
	defer teardown()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := c.OptOutDevice(ctx, 12345, "evt-1", 67890); err != nil {
		t.Fatal(err)
	}
	want := "/GDService/v1/api/locations/12345/events/evt-1/Devices/67890/Optout"
	if !strings.HasSuffix(gotPath, want) {
		t.Errorf("path=%s, want suffix %s", gotPath, want)
	}
}

func TestSetLocationPreferences(t *testing.T) {
	c := NewClient()
	gotPath := ""
	gotBody := ""
	teardown := withTestServer(t, c, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "PUT" {
			t.Errorf("method=%s", r.Method)
		}
		gotPath = r.URL.Path
		body, _ := readAll(r.Body)
		gotBody = string(body)
		w.WriteHeader(http.StatusOK)
	})
	defer teardown()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := c.SetLocationPreferences(ctx, 12345, LocationPreferences{
		PreferenceType: "Thermostat",
		OptOut:         true,
	}); err != nil {
		t.Fatal(err)
	}
	want := "/challenge/v1/api/locations/12345/preferences"
	if !strings.HasSuffix(gotPath, want) {
		t.Errorf("path=%s, want suffix %s", gotPath, want)
	}
	if !strings.Contains(gotBody, `"preferenceType":"Thermostat"`) {
		t.Errorf("body=%s", gotBody)
	}
	if !strings.Contains(gotBody, `"optOut":true`) {
		t.Errorf("body=%s", gotBody)
	}
}
