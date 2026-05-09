package hilo

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestUpdateDevicePUT(t *testing.T) {
	c := NewClient()
	gotPath := ""
	gotBody := []byte(nil)
	teardown := withTestServer(t, c, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "PUT" {
			t.Errorf("method=%s", r.Method)
		}
		gotPath = r.URL.Path
		gotBody, _ = readAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"id":67890,"hiloId":"urn:hilo:philo:f000000000000001:0","identifier":"F000000000000001","name":"Renamed","type":"Thermostat","groupId":42,"category":"Heating"}`))
	})
	defer teardown()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	in := HubDevice{ID: 67890, HiloID: "urn:hilo:philo:f000000000000001:0", Name: "Renamed", GroupID: 42}
	out, err := c.UpdateDevice(ctx, 12345, in)
	if err != nil {
		t.Fatal(err)
	}
	want := "/Automation/v1/api/Locations/12345/Devices/67890"
	if gotPath != want {
		t.Errorf("path=%s, want %s", gotPath, want)
	}
	if !strings.Contains(string(gotBody), `"name":"Renamed"`) {
		t.Errorf("body missing rename: %s", gotBody)
	}
	if !strings.Contains(string(gotBody), `"groupId":42`) {
		t.Errorf("body missing groupId: %s", gotBody)
	}
	if out.Name != "Renamed" {
		t.Errorf("returned Name=%q", out.Name)
	}
}

func TestToggleDeviceFavoritePATCH(t *testing.T) {
	c := NewClient()
	gotPath := ""
	gotMethod := ""
	teardown := withTestServer(t, c, func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
	})
	defer teardown()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := c.ToggleDeviceFavorite(ctx, 12345, 67890); err != nil {
		t.Fatal(err)
	}
	if gotMethod != "PATCH" {
		t.Errorf("method=%s", gotMethod)
	}
	want := "/Automation/v1/api/Locations/12345/Devices/67890/favorite"
	if gotPath != want {
		t.Errorf("path=%s, want %s", gotPath, want)
	}
}

func TestSetDevicesFavoriteBatchPATCH(t *testing.T) {
	c := NewClient()
	gotPath := ""
	gotBody := []byte(nil)
	teardown := withTestServer(t, c, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "PATCH" {
			t.Errorf("method=%s", r.Method)
		}
		gotPath = r.URL.Path
		gotBody, _ = readAll(r.Body)
		w.WriteHeader(http.StatusOK)
	})
	defer teardown()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := c.SetDevicesFavorite(ctx, 12345, []FavoriteUpdate{
		{DeviceID: 1, IsFavorite: true},
		{DeviceID: 2, IsFavorite: false},
	}); err != nil {
		t.Fatal(err)
	}
	want := "/Automation/v1/api/Locations/12345/Devices/favorite"
	if gotPath != want {
		t.Errorf("path=%s, want %s", gotPath, want)
	}
	if !strings.Contains(string(gotBody), `"deviceId":1`) || !strings.Contains(string(gotBody), `"isFavorite":true`) {
		t.Errorf("body=%s", gotBody)
	}
}
