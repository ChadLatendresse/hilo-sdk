package hilo

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestActivateScene(t *testing.T) {
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

	if err := c.ActivateScene(ctx, 12345, 4321); err != nil {
		t.Fatal(err)
	}
	want := "/program/v1/api/locations/12345/scenes/4321/execute"
	if !strings.HasSuffix(gotPath, want) {
		t.Errorf("path=%s, want suffix %s", gotPath, want)
	}
}

func TestCreateScenePost(t *testing.T) {
	c := NewClient()
	gotPath := ""
	gotBody := []byte(nil)
	teardown := withTestServer(t, c, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Errorf("method=%s", r.Method)
		}
		gotPath = r.URL.Path
		gotBody, _ = readAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"id":4321,"name":"Bedtime","locationId":12345,"deviceActions":[]}`))
	})
	defer teardown()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	out, err := c.CreateScene(ctx, 12345, Scene{Name: "Bedtime"})
	if err != nil {
		t.Fatal(err)
	}
	wantPath := "/program/v1/api/locations/12345/scenes"
	if gotPath != wantPath {
		t.Errorf("path=%s, want %s", gotPath, wantPath)
	}
	if !strings.Contains(string(gotBody), `"name":"Bedtime"`) {
		t.Errorf("body missing name: %s", gotBody)
	}
	if out.ID != 4321 {
		t.Errorf("returned ID=%d, want 4321", out.ID)
	}
	if out.Name != "Bedtime" {
		t.Errorf("returned Name=%q", out.Name)
	}
}

func TestUpdateScenePut(t *testing.T) {
	c := NewClient()
	gotPath := ""
	teardown := withTestServer(t, c, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "PUT" {
			t.Errorf("method=%s", r.Method)
		}
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"id":4321,"name":"Renamed Scene","locationId":12345}`))
	})
	defer teardown()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	out, err := c.UpdateScene(ctx, 12345, Scene{ID: 4321, Name: "Renamed Scene"})
	if err != nil {
		t.Fatal(err)
	}
	want := "/program/v1/api/locations/12345/scenes/4321"
	if gotPath != want {
		t.Errorf("path=%s, want %s", gotPath, want)
	}
	if out.Name != "Renamed Scene" {
		t.Errorf("returned Name=%q", out.Name)
	}
}

func TestUpdateSceneRejectsZeroID(t *testing.T) {
	c := NewClient()
	ctx := context.Background()
	if _, err := c.UpdateScene(ctx, 12345, Scene{Name: "no-id"}); err == nil {
		t.Error("expected error for scene.ID=0, got nil")
	}
}

func TestDeleteSceneDelete(t *testing.T) {
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

	if err := c.DeleteScene(ctx, 12345, 4321); err != nil {
		t.Fatal(err)
	}
	if gotMethod != "DELETE" {
		t.Errorf("method=%s", gotMethod)
	}
	want := "/program/v1/api/locations/12345/scenes/4321"
	if gotPath != want {
		t.Errorf("path=%s, want %s", gotPath, want)
	}
}

func TestSceneRoundTripJSON(t *testing.T) {
	in := Scene{
		ID:         42,
		Name:       "Test",
		LocationID: 12345,
		DeviceActions: []SceneAction{
			{DeviceID: 100, Attributes: map[string]any{"Power": 100.0}},
		},
	}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	var out Scene
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	if out.ID != 42 || out.Name != "Test" || len(out.DeviceActions) != 1 {
		t.Errorf("round-trip mismatch: %+v", out)
	}
}
