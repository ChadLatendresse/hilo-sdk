package hilo

import (
	"context"
	"encoding/json"
	"fmt"
)

// Scene is the typed shape for the GET /scenes endpoint.
// Existing Scenes/GetScene calls keep returning RawMessage for
// backwards compatibility; this typed struct is offered for callers
// who want it.
type Scene struct {
	ID            int             `json:"id,omitempty"`
	Name          string          `json:"name"`
	LocationID    int             `json:"locationId,omitempty"`
	DeviceActions []SceneAction   `json:"deviceActions,omitempty"`
	Raw           json.RawMessage `json:"-"`
}

// SceneAction is one device action within a scene. The Attributes map is
// keyed on AttributeType wire names (same as SetAttributes write payloads);
// values are typed by attribute (number, string, etc.).
type SceneAction struct {
	DeviceID   int             `json:"deviceId"`
	Attributes map[string]any  `json:"attributes,omitempty"`
	Raw        json.RawMessage `json:"-"`
}

// CreateScene creates a scene at this location. The input Scene's ID
// field should be zero; the server assigns it and returns it in the
// response.
func (c *Client) CreateScene(ctx context.Context, locID int, scene Scene) (*Scene, error) {
	var out Scene
	path := fmt.Sprintf("/program/v1/api/locations/%d/scenes", locID)
	if err := c.Post(ctx, path, scene, &out); err != nil {
		return nil, fmt.Errorf("CreateScene: %w", err)
	}
	return &out, nil
}

// UpdateScene replaces a scene's fields with the input. The Scene's ID
// must be set.
func (c *Client) UpdateScene(ctx context.Context, locID int, scene Scene) (*Scene, error) {
	if scene.ID == 0 {
		return nil, fmt.Errorf("UpdateScene: scene.ID is zero")
	}
	var out Scene
	path := fmt.Sprintf("/program/v1/api/locations/%d/scenes/%d", locID, scene.ID)
	if err := c.Put(ctx, path, scene, &out); err != nil {
		return nil, fmt.Errorf("UpdateScene: %w", err)
	}
	return &out, nil
}

// DeleteScene removes a scene. Returns nil on success; an error if the
// scene doesn't exist or the user lacks permission.
func (c *Client) DeleteScene(ctx context.Context, locID, sceneID int) error {
	path := fmt.Sprintf("/program/v1/api/locations/%d/scenes/%d", locID, sceneID)
	if err := c.Delete(ctx, path, nil); err != nil {
		return fmt.Errorf("DeleteScene: %w", err)
	}
	return nil
}

// ActivateScene executes (activates) a previously-defined scene at this
// location. The server fans out attribute writes to every device in the
// scene; per-device operationIds are not surfaced at the scene-level
// call (the call returns as soon as the server accepts the execute
// request).
//
// To monitor what happens after activation, observe a SubscribeDeviceValues
// stream and watch the values change.
func (c *Client) ActivateScene(ctx context.Context, locID, sceneID int) error {
	path := fmt.Sprintf("/program/v1/api/locations/%d/scenes/%d/execute", locID, sceneID)
	if err := c.Post(ctx, path, nil, nil); err != nil {
		return fmt.Errorf("ActivateScene: %w", err)
	}
	return nil
}
