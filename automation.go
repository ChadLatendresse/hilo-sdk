package hilo

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// Location is one home/site associated with the authenticated account, as
// returned by /Automation/v1/api/Locations and /Locations/{id}.
type Location struct {
	ID                          int              `json:"id"`
	Name                        string           `json:"name"`
	LocationHiloID              HiloID           `json:"locationHiloId"`
	AddressID                   string           `json:"addressId"`
	CountryCode                 string           `json:"countryCode"`
	PostalCode                  string           `json:"postalCode"`
	TimeZone                    string           `json:"timeZone"`
	TemperatureFormat           string           `json:"temperatureFormat"`
	TimeFormat                  string           `json:"timeFormat"`
	GatewayCount                int              `json:"gatewayCount"`
	EnergyCostConfigured        bool             `json:"energyCostConfigured"`
	MobileAppAccessDeniedReason *string          `json:"mobileAppAccessDeniedReason"`
	CreatedUtc                  time.Time        `json:"createdUtc"`
	RatePlan                    LocationRatePlan `json:"ratePlan"`
	Raw                         json.RawMessage  `json:"-"`
}

type LocationRatePlan struct {
	Current string                  `json:"current"`
	History []LocationRatePlanEntry `json:"history"`
}

type LocationRatePlanEntry struct {
	EffectiveDate time.Time `json:"effectiveDate"`
	Name          string    `json:"name"`
}

// ListLocations returns all locations owned by the authenticated user.
func (c *Client) ListLocations(ctx context.Context) ([]Location, error) {
	var raw json.RawMessage
	if err := c.Get(ctx, "/Automation/v1/api/Locations", &raw); err != nil {
		return nil, err
	}
	var locs []Location
	if err := json.Unmarshal(raw, &locs); err != nil {
		return nil, err
	}
	return locs, nil
}

// GetLocationREST returns the detail document for a single location by its
// numeric ID. (Note: this is the REST representation, distinct from the
// GraphQL Container exposed by GetLocation.)
func (c *Client) GetLocationREST(ctx context.Context, id string) (*Location, error) {
	var raw json.RawMessage
	if err := c.Get(ctx, fmt.Sprintf("/Automation/v1/api/Locations/%s", id), &raw); err != nil {
		return nil, err
	}
	var loc Location
	if err := json.Unmarshal(raw, &loc); err != nil {
		return nil, err
	}
	loc.Raw = raw
	return &loc, nil
}

// FeatureFlags returns the feature flags enabled for one location.
// Shape varies; raw bytes are exposed as `Raw`.
type LocationFeatureFlags struct {
	Raw json.RawMessage
}

func (c *Client) LocationFeatureFlags(ctx context.Context, locationID string) (*LocationFeatureFlags, error) {
	var raw json.RawMessage
	if err := c.Get(ctx, fmt.Sprintf("/Automation/v1/api/Locations/%s/FeatureFlags", locationID), &raw); err != nil {
		return nil, err
	}
	return &LocationFeatureFlags{Raw: raw}, nil
}

// LocationWeather returns the current weather and (optionally) suntime block
// for a location. Shape unverified — exposed as raw JSON.
func (c *Client) LocationWeather(ctx context.Context, locationID string) (json.RawMessage, error) {
	var raw json.RawMessage
	err := c.Get(ctx, fmt.Sprintf("/Automation/v1/api/Locations/%s/Weather", locationID), &raw)
	return raw, err
}

func (c *Client) LocationSunTime(ctx context.Context, locationID string) (json.RawMessage, error) {
	var raw json.RawMessage
	err := c.Get(ctx, fmt.Sprintf("/Automation/v1/api/Locations/%s/Weather/Suntime", locationID), &raw)
	return raw, err
}

// Gateways returns all gateways at a location.
func (c *Client) Gateways(ctx context.Context, locationID string) (json.RawMessage, error) {
	var raw json.RawMessage
	err := c.Get(ctx, fmt.Sprintf("/Automation/v1/api/Locations/%s/Gateways", locationID), &raw)
	return raw, err
}

// Scenes returns the scenes at a location.
// Lives under PROGRAM_SERVICE_API_URL despite being thematically "automation".
func (c *Client) Scenes(ctx context.Context, locationID string) (json.RawMessage, error) {
	var raw json.RawMessage
	err := c.Get(ctx, fmt.Sprintf("/program/v1/api/locations/%s/scenes", locationID), &raw)
	return raw, err
}

// Automations returns the automation rules at a location.
func (c *Client) Automations(ctx context.Context, locationID string) (json.RawMessage, error) {
	var raw json.RawMessage
	err := c.Get(ctx, fmt.Sprintf("/program/v1/api/locations/%s/automations", locationID), &raw)
	return raw, err
}

func (c *Client) UpcomingAutomations(ctx context.Context, locationID string) (json.RawMessage, error) {
	var raw json.RawMessage
	err := c.Get(ctx, fmt.Sprintf("/program/v1/api/locations/%s/automations/upcoming", locationID), &raw)
	return raw, err
}

// LocationPreferences returns the user's default-event-parameter preferences
// for a location (per-device behaviour during peak events). Backed by
// /challenge/v1/api/locations/{id}/preferences with a required PreferenceType
// query param the bundle always sends.
func (c *Client) LocationPreferences(ctx context.Context, locationID string) (json.RawMessage, error) {
	var raw json.RawMessage
	err := c.Get(ctx, fmt.Sprintf(
		"/challenge/v1/api/locations/%s/preferences?PreferenceType=Thermostat,OtherDevices",
		locationID), &raw)
	return raw, err
}

// GetDevice returns one device's REST representation.
func (c *Client) GetDevice(ctx context.Context, locationID, deviceID string) (json.RawMessage, error) {
	var raw json.RawMessage
	err := c.Get(ctx, fmt.Sprintf("/Automation/v1/api/Locations/%s/Devices/%s", locationID, deviceID), &raw)
	return raw, err
}

// GetDeviceAttributes returns the attribute set of one device.
func (c *Client) GetDeviceAttributes(ctx context.Context, locationID, deviceID string) (json.RawMessage, error) {
	var raw json.RawMessage
	err := c.Get(ctx, fmt.Sprintf("/Automation/v1/api/Locations/%s/Devices/%s/Attributes", locationID, deviceID), &raw)
	return raw, err
}

// Groups returns all device groups at a location.
func (c *Client) Groups(ctx context.Context, locationID string) (json.RawMessage, error) {
	var raw json.RawMessage
	err := c.Get(ctx, fmt.Sprintf("/Automation/v1/api/Locations/%s/Groups", locationID), &raw)
	return raw, err
}

// GetScene returns one scene by ID.
func (c *Client) GetScene(ctx context.Context, locationID, sceneID string) (json.RawMessage, error) {
	var raw json.RawMessage
	err := c.Get(ctx, fmt.Sprintf("/program/v1/api/locations/%s/scenes/%s", locationID, sceneID), &raw)
	return raw, err
}

// GetAutomation returns one automation rule by ID.
func (c *Client) GetAutomation(ctx context.Context, locationID, automationID string) (json.RawMessage, error) {
	var raw json.RawMessage
	err := c.Get(ctx, fmt.Sprintf("/program/v1/api/locations/%s/automations/%s", locationID, automationID), &raw)
	return raw, err
}

// LocationResidence returns residence metadata (square footage, occupants, etc.).
// Backed by the Clientele service.
func (c *Client) LocationResidence(ctx context.Context, locationID string) (json.RawMessage, error) {
	var raw json.RawMessage
	err := c.Get(ctx, fmt.Sprintf("/Clientele/api/locations/%s/residence", locationID), &raw)
	return raw, err
}
