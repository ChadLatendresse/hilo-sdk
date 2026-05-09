package hilo

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// EnergyHistory returns time-series energy consumption for a location.
// Lives under AUTOMATION_API_URL despite being a "history" endpoint.
//
// Both `timescale` and `date` are required by the server. The bundle uses
// "Day" almost exclusively; "Month" and "Year" exist but currently return
// 500 from the live API. Date is sent as YYYY-MM-DD.
func (c *Client) EnergyHistory(ctx context.Context, locationID, timescale string, date time.Time) (json.RawMessage, error) {
	var raw json.RawMessage
	path := fmt.Sprintf("/Automation/v1/api/Locations/%s/History/Energy2?timescale=%s&date=%s",
		locationID, timescale, date.Format("2006-01-02"))
	err := c.Get(ctx, path, &raw)
	return raw, err
}

// LocationComparison returns coach comparison data for a location.
func (c *Client) LocationComparison(ctx context.Context, locationID string) (json.RawMessage, error) {
	var raw json.RawMessage
	err := c.Get(ctx, fmt.Sprintf("/Coach/v1/api/Locations/%s/Comparison", locationID), &raw)
	return raw, err
}

// LocationOverconsumption returns overconsumption data for a billing period.
// periodID is YYYY-MM (e.g. "2025-12"). Lives under AUTOMATION_API_URL.
func (c *Client) LocationOverconsumption(ctx context.Context, locationID, periodID string) (json.RawMessage, error) {
	var raw json.RawMessage
	err := c.Get(ctx, fmt.Sprintf("/Automation/v1/api/Locations/%s/Overconsumption/%s", locationID, periodID), &raw)
	return raw, err
}

// OverconsumptionFeatureHasRate reports whether the user has the rate-aware
// overconsumption coach enabled. Lives under COACH_API_URL.
func (c *Client) OverconsumptionFeatureHasRate(ctx context.Context, locationID string) (json.RawMessage, error) {
	var raw json.RawMessage
	err := c.Get(ctx, fmt.Sprintf("/Coach/v1/api/Locations/%s/Overconsumption/Feature/HasRate", locationID), &raw)
	return raw, err
}

// SeasonsSummary returns the seasonal summary across all rate plans.
// Backed by the Challenge service despite the "seasons" naming.
func (c *Client) SeasonsSummary(ctx context.Context, locationID string) (json.RawMessage, error) {
	var raw json.RawMessage
	err := c.Get(ctx, fmt.Sprintf("/challenge/v1/api/locations/%s/seasonssummary", locationID), &raw)
	return raw, err
}

// FlexSavings returns Flex-D savings for a location and (integer) season.
// The bundle requires the season query param.
func (c *Client) FlexSavings(ctx context.Context, locationID string, season int) (json.RawMessage, error) {
	var raw json.RawMessage
	path := fmt.Sprintf("/challenge/v1/api/locations/%s/savings/flex?season=%d", locationID, season)
	err := c.Get(ctx, path, &raw)
	return raw, err
}

// RateEvents returns the events for a given rate (ch|flex|hilo) and integer
// season at a location. Equivalent to HiloEvents when rate="hilo".
func (c *Client) RateEvents(ctx context.Context, locationID, rate string, season int) (json.RawMessage, error) {
	var raw json.RawMessage
	path := fmt.Sprintf("/challenge/v1/api/locations/%s/rates/%s/seasons/%d/events", locationID, rate, season)
	err := c.Get(ctx, path, &raw)
	return raw, err
}
