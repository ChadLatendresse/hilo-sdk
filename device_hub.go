package hilo

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// HubDevice is DeviceHub's flat device representation. Distinct from the REST
// GraphQL Device union — different fields, no __typename. Use Type plus
// SupportedAttributesList to interpret the device.
type HubDevice struct {
	ID                      int      `json:"id"`
	HiloID                  HiloID   `json:"hiloId"`
	Identifier              string   `json:"identifier"`
	Name                    string   `json:"name"`
	Type                    string   `json:"type"` // "Meter", "Thermostat", "Light", ...
	GroupID                 int      `json:"groupId,omitempty"`
	Category                string   `json:"category"` // "Other", "Heating", "Lighting", ...
	ModelNumber             string   `json:"modelNumber"`
	ExternalGroup           string   `json:"externalGroup"`
	Provider                int      `json:"provider"`
	IsFavorite              bool     `json:"isFavorite"`
	ETag                    string   `json:"eTag"`
	SupportedAttributesList []string `json:"supportedAttributesList"`
	SettableAttributesList  []string `json:"settableAttributesList"`
	SupportedParametersList []string `json:"supportedParametersList"`
}

// DeviceListUpdateKind classifies a DeviceHub list-side push.
type DeviceListUpdateKind int

const (
	DeviceListSnapshot DeviceListUpdateKind = iota // DeviceListInitialValuesReceived
	DeviceListDelta                                // DeviceListUpdatedValuesReceived
	DeviceListAdded                                // DeviceAdded
	DeviceListDeleted                              // DeviceDeleted
)

func (k DeviceListUpdateKind) String() string {
	switch k {
	case DeviceListSnapshot:
		return "Snapshot"
	case DeviceListDelta:
		return "Delta"
	case DeviceListAdded:
		return "Added"
	case DeviceListDeleted:
		return "Deleted"
	default:
		return "Unknown"
	}
}

// DeviceListUpdate is one push from DeviceHub's list-side methods. Snapshots
// carry the full device list; Deltas carry changed entries; Added/Deleted
// carry exactly one device.
type DeviceListUpdate struct {
	Kind    DeviceListUpdateKind
	Devices []HubDevice
}

// DeviceValuesUpdate carries one push of attribute-value updates from
// DevicesValuesReceived. Each Value is one (deviceId, attribute, value)
// observation. The device-control layer reuses this stream for operationId-keyed
// completion replies on attribute writes.
type DeviceValuesUpdate struct {
	Initial bool // currently always true; refine when wire shape is observed
	Values  []DeviceAttrValue
}

// DeviceAttrValue is intentionally schema-light: the value type varies by
// attribute (Power is a number, ThermostatMode is a string, etc.) so we
// surface the raw payload via Value alongside parsed fields.
type DeviceAttrValue struct {
	DeviceID      int             `json:"deviceId"`
	AttributeType string          `json:"attributeType"`
	Value         json.RawMessage `json:"value"`
	OperationID   string          `json:"operationId,omitempty"`
	Timestamp     time.Time       `json:"timestamp,omitempty"`
}

// SubscribeDeviceList subscribes to DeviceHub's list-side server-pushes
// (snapshots + deltas + add/delete) for one location. The returned Stream
// closes when ctx is cancelled or the connection terminally fails.
//
// Note on locID type: DeviceHub uses the integer location ID, not the
// HiloID. The bundle's currentHubLocationId carries the int form.
func (c *Client) SubscribeDeviceList(ctx context.Context, locID int) (*Stream[DeviceListUpdate], error) {
	stream := newStream[DeviceListUpdate](64)
	sinks := []*subSink{}

	subArgs := []any{locID}

	register := func(h *hubConn) {
		rejoin := func() error {
			res := <-h.client.Invoke("SubscribeToLocation", subArgs...)
			return res.err
		}
		sinks = append(sinks,
			h.addSinkWithRejoin("DeviceListInitialValuesReceived", nil,
				func(args []json.RawMessage) {
					devs, err := decodeHubDevices(args)
					if err != nil {
						return
					}
					stream.deliver(DeviceListUpdate{Kind: DeviceListSnapshot, Devices: devs})
				}, rejoin),
			h.addSink("DeviceListUpdatedValuesReceived", nil,
				func(args []json.RawMessage) {
					devs, err := decodeHubDevices(args)
					if err != nil {
						return
					}
					stream.deliver(DeviceListUpdate{Kind: DeviceListDelta, Devices: devs})
				}),
			h.addSink("DeviceAdded", nil,
				func(args []json.RawMessage) {
					devs, err := decodeHubDevices(args)
					if err != nil {
						return
					}
					stream.deliver(DeviceListUpdate{Kind: DeviceListAdded, Devices: devs})
				}),
			h.addSink("DeviceDeleted", nil,
				func(args []json.RawMessage) {
					devs, err := decodeHubDevices(args)
					if err != nil {
						return
					}
					stream.deliver(DeviceListUpdate{Kind: DeviceListDeleted, Devices: devs})
				}),
		)
	}

	if err := c.startSubscription(ctx, hubDevice, stream, &sinks, register,
		"SubscribeToLocation", subArgs,
		"UnsubscribeFromLocation", subArgs); err != nil {
		return nil, err
	}
	return stream, nil
}

// SubscribeDeviceValues subscribes to DeviceHub's DevicesValuesReceived
// pushes — live attribute value updates. Decoder uses json.RawMessage for
// the value field as an escape hatch since the exact wire shape per
// attribute type is not yet captured (DevicesValuesReceived only fires when
// a device value changes).
//
// Sharing the connection with SubscribeDeviceList: both invocations send
// SubscribeToLocation/UnsubscribeFromLocation independently. The server
// is expected to be idempotent for both.
func (c *Client) SubscribeDeviceValues(ctx context.Context, locID int) (*Stream[DeviceValuesUpdate], error) {
	stream := newStream[DeviceValuesUpdate](64)
	sinks := []*subSink{}

	subArgs := []any{locID}

	register := func(h *hubConn) {
		rejoin := func() error {
			res := <-h.client.Invoke("SubscribeToLocation", subArgs...)
			return res.err
		}
		sinks = append(sinks,
			h.addSinkWithRejoin("DevicesValuesReceived", nil,
				func(args []json.RawMessage) {
					values, err := decodeDeviceAttrValues(args)
					if err != nil {
						return
					}
					stream.deliver(DeviceValuesUpdate{Initial: true, Values: values})
				}, rejoin),
		)
	}

	if err := c.startSubscription(ctx, hubDevice, stream, &sinks, register,
		"SubscribeToLocation", subArgs,
		"UnsubscribeFromLocation", subArgs); err != nil {
		return nil, err
	}
	return stream, nil
}

// decodeHubDevices accepts either an array (Initial/Updated) or a single
// device (Added/Deleted) in args[0] and returns a []HubDevice.
func decodeHubDevices(args []json.RawMessage) ([]HubDevice, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("decodeHubDevices: no args")
	}
	var devs []HubDevice
	if err := json.Unmarshal(args[0], &devs); err == nil {
		return devs, nil
	}
	var d HubDevice
	if err := json.Unmarshal(args[0], &d); err != nil {
		return nil, fmt.Errorf("decode device: %w", err)
	}
	return []HubDevice{d}, nil
}

// decodeDeviceAttrValues accepts an array of DeviceAttrValue in args[0].
// Wire shape unverified; revise if a real fixture lands and disagrees.
func decodeDeviceAttrValues(args []json.RawMessage) ([]DeviceAttrValue, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("decodeDeviceAttrValues: no args")
	}
	var values []DeviceAttrValue
	if err := json.Unmarshal(args[0], &values); err != nil {
		return nil, fmt.Errorf("decode attr values: %w", err)
	}
	return values, nil
}
