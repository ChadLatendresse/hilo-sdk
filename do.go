package hilo

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
)

// Do executes a request against the Hilo gateway. /status and a few account
// bootstrap paths are unauthenticated; everything else gets an Authorization:
// Bearer header pulled from the saved token (and refreshed if expired).
//
// If out is non-nil the response is decoded as JSON; if out is *[]byte the raw
// bytes are returned (useful for plain-text endpoints like /status/MinVersion).
func (c *Client) Do(ctx context.Context, method, urlStr string, body any, out any) error {
	auth := !isUnauthenticatedPath(urlStr)
	var tok string
	if auth {
		t, err := c.AccessToken(ctx)
		if err != nil {
			return err
		}
		tok = t
	}

	var rdr io.Reader
	if body != nil {
		switch b := body.(type) {
		case io.Reader:
			rdr = b
		case []byte:
			rdr = bytes.NewReader(b)
		case string:
			rdr = strings.NewReader(b)
		default:
			j, err := json.Marshal(body)
			if err != nil {
				return err
			}
			rdr = bytes.NewReader(j)
		}
	}

	req, err := http.NewRequestWithContext(ctx, method, urlStr, rdr)
	if err != nil {
		return err
	}
	if auth {
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	req.Header.Set("Ocp-Apim-Subscription-Key", c.SubscriptionKey)
	req.Header.Set("Accept", "application/json")
	if body != nil && req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)

	if resp.StatusCode >= 400 {
		return &APIError{Status: resp.StatusCode, Body: string(raw), URL: urlStr}
	}
	if out == nil {
		return nil
	}
	if rawOut, ok := out.(*[]byte); ok {
		*rawOut = raw
		return nil
	}
	if len(raw) == 0 {
		return nil
	}
	return json.Unmarshal(raw, out)
}

func (c *Client) Get(ctx context.Context, path string, out any) error {
	return c.Do(ctx, http.MethodGet, c.URL(path), nil, out)
}

func (c *Client) Post(ctx context.Context, path string, body, out any) error {
	return c.Do(ctx, http.MethodPost, c.URL(path), body, out)
}

func (c *Client) Put(ctx context.Context, path string, body, out any) error {
	return c.Do(ctx, http.MethodPut, c.URL(path), body, out)
}

func (c *Client) Patch(ctx context.Context, path string, body, out any) error {
	return c.Do(ctx, http.MethodPatch, c.URL(path), body, out)
}

func (c *Client) Delete(ctx context.Context, path string, out any) error {
	return c.Do(ctx, http.MethodDelete, c.URL(path), nil, out)
}

// URL resolves a relative path against the appropriate base URL.
func (c *Client) URL(p string) string {
	if strings.HasPrefix(p, "http://") || strings.HasPrefix(p, "https://") {
		return p
	}
	switch {
	case strings.HasPrefix(p, "/api/digital-twin"),
		strings.HasPrefix(p, "/api/appcue"):
		return c.PlatformBase + p
	default:
		if !strings.HasPrefix(p, "/") {
			p = "/" + p
		}
		return c.APIBase + p
	}
}

// Mirrors the bundle's _API_UNAUTHENTICATED_URL_PREFIXES list.
var unauthPaths = []string{
	"/status",
	"/Clientele/api/Account/CreateAccount",
	"/Clientele/api/Account/ActivateAccount",
	"/Clientele/api/Account/ValidateToken",
	"/Clientele/api/UserInfo/SetPassword",
}

func isUnauthenticatedPath(urlStr string) bool {
	for _, p := range unauthPaths {
		if strings.Contains(urlStr, p) {
			return true
		}
	}
	return false
}
