package hilo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"os/user"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

type TokenSet struct {
	AccessToken  string    `json:"access_token"`
	RefreshToken string    `json:"refresh_token"`
	IDToken      string    `json:"id_token,omitempty"`
	ExpiresAt    time.Time `json:"expires_at"`
	Scope        string    `json:"scope,omitempty"`
}

func (t *TokenSet) Expired() bool {
	return t == nil || time.Now().After(t.ExpiresAt.Add(-30*time.Second))
}

type TokenStore interface {
	Load() (*TokenSet, error)
	Save(*TokenSet) error
}

type FileStore struct {
	Path string
}

func DefaultStorePath() string {
	return filepath.Join(configDir(), "tokens.json")
}

// configDir returns ~/.config/hilo, resolving to the original user's home when
// the CLI runs under sudo (so login on :443 doesn't write into /root).
func configDir() string {
	home := ""
	if u := os.Getenv("SUDO_USER"); u != "" && u != "root" {
		if usr, err := user.Lookup(u); err == nil {
			home = usr.HomeDir
		}
	}
	if home == "" {
		home, _ = os.UserHomeDir()
	}
	return filepath.Join(home, ".config", "hilo")
}

func (s *FileStore) Load() (*TokenSet, error) {
	b, err := os.ReadFile(s.Path)
	if err != nil {
		return nil, err
	}
	var t TokenSet
	if err := json.Unmarshal(b, &t); err != nil {
		return nil, err
	}
	return &t, nil
}

func (s *FileStore) Save(t *TokenSet) error {
	if err := os.MkdirAll(filepath.Dir(s.Path), 0o700); err != nil {
		return err
	}
	b, _ := json.MarshalIndent(t, "", "  ")
	if err := os.WriteFile(s.Path, b, 0o600); err != nil {
		return err
	}
	chownToSudoUser(s.Path)
	return nil
}

// chownToSudoUser hands ownership back to the invoking user when running under
// sudo. Best-effort; ignored on errors.
func chownToSudoUser(path string) {
	uid := os.Getenv("SUDO_UID")
	gid := os.Getenv("SUDO_GID")
	if uid == "" || gid == "" {
		return
	}
	var uidI, gidI int
	fmt.Sscanf(uid, "%d", &uidI)
	fmt.Sscanf(gid, "%d", &gidI)
	_ = os.Chown(path, uidI, gidI)
	_ = os.Chown(filepath.Dir(path), uidI, gidI)
}

type tokenResponse struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	IDToken      string `json:"id_token"`
	ExpiresIn    int    `json:"expires_in"`
	Scope        string `json:"scope"`
	TokenType    string `json:"token_type"`
}

func (tr tokenResponse) toTokenSet(prevRefresh string) *TokenSet {
	rt := tr.RefreshToken
	if rt == "" {
		rt = prevRefresh
	}
	return &TokenSet{
		AccessToken:  tr.AccessToken,
		RefreshToken: rt,
		IDToken:      tr.IDToken,
		ExpiresAt:    time.Now().Add(time.Duration(tr.ExpiresIn) * time.Second),
		Scope:        tr.Scope,
	}
}

// loginScrape walks B2C's self-asserted custom-policy login flow:
//
//   - GET the authorize URL → land on the self-asserted login page →
//     parse the inline `var SETTINGS = {...}` JS object for csrf, transId,
//     hosts.tenant, and the API name.
//   - POST signInName + password to /SelfAsserted?tx=&p= with X-CSRF-TOKEN.
//   - GET /api/<api>/confirmed?csrf_token=&tx=&p= → 302 to the redirect URI
//     with the auth code. We never actually hit the redirect host — the HTTP
//     client's CheckRedirect intercepts and we read the Location header.
//   - Exchange the code for tokens via the standard auth-code+PKCE flow.
//
// No DNS spoofing, no /etc/hosts entry, no browser, no Chromium.
func (c *Client) loginScrape(ctx context.Context, email, password string) (*TokenSet, error) {
	pkce, err := NewPKCE()
	if err != nil {
		return nil, err
	}
	state, err := randString(24)
	if err != nil {
		return nil, err
	}
	nonce, err := randString(24)
	if err != nil {
		return nil, err
	}

	jar, _ := cookiejar.New(nil)
	redirectHost := mustHost(c.RedirectURI)
	browser := &http.Client{
		Jar:     jar,
		Timeout: c.HTTP.Timeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if strings.EqualFold(req.URL.Host, redirectHost) {
				return http.ErrUseLastResponse
			}
			if len(via) >= 15 {
				return errors.New("too many redirects")
			}
			return nil
		},
	}

	authURL := c.buildAuthorizeURL(pkce.Challenge, state, nonce)

	body, finalURL, err := loadHTML(ctx, browser, authURL)
	if err != nil {
		return nil, fmt.Errorf("load login page: %w", err)
	}
	settings, err := parseB2CSettings(body)
	if err != nil {
		return nil, fmt.Errorf("parse B2C SETTINGS: %w", err)
	}

	tenantBase := DefaultB2CHost + settings.Hosts.Tenant
	submitURL := fmt.Sprintf("%s/SelfAsserted?tx=%s&p=%s",
		tenantBase, url.QueryEscape(settings.TransID), url.QueryEscape(c.B2CPolicy))

	form := url.Values{}
	form.Set("request_type", "RESPONSE")
	form.Set("signInName", email)
	form.Set("password", password)

	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, submitURL, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")
	req.Header.Set("X-CSRF-TOKEN", settings.CSRF)
	req.Header.Set("X-Requested-With", "XMLHttpRequest")
	req.Header.Set("Origin", DefaultB2CHost)
	req.Header.Set("Referer", finalURL)
	req.Header.Set("User-Agent", browserUA)

	resp, err := browser.Do(req)
	if err != nil {
		return nil, fmt.Errorf("submit credentials: %w", err)
	}
	respBody, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	var status struct {
		Status  string `json:"status"`
		Message string `json:"message"`
	}
	_ = json.Unmarshal(respBody, &status)
	if status.Status != "200" {
		msg := status.Message
		if msg == "" {
			msg = string(respBody)
		}
		if len(msg) > 400 {
			msg = msg[:400] + "...(truncated)"
		}
		return nil, fmt.Errorf("B2C rejected credentials (status=%s): %s", status.Status, msg)
	}

	confirmURL := fmt.Sprintf("%s/api/%s/confirmed?csrf_token=%s&tx=%s&p=%s",
		tenantBase, url.PathEscape(settings.API),
		url.QueryEscape(settings.CSRF),
		url.QueryEscape(settings.TransID),
		url.QueryEscape(c.B2CPolicy))

	req, _ = http.NewRequestWithContext(ctx, http.MethodGet, confirmURL, nil)
	req.Header.Set("Referer", finalURL)
	req.Header.Set("User-Agent", browserUA)

	resp, err = browser.Do(req)
	if err != nil {
		return nil, fmt.Errorf("confirm: %w", err)
	}
	resp.Body.Close()

	loc := resp.Header.Get("Location")
	if loc == "" {
		return nil, fmt.Errorf("no redirect after confirm (status=%d); B2C may require an additional step", resp.StatusCode)
	}
	u, err := url.Parse(loc)
	if err != nil {
		return nil, err
	}
	if e := u.Query().Get("error"); e != "" {
		return nil, fmt.Errorf("B2C error: %s: %s", e, u.Query().Get("error_description"))
	}
	code := u.Query().Get("code")
	if code == "" {
		return nil, fmt.Errorf("no code in redirect: %s", loc)
	}

	return c.exchangeCode(ctx, code, pkce.Verifier)
}

const browserUA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

func (c *Client) buildAuthorizeURL(challenge, state, nonce string) string {
	u, _ := url.Parse(c.AuthorizeURL)
	q := u.Query()
	q.Set("client_id", c.ClientID)
	q.Set("redirect_uri", c.RedirectURI)
	q.Set("scope", c.Scope)
	q.Set("response_type", "code")
	q.Set("response_mode", "query")
	q.Set("code_challenge_method", "S256")
	q.Set("code_challenge", challenge)
	q.Set("state", state)
	q.Set("nonce", nonce)
	q.Set("p", c.B2CPolicy)
	u.RawQuery = q.Encode()
	return u.String()
}

func (c *Client) exchangeCode(ctx context.Context, code, verifier string) (*TokenSet, error) {
	form := url.Values{}
	form.Set("grant_type", "authorization_code")
	form.Set("client_id", c.ClientID)
	form.Set("redirect_uri", c.RedirectURI)
	form.Set("code", code)
	form.Set("code_verifier", verifier)
	form.Set("scope", c.Scope)

	endpoint := c.TokenURL + "?p=" + url.QueryEscape(c.B2CPolicy)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("token exchange %d: %s", resp.StatusCode, body)
	}
	var tr tokenResponse
	if err := json.Unmarshal(body, &tr); err != nil {
		return nil, err
	}
	return tr.toTokenSet(""), nil
}

func loadHTML(ctx context.Context, hc *http.Client, urlStr string) ([]byte, string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, urlStr, nil)
	if err != nil {
		return nil, "", err
	}
	req.Header.Set("User-Agent", browserUA)
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
	resp, err := hc.Do(req)
	if err != nil {
		return nil, "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, "", err
	}
	final := urlStr
	if resp.Request != nil && resp.Request.URL != nil {
		final = resp.Request.URL.String()
	}
	return body, final, nil
}

// b2cSettings mirrors the inline `var SETTINGS = {...}` object served on the
// B2C self-asserted login page. Only the fields we use are decoded.
type b2cSettings struct {
	CSRF    string `json:"csrf"`
	TransID string `json:"transId"`
	API     string `json:"api"`
	Hosts   struct {
		Tenant string `json:"tenant"`
	} `json:"hosts"`
}

var settingsRE = regexp.MustCompile(`(?s)var SETTINGS = (\{.*?\});`)

func parseB2CSettings(body []byte) (*b2cSettings, error) {
	m := settingsRE.FindSubmatch(body)
	if len(m) < 2 {
		return nil, errors.New("SETTINGS object not found in B2C page")
	}
	var s b2cSettings
	if err := json.Unmarshal(m[1], &s); err != nil {
		return nil, err
	}
	if s.CSRF == "" || s.TransID == "" || s.Hosts.Tenant == "" || s.API == "" {
		return nil, fmt.Errorf("incomplete SETTINGS: %+v", s)
	}
	return &s, nil
}

func mustHost(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	return u.Host
}

func (c *Client) refresh(ctx context.Context, t *TokenSet) (*TokenSet, error) {
	if c.tokenEndpoint == "" {
		if err := c.discover(ctx); err != nil {
			return nil, fmt.Errorf("discovery: %w", err)
		}
	}
	form := url.Values{}
	form.Set("grant_type", "refresh_token")
	form.Set("client_id", c.ClientID)
	form.Set("refresh_token", t.RefreshToken)
	form.Set("scope", c.Scope)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.tokenEndpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("refresh failed %d: %s", resp.StatusCode, body)
	}
	var tr tokenResponse
	if err := json.Unmarshal(body, &tr); err != nil {
		return nil, err
	}
	return tr.toTokenSet(t.RefreshToken), nil
}
