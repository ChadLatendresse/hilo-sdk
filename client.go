package hilo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"time"
)

// Public Hilo client/B2C constants extracted from the Android bundle. These are
// "public client" identifiers shipped to every device that installs the app
// and have no security significance on their own.
const (
	DefaultClientID     = "fd8e2de8-8ee3-4871-9812-397023e7638b"
	DefaultDiscoveryURL = "https://connexion.hiloenergie.com/HiloDirectoryB2C.onmicrosoft.com/v2.0/.well-known/openid-configuration?p=B2C_1A_SIGN_IN"
	DefaultB2CPolicy    = "B2C_1A_SIGN_IN"
	DefaultScope        = "openid offline_access https://HiloDirectoryB2C.onmicrosoft.com/hiloapis/user_impersonation"
	DefaultAPIBase      = "https://api.hiloenergie.com"
	DefaultPlatformBase = "https://platform.hiloenergie.com"
	DefaultSubKey       = "20eeaedcb86945afa3fe792cea89b8bf"

	DefaultB2CHost      = "https://connexion.hiloenergie.com"
	DefaultTokenURL     = DefaultB2CHost + "/HiloDirectoryB2C.onmicrosoft.com/oauth2/v2.0/token"
	DefaultAuthorizeURL = DefaultB2CHost + "/HiloDirectoryB2C.onmicrosoft.com/oauth2/v2.0/authorize"

	// DefaultRedirectURI is the only B2C-registered web redirect URI we know
	// of. We never actually navigate to it — the form-scrape login intercepts
	// the 302 from B2C before DNS resolution would matter.
	DefaultRedirectURI = "https://salc.hiloenergie.com/login-callback"
)

// Client is the SDK entry point. After NewClient, configuration fields
// (ClientID, *Base, etc.) and Email/Password should be considered
// immutable for the lifetime of the value; callers must not mutate them
// once any method has been invoked. HTTP and Store may be replaced
// before first use.
//
// All exported methods are safe to call concurrently from multiple
// goroutines. Internal state (token cache, hub registry, in-flight
// operation correlation, GraphQL subscription registry, location
// HiloID cache) is guarded by per-domain mutexes; callers do not need
// to serialize requests.
//
// Logger, when set, is invoked from background goroutines (hub
// dispatch, subscription readers); the supplied function must be
// goroutine-safe.
type Client struct {
	ClientID        string
	Scope           string
	B2CPolicy       string
	APIBase         string
	PlatformBase    string
	SubscriptionKey string
	DiscoveryURL    string
	AuthorizeURL    string
	TokenURL        string
	RedirectURI     string

	// Email/Password are read from env on construction; if both are set,
	// AccessToken() will fall back to ROPC when the token store is empty.
	Email    string
	Password string

	HTTP  *http.Client
	Store TokenStore

	mu                sync.Mutex
	token             *TokenSet
	authorizeEndpoint string
	tokenEndpoint     string
	endSessionURL     string
	issuer            string

	hubs      map[hubKind]*hubConn
	hubsMu    sync.Mutex
	dialHubFn func(ctx context.Context, k hubKind) (*hubConn, error)

	// Logger is optional. When set, transport drop and reconnect events are reported here.
	Logger func(format string, args ...any)

	// Device-control operation correlation.
	opMu      sync.Mutex
	opPending map[opKey]*opPendingEntry
	opSubs    map[int]*opSub

	// Recent-echoes ring buffer (64 entries, protected by opMu).
	opEchoBuf [64]opEcho
	opEchoIdx int

	// Per-location GraphQL subscription for explicit status.
	gqlMu          sync.Mutex
	gqlSubs        map[int]*gqlSub
	locHiloIDCache map[int]HiloID // populated lazily by locationHiloID; guards under gqlMu
}

// NewClient builds a Client populated from defaults and HILO_* environment
// variables. Pair with LoadDotEnv if you want a .env file to seed env first.
func NewClient() *Client {
	c := &Client{
		ClientID:        envOr("HILO_CLIENT_ID", DefaultClientID),
		Scope:           envOr("HILO_SCOPE", DefaultScope),
		B2CPolicy:       envOr("HILO_B2C_POLICY", DefaultB2CPolicy),
		APIBase:         envOr("HILO_API_BASE", DefaultAPIBase),
		PlatformBase:    envOr("HILO_PLATFORM_BASE", DefaultPlatformBase),
		SubscriptionKey: envOr("HILO_SUBSCRIPTION_KEY", DefaultSubKey),
		DiscoveryURL:    envOr("HILO_DISCOVERY_URL", DefaultDiscoveryURL),
		AuthorizeURL:    envOr("HILO_AUTHORIZE_URL", DefaultAuthorizeURL),
		TokenURL:        envOr("HILO_TOKEN_URL", DefaultTokenURL),
		RedirectURI:     envOr("HILO_REDIRECT_URI", DefaultRedirectURI),
		Email:           os.Getenv("HILO_EMAIL"),
		Password:        os.Getenv("HILO_PASSWORD"),
		HTTP:            &http.Client{Timeout: 30 * time.Second},
		Store:           &FileStore{Path: envOr("HILO_TOKEN_STORE", DefaultStorePath())},
		hubs:            map[hubKind]*hubConn{},
	}
	c.dialHubFn = c.realDialHub
	c.opPending = map[opKey]*opPendingEntry{}
	c.opSubs = map[int]*opSub{}
	c.gqlSubs = map[int]*gqlSub{}
	c.locHiloIDCache = map[int]HiloID{}
	return c
}

// realDialHub is the production hub dialer; the real implementation is
// injected from signalr.go. This thin wrapper exists so test code can
// replace dialHubFn with a fake without touching client.go.
func (c *Client) realDialHub(ctx context.Context, k hubKind) (*hubConn, error) {
	return c.dialHub(ctx, k)
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

type discoveryDoc struct {
	Issuer                string `json:"issuer"`
	AuthorizationEndpoint string `json:"authorization_endpoint"`
	TokenEndpoint         string `json:"token_endpoint"`
	EndSessionEndpoint    string `json:"end_session_endpoint"`
}

func (c *Client) discover(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.DiscoveryURL, nil)
	if err != nil {
		return err
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("discovery %d: %s", resp.StatusCode, b)
	}
	var d discoveryDoc
	if err := json.NewDecoder(resp.Body).Decode(&d); err != nil {
		return err
	}
	c.authorizeEndpoint = d.AuthorizationEndpoint
	c.tokenEndpoint = d.TokenEndpoint
	c.endSessionURL = d.EndSessionEndpoint
	c.issuer = d.Issuer
	return nil
}

// AccessToken returns a valid access token, refreshing or re-authenticating
// (via ROPC) as needed. Order of operations:
//  1. Use the in-memory token if it's still valid.
//  2. Load from FileStore; if present and valid, use it; if expired but with a
//     refresh_token, exchange the refresh_token for a new pair.
//  3. If FileStore is empty *or* refresh fails, fall back to ROPC password
//     grant when HILO_EMAIL/HILO_PASSWORD are set. Save the result.
//  4. Otherwise return a clear error.
func (c *Client) AccessToken(ctx context.Context) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.token != nil && !c.token.Expired() {
		return c.token.AccessToken, nil
	}

	if c.token == nil {
		if t, err := c.Store.Load(); err == nil {
			c.token = t
		}
	}

	if c.token != nil && !c.token.Expired() {
		return c.token.AccessToken, nil
	}

	if c.token != nil && c.token.RefreshToken != "" {
		if nt, err := c.refresh(ctx, c.token); err == nil {
			c.token = nt
			_ = c.Store.Save(nt)
			return nt.AccessToken, nil
		}
		// fall through to ROPC if refresh fails
	}

	if c.Email != "" && c.Password != "" {
		nt, err := c.loginScrape(ctx, c.Email, c.Password)
		if err != nil {
			return "", fmt.Errorf("login failed: %w", err)
		}
		c.token = nt
		_ = c.Store.Save(nt)
		return nt.AccessToken, nil
	}

	return "", errors.New("no valid token and no HILO_EMAIL/HILO_PASSWORD set")
}
