package hilo

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// multipartStreamServer returns an HTTP server that streams the given
// payloads as a Apollo GraphQL multipart subscription response.
func multipartStreamServer(t *testing.T, payloads []string, holdAfter bool) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", `multipart/mixed; boundary=graphql; subscriptionSpec=1.0`)
		w.WriteHeader(http.StatusOK)
		flusher, ok := w.(http.Flusher)
		if !ok {
			t.Fatal("ResponseWriter does not support Flush")
		}
		for _, p := range payloads {
			fmt.Fprintf(w, "--graphql\r\nContent-Type: application/json\r\n\r\n%s\r\n", p)
			flusher.Flush()
			time.Sleep(5 * time.Millisecond)
		}
		fmt.Fprint(w, "--graphql--\r\n")
		flusher.Flush()
		if holdAfter {
			<-r.Context().Done()
		}
	}))
}

func TestOpenGraphQLSubscriptionStreamsParts(t *testing.T) {
	srv := multipartStreamServer(t, []string{
		`{"data":{"onAnyDeviceUpdated":{"operationId":"op-1","device":{"id":42}}}}`,
		`{"data":{"onAnyDeviceUpdated":{"operationId":"op-2","device":{"id":42}}}}`,
	}, false)
	defer srv.Close()

	c := NewClient()
	c.PlatformBase = srv.URL
	c.mu.Lock()
	c.token = &TokenSet{AccessToken: "test-token", ExpiresAt: time.Now().Add(time.Hour)}
	c.mu.Unlock()

	var mu sync.Mutex
	got := []string{}
	handler := func(payload []byte) {
		mu.Lock()
		got = append(got, string(payload))
		mu.Unlock()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	sub, err := c.openGraphQLSubscription(ctx, `subscription { test }`, nil, handler)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.cancel()

	// Wait until both parts have been delivered.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(got)
		mu.Unlock()
		if n >= 2 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(got) != 2 {
		t.Fatalf("got %d parts, want 2: %v", len(got), got)
	}
	if !strings.Contains(got[0], `"op-1"`) {
		t.Errorf("part 0 missing op-1: %s", got[0])
	}
	if !strings.Contains(got[1], `"op-2"`) {
		t.Errorf("part 1 missing op-2: %s", got[1])
	}
}

func TestOpenGraphQLSubscriptionRejectsNonMultipart(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"data": null}`)
	}))
	defer srv.Close()

	c := NewClient()
	c.PlatformBase = srv.URL
	c.mu.Lock()
	c.token = &TokenSet{AccessToken: "test-token", ExpiresAt: time.Now().Add(time.Hour)}
	c.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err := c.openGraphQLSubscription(ctx, `subscription { test }`, nil, func([]byte) {})
	if err == nil {
		t.Fatal("expected error on non-multipart response, got nil")
	}
	if !strings.Contains(err.Error(), "multipart") {
		t.Errorf("err=%v, want multipart mention", err)
	}
}
