package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

type stubWorkerAuth struct {
	lastWorkerID string
	lastToken    string
	err          error
}

func (s *stubWorkerAuth) AuthenticateWorker(_ context.Context, workerID, token string) error {
	s.lastWorkerID = workerID
	s.lastToken = token
	return s.err
}

func TestBearerTokenAuthMiddlewareRejectsMissingToken(t *testing.T) {
	handler := chain(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}), bearerTokenAuthMiddleware("secret"))

	req := httptest.NewRequest(http.MethodGet, "/protected", nil)
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, req)

	if resp.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", resp.Code)
	}
}

func TestWorkerAuthMiddlewareUsesRouteWorkerID(t *testing.T) {
	auth := &stubWorkerAuth{}
	handler := chain(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}), workerAuthMiddleware(auth))

	req := httptest.NewRequest(http.MethodPost, "/workers/w1/heartbeat", nil)
	req.SetPathValue("workerID", "w1")
	req.Header.Set("Authorization", "Bearer token-123")
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, req)

	if resp.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", resp.Code)
	}
	if auth.lastWorkerID != "w1" || auth.lastToken != "token-123" {
		t.Fatalf("unexpected auth call: worker=%q token=%q", auth.lastWorkerID, auth.lastToken)
	}
}

func TestSubmissionLimiterBlocksAfterLimit(t *testing.T) {
	limiter := newSubmissionLimiter(2, time.Minute)
	if !limiter.allow("127.0.0.1", time.Now().UTC()) {
		t.Fatal("first request should be allowed")
	}
	if !limiter.allow("127.0.0.1", time.Now().UTC()) {
		t.Fatal("second request should be allowed")
	}
	if limiter.allow("127.0.0.1", time.Now().UTC()) {
		t.Fatal("third request should be rate limited")
	}
}

func TestRequestContextMiddlewareSetsHeader(t *testing.T) {
	handler := chain(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requestIDFromContext(r.Context()) == "" {
			t.Fatal("request id missing from context")
		}
		w.WriteHeader(http.StatusNoContent)
	}), requestContextMiddleware())

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, req)

	if resp.Header().Get("X-Request-Id") == "" {
		t.Fatal("expected X-Request-Id header")
	}
}
