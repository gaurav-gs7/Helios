package api

import (
	"context"
	"crypto/subtle"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

type contextKey string

const requestIDKey contextKey = "request_id"

func chain(handler http.Handler, middlewares ...func(http.Handler) http.Handler) http.Handler {
	for i := len(middlewares) - 1; i >= 0; i-- {
		handler = middlewares[i](handler)
	}
	return handler
}

func requestContextMiddleware() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requestID := r.Header.Get("X-Request-Id")
			if requestID == "" {
				requestID = uuid.NewString()
			}
			w.Header().Set("X-Request-Id", requestID)
			next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), requestIDKey, requestID)))
		})
	}
}

func recoveryMiddleware(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if recovered := recover(); recovered != nil {
					logger.Error("panic recovered", "panic", recovered, "path", r.URL.Path, "request_id", requestIDFromContext(r.Context()))
					writeError(w, http.StatusInternalServerError, fmt.Errorf("internal server error"))
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}

func loggingMiddleware(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			recorder := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(recorder, r)
			logger.Info("http request",
				"method", r.Method,
				"path", r.URL.Path,
				"status", recorder.status,
				"duration_ms", time.Since(start).Milliseconds(),
				"remote_addr", r.RemoteAddr,
				"request_id", requestIDFromContext(r.Context()),
			)
		})
	}
}

func bodyLimitMiddleware(limit int64) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if limit > 0 && r.Body != nil {
				r.Body = http.MaxBytesReader(w, r.Body, limit)
			}
			next.ServeHTTP(w, r)
		})
	}
}

func bearerTokenAuthMiddleware(expected string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			token := bearerTokenFromHeader(r.Header.Get("Authorization"))
			if expected == "" || subtle.ConstantTimeCompare([]byte(token), []byte(expected)) != 1 {
				w.Header().Set("WWW-Authenticate", `Bearer realm="helios"`)
				writeError(w, http.StatusUnauthorized, fmt.Errorf("unauthorized"))
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

type workerAuthenticator interface {
	AuthenticateWorker(ctx context.Context, workerID, token string) error
}

func workerAuthMiddleware(auth workerAuthenticator) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			workerID := r.PathValue("workerID")
			if workerID == "" {
				workerID = r.Header.Get("X-Helios-Worker-ID")
			}
			token := bearerTokenFromHeader(r.Header.Get("Authorization"))
			if err := auth.AuthenticateWorker(r.Context(), workerID, token); err != nil {
				writeError(w, http.StatusUnauthorized, err)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

type submissionLimiter struct {
	mu     sync.Mutex
	limit  int
	window time.Duration
	hits   map[string][]time.Time
}

func newSubmissionLimiter(limit int, window time.Duration) *submissionLimiter {
	return &submissionLimiter{
		limit:  limit,
		window: window,
		hits:   make(map[string][]time.Time),
	}
}

func (l *submissionLimiter) allow(key string, now time.Time) bool {
	if l == nil || l.limit <= 0 {
		return true
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	cutoff := now.Add(-l.window)
	entries := l.hits[key][:0]
	for _, seen := range l.hits[key] {
		if seen.After(cutoff) {
			entries = append(entries, seen)
		}
	}
	if len(entries) >= l.limit {
		l.hits[key] = entries
		return false
	}
	l.hits[key] = append(entries, now)
	return true
}

func submissionRateLimitMiddleware(limiter *submissionLimiter) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if limiter == nil {
				next.ServeHTTP(w, r)
				return
			}
			if !limiter.allow(clientIP(r.RemoteAddr), time.Now().UTC()) {
				writeError(w, http.StatusTooManyRequests, fmt.Errorf("submission rate limit exceeded"))
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

func requestIDFromContext(ctx context.Context) string {
	value, _ := ctx.Value(requestIDKey).(string)
	return value
}

func bearerTokenFromHeader(header string) string {
	if header == "" {
		return ""
	}
	parts := strings.SplitN(header, " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return ""
	}
	return strings.TrimSpace(parts[1])
}

func clientIP(remoteAddr string) string {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		return remoteAddr
	}
	return host
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}
