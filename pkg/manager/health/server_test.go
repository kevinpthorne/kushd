package health

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type mockChecker struct {
	err error
}

func (m *mockChecker) Health() error {
	return m.err
}

func TestHealthHandler_AllHealthy(t *testing.T) {
	c1 := &mockChecker{err: nil}
	c2 := &mockChecker{err: nil}

	handler := NewHandler(
		NamedChecker{Name: "poller", Checker: c1},
		NamedChecker{Name: "reconciler", Checker: c2},
	)

	for _, path := range []string{"/healthz", "/readyz"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("expected 200 OK for %s, got %d", path, rec.Code)
		}
		if !strings.Contains(rec.Body.String(), "ok") {
			t.Errorf("expected body to contain 'ok', got %q", rec.Body.String())
		}
	}
}

func TestHealthHandler_OneFailing(t *testing.T) {
	c1 := &mockChecker{err: errors.New("upsc connection refused")}
	c2 := &mockChecker{err: nil}

	handler := NewHandler(
		NamedChecker{Name: "hardware-poller", Checker: c1},
		NamedChecker{Name: "reconciler", Checker: c2},
	)

	for _, path := range []string{"/healthz", "/readyz"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusServiceUnavailable {
			t.Errorf("expected 503 Service Unavailable for %s, got %d", path, rec.Code)
		}
		if !strings.Contains(rec.Body.String(), "hardware-poller: upsc connection refused") {
			t.Errorf("expected failure message in body, got %q", rec.Body.String())
		}
	}
}
