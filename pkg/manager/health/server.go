package health

import (
	"fmt"
	"net/http"
	"strings"
)

// Checker represents a component that can report its health.
type Checker interface {
	Health() error
}

// NamedChecker associates a name with a Checker for detailed error messages.
type NamedChecker struct {
	Name    string
	Checker Checker
}

// NewHandler returns an http.Handler that serves /healthz and /readyz endpoints.
// If any checker returns an error, HTTP 503 is returned with error descriptions.
func NewHandler(checkers ...NamedChecker) http.Handler {
	mux := http.NewServeMux()

	checkFunc := func(w http.ResponseWriter, r *http.Request) {
		var failures []string
		for _, c := range checkers {
			if c.Checker != nil {
				if err := c.Checker.Health(); err != nil {
					name := c.Name
					if name == "" {
						name = "component"
					}
					failures = append(failures, fmt.Sprintf("%s: %v", name, err))
				}
			}
		}

		if len(failures) > 0 {
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprintln(w, strings.Join(failures, "\n"))
			return
		}

		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "ok")
	}

	mux.HandleFunc("/healthz", checkFunc)
	mux.HandleFunc("/readyz", checkFunc)

	return mux
}
