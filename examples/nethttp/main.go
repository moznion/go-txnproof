// Command nethttp shows how to wire go-txnproof into a net/http server: a
// middleware opens a txnproof boundary per request, named after the HTTP
// method and the http.ServeMux route pattern (e.g. "POST /users"),
// so every statement executed while handling the request is attributed to
// that boundary.
//
// The example runs against detector.NewNullDB(), so it needs zero
// infrastructure: every statement succeeds and only the
// statement/transaction timeline is observed. In a real application you
// would wrap your actual driver instead (see the root README) — the
// middleware is identical.
package main

import (
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"

	"github.com/moznion/go-txnproof"
)

// TxnProofMiddleware wraps mux so that every request runs inside a txnproof
// boundary named after the matched route pattern (e.g. "POST /users").
//
// The pattern is resolved with mux.Handler before dispatch: http.ServeMux
// populates r.Pattern only after routing, so an outer middleware reading
// r.Pattern would always see it empty and silently fall back. Boundary names
// key the allowlist, baseline, and throttling state, so they must come from
// the small, code-defined set of route patterns — never the raw URL path,
// which would leak one boundary name per path-variable value
// ("POST /users/123", "POST /users/124", ...). Unmatched requests (404s)
// fall back to the method and raw path.
func TxnProofMiddleware(detector *txnproof.Detector, mux *http.ServeMux) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, pattern := mux.Handler(r)
		if pattern == "" {
			pattern = r.Method + " " + r.URL.Path
		}
		ctx, b := detector.StartBoundary(r.Context(), pattern)
		defer b.Finish()
		mux.ServeHTTP(w, r.WithContext(ctx))
	})
}

// createUserNonAtomic performs two auto-commit writes: structurally
// non-atomic. A crash between the two INSERTs leaves a user without its
// audit record — txnproof reports this boundary as a violation.
func createUserNonAtomic(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		if _, err := db.ExecContext(ctx, "INSERT INTO users (name) VALUES ('alice')"); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if _, err := db.ExecContext(ctx, "INSERT INTO audit_log (event) VALUES ('user created')"); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusCreated)
		_, _ = fmt.Fprintln(w, "user created (non-atomically!)")
	}
}

// createOrderAtomic performs the same two writes inside a single
// transaction: one atomic unit, so txnproof stays silent.
func createOrderAtomic(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer func() { _ = tx.Rollback() }() // no-op after Commit
		if _, err := tx.ExecContext(ctx, "INSERT INTO orders (item) VALUES ('book')"); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO audit_log (event) VALUES ('order created')"); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if err := tx.Commit(); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusCreated)
		_, _ = fmt.Fprintln(w, "order created (atomically)")
	}
}

func main() {
	// Report violations through slog. Timestamps are stripped only to keep
	// this example's output stable; in production use your regular logger.
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if len(groups) == 0 && a.Key == slog.TimeKey {
				return slog.Attr{}
			}
			return a
		},
	}))
	detector := txnproof.New(txnproof.WithReporter(txnproof.NewSlogReporter(logger)))
	db := detector.NewNullDB()
	defer func() { _ = db.Close() }()

	mux := http.NewServeMux()
	mux.HandleFunc("POST /users", createUserNonAtomic(db))
	mux.HandleFunc("POST /orders", createOrderAtomic(db))
	handler := TxnProofMiddleware(detector, mux)

	// A real application would run http.ListenAndServe(addr, handler);
	// this demo serves two requests over a loopback test server instead.
	server := httptest.NewServer(handler)
	defer server.Close()

	post(server.URL+"/users", "two auto-commit INSERTs -> violation expected")
	post(server.URL+"/orders", "both INSERTs in one transaction -> clean")
}

func post(url, comment string) {
	fmt.Printf("-> POST %s (%s)\n", url, comment)
	resp, err := http.Post(url, "text/plain", nil)
	if err != nil {
		fmt.Fprintln(os.Stderr, "request failed:", err)
		os.Exit(1)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	fmt.Printf("   %d %s", resp.StatusCode, body)
}
