# net/http middleware example

Wires go-txnproof into a net/http server: `TxnProofMiddleware` opens a txnproof
boundary per request, named after the matched `http.ServeMux` route pattern
(e.g. `POST /users`), resolved with `mux.Handler` before dispatch — an outer
middleware cannot read `http.Request.Pattern`, which the mux populates only
after routing. Route patterns (never raw URL paths, which would leak one
boundary name per path-variable value) keep boundary names a small,
code-defined set, which the allowlist, baseline, and throttling state all
key on. Every statement executed while handling the request is attributed to
that boundary, and violations are reported when the handler returns.

The example runs against `detector.NewNullDB()`, so no database is needed.
Three routes are exercised:

- `POST /users` — two auto-commit `INSERT`s: non-atomic, reported.
- `POST /orders` — the same two writes inside one transaction: clean.
- `POST /signups` — a transactional signup plus a best-effort analytics write:
  non-atomic on purpose, so the handler marks it with
  `txnproof.AllowNonAtomicHere(ctx, reason, 2)` at the analytics write itself.
  The boundary is started by the middleware, far from the code that explains
  it; marking at the write site keeps the reason next to that code and makes
  it running code instead of a comment, pinned to exactly the 2 write units
  reviewed. Clean, and it stops being clean if a third write appears.

## Run

```console
go run .
```

Expected output (the port varies):

```
-> POST http://127.0.0.1:60585/users (two auto-commit INSERTs -> violation expected)
level=ERROR msg="txnproof: non-atomic SQL execution detected" boundary="POST /users" write_units=2 writes="[[auto-commit] INSERT INTO users (name) VALUES ('alice') [auto-commit] INSERT INTO audit_log (event) VALUES ('user created')]"
   201 user created (non-atomically!)
-> POST http://127.0.0.1:60585/orders (both INSERTs in one transaction -> clean)
   201 order created (atomically)
-> POST http://127.0.0.1:60585/signups (non-atomic on purpose, allowed at the write site -> clean)
   201 signup created (non-atomically, on purpose)
```

In a real application, keep the middleware as-is and wrap your actual driver
instead of using `NewNullDB` (see the root README's Quick start).
