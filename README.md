# go-txnproof

[![.github/workflows/check.yml](https://github.com/moznion/go-txnproof/actions/workflows/check.yml/badge.svg)](https://github.com/moznion/go-txnproof/actions/workflows/check.yml)

Detects **non-atomic SQL execution** in Go applications: multiple write statements that run inside one logical boundary (a use case, a request, a job) without being wrapped in a single database transaction.

A crash or error between such writes leaves partial state behind. `go-txnproof` finds those spots — in unit tests, in tests against a real database, and continuously in production — with a single core mechanism.

## How it works

`go-txnproof` is a [`database/sql` driver middleware](https://pkg.go.dev/database/sql/driver). It observes every statement, tracks driver-level `Begin`/`Commit`/`Rollback` to know whether each statement ran inside a transaction, and attributes statements to a logical **boundary** carried on `context.Context`.

At the end of a boundary it counts **atomic units** that contained writes:

- every transaction that contained at least one write = 1 unit
- every auto-commit write = 1 unit each

If a boundary's writes span **2 or more units**, the boundary is not atomic and a `Violation` is reported to your configured `Reporter`s.

Reads never count. Statements executed outside any boundary are ignored by default (see [unbounded writes](#unbounded-writes)).

## Install

```console
go get github.com/moznion/go-txnproof
```

Zero dependencies outside the standard library.

## Quick start

### 1. Wrap your driver

```go
import (
	"database/sql"

	"github.com/jackc/pgx/v4/stdlib"
	"github.com/moznion/go-txnproof"
)

detector := txnproof.New(
	txnproof.WithReporter(txnproof.NewSlogReporter(nil)), // or your own Reporter
)

sql.Register("pgx-txnproof", detector.Wrap(stdlib.GetDefaultDriver()))
db, err := sql.Open("pgx-txnproof", dsn)
```

Any `database/sql` driver works (`pgx`, `lib/pq`, `go-sql-driver/mysql`, `mattn/go-sqlite3`, ...). If you use `sql.OpenDB`, wrap the connector instead with `detector.WrapConnector`.

### 2. Mark boundaries

A boundary is the unit that *should* be atomic — typically a use case invocation. Put it in middleware so every code path is covered:

```go
ctx, b := detector.StartBoundary(ctx, "CreateUser")
defer b.Finish()

// ... run the use case with ctx ...
```

or use the closure form:

```go
err := detector.InBoundary(ctx, "CreateUser", func(ctx context.Context) error {
	return createUserUseCase.Do(ctx, input)
})
```

Every statement executed with that context (through the wrapped driver) is attributed to the boundary. When `b.Finish()` runs, violations are reported.

### 3. Testing without a database

`NewNullDB` returns a `*sql.DB` backed by an in-memory no-op driver: every statement succeeds and returns no rows, and only the statement/transaction timeline is observed. Unlike sqlmock, **no expectations need to be declared** — inject it and assert atomicity:

```go
func TestCreateUserIsAtomic(t *testing.T) {
	reporter := txnproof.NewCollectingReporter()
	detector := txnproof.New(txnproof.WithReporter(reporter))
	db := detector.NewNullDB()

	uc := NewCreateUserUseCase(db)
	_ = detector.InBoundary(context.Background(), "CreateUser", func(ctx context.Context) error {
		return uc.Do(ctx, input)
	})

	reporter.RequireNoViolations(t)
}
```

For tests against a real database, keep your real driver and just wrap it (step 1) — the same assertions work, and the database actually executes the statements.

### 4. Production monitoring

The same wiring, with a monitoring reporter instead of a test reporter:

```go
detector := txnproof.New(
	txnproof.WithReporter(txnproof.NewSlogReporter(logger)),
	// or ReporterFunc to emit metrics / notify an error tracker:
	txnproof.WithReporter(txnproof.ReporterFunc(func(ctx context.Context, v txnproof.Violation) {
		metrics.Count("txnproof.violation", 1, "boundary:"+v.Boundary)
	})),
)
```

The overhead is per-statement bookkeeping — a classification and a write-unit tally — with no extra I/O, and it is **allocation-free per statement**. See [Performance](#performance).

#### Throttling reports on hot paths

A violating boundary on a hot path fires the reporter on every request. Wrap your monitoring reporter in a `ThrottlingReporter` to report each boundary at most once per interval:

```go
throttled := txnproof.NewThrottlingReporter(txnproof.NewSlogReporter(logger), 10*time.Minute)
detector := txnproof.New(txnproof.WithReporter(throttled))
```

Per boundary name, the first violation is forwarded immediately; further violations for the same boundary within the interval are suppressed; after the interval elapses, the next one is forwarded again. The optional signals pass through with the same interval but independent windows: unbounded writes are deduplicated per statement text, stale `AllowNonAtomic` marks per boundary.  Wrapping does not swallow these signals — they are forwarded whenever the wrapped reporter implements the corresponding interface.

Suppressed reports are counted, not lost. The cumulative counts are available as snapshots to log or export periodically:

```go
go func() {
	for range time.Tick(time.Minute) {
		for boundary, n := range throttled.SuppressedViolations() {
			metrics.Gauge("txnproof.suppressed_violations", n, "boundary:"+boundary)
		}
	}
}()
```

Memory stays bounded: the boundary-keyed state grows only with the set of boundary names (code-defined and small), and the statement-keyed state is capped, beyond which new statements are reported unthrottled.

## Allowing intentional non-atomicity

Some boundaries are intentionally non-atomic (best-effort audit writes, writes spanning two databases that a single transaction cannot cover). There are two ways to suppress them explicitly, both requiring a reason.

### In-code: `AllowNonAtomic`

Mark the boundary at its call site — the reason lives next to the code, survives refactors, and shows up in code review diffs:

```go
ctx, b := detector.StartBoundary(ctx, "WriteAuditLog",
	txnproof.AllowNonAtomic("audit writes are best-effort by design (TICKET-123)"))
defer b.Finish()
```

Rot prevention works per execution: when an allowed boundary finishes with fewer than 2 write units (the allow suppressed nothing), reporters implementing `StaleAllowReporter` (both `CollectingReporter` and `SlogReporter` do) are notified. In tests, assert it:

```go
reporter.RequireNoViolations(t)
reporter.RequireNoStaleAllows(t)
```

Because a boundary's write count can vary by code path, a stale-allow report in production is a hint, not proof — an allowed boundary may violate on one request and not on the next. In deterministic tests it is exact.

### Central list: `Allowlist`

Alternatively, keep exemptions in one place — convenient for bulk initial adoption on an existing codebase:

```go
allowlist := txnproof.NewAllowlist().
	Add("WriteAuditLog", "audit writes are best-effort by design (TICKET-123)").
	Add("SyncToAnalyticsDB", "spans two databases; compensated by nightly reconcile (TICKET-456)")

detector := txnproof.New(
	txnproof.WithReporter(reporter),
	txnproof.WithAllowlist(allowlist),
)
```

To keep the list from rotting, every entry tracks whether it actually suppressed a violation. Fail CI when entries go stale — the same discipline as unused `//nolint` directives:

```go
if unused := allowlist.UnusedEntries(); len(unused) > 0 {
	t.Errorf("stale allowlist entries (remove them): %v", unused)
}
```

The two mechanisms coexist: `AllowNonAtomic` on the boundary wins first, then the `Allowlist` is consulted. A practical migration is to allowlist everything on first adoption, then move the permanent exemptions to in-code `AllowNonAtomic` marks.

## Baseline / ratchet

Adopting txnproof on an existing codebase usually surfaces violations you cannot fix on day one. A **baseline** captures them once; from then on only *new* violations fail, and existing ones are tolerated until fixed — the same ratchet idea as `golangci-lint --new-from-rev` or `rubocop --auto-gen-config`.

Generate the baseline deliberately, once (e.g. behind an env var or a small helper command):

```go
reporter := txnproof.NewCollectingReporter()
detector := txnproof.New(txnproof.WithReporter(reporter))
// ... run the full test suite / scenario ...

// Explicit, intentional write — txnproof never rewrites the file on its own.
err := txnproof.BaselineFromViolations(reporter.Violations()).Save("txnproof-baseline.json")
```

The file is deterministic, sorted, indented JSON keyed on boundary names (the stable identifier — no counts, statements, or timestamps), so diffs stay clean. Commit it.

On every subsequent run, load it and wrap your reporter — baselined boundaries are filtered out before the reporter sees them:

```go
baseline, err := txnproof.LoadBaseline("txnproof-baseline.json")
if err != nil {
	// A missing file is an error on purpose: creating the baseline is a
	// deliberate Save call, never a silent fallback.
	log.Fatal(err)
}

reporter := txnproof.NewCollectingReporter()
detector := txnproof.New(
	txnproof.WithReporter(txnproof.NewBaselineReporter(baseline, reporter)),
)

// ... run the suite ...
reporter.RequireNoViolations(t) // fails only on violations NOT in the baseline
```

The ratchet must only go down: like `Allowlist`, every baseline entry tracks whether it actually suppressed a violation. Fail CI when a boundary got fixed but its entry lingers, and remove the entry (or regenerate the file intentionally):

```go
if stale := baseline.UnusedEntries(); len(stale) > 0 {
	t.Errorf("boundaries fixed but still baselined (remove them from txnproof-baseline.json): %v", stale)
}
```

Baseline vs. `Allowlist`: an allowlist entry says "this is intentionally non-atomic, forever, for this reason"; a baseline entry says "this is a known bug we have not fixed yet". Debt goes in the baseline, design decisions go in the allowlist (or in-code `AllowNonAtomic`).

## Unbounded writes

Writes executed with a context that carries no boundary (e.g. goroutines detached via `context.Background()`) cannot be attributed to any boundary — they never count toward any boundary's write units and never produce a `Violation`. A boundary that does one write itself and detaches a goroutine for a second write therefore looks atomic to txnproof: that is the blind spot this option surfaces. Opt in:

```go
detector := txnproof.New(
	txnproof.WithReporter(reporter),
	txnproof.WithUnboundedWriteDetection(),
)
```

Reporters that implement `UnboundedWriteReporter` (both `CollectingReporter` and `SlogReporter` do) receive each unbounded write as it executes: one report per statement, immediately, with no threshold — a single write is reported. Reads are never reported. There is no judgment attached: an unbounded write may be a missing boundary, a context detached by mistake, or a deliberate background write — the report says only that a state change happened outside the detection net.

In tests, require full boundary coverage alongside atomicity:

```go
reporter.RequireNoViolations(t)
reporter.RequireNoUnboundedWrites(t)
```

In production, "zero violations but unbounded writes present" is the signal to investigate boundary coverage. `ThrottlingReporter` deduplicates unbounded writes per statement text (see [throttling](#throttling-reports-on-hot-paths)), and `WithBoundaryAttrsFunc` is evaluated against each unbounded write's own context at record time (see [tying violations back to requests](#tying-violations-back-to-requests)), so even detached writes stay traceable.

## Nested boundaries

Starting a boundary on a context that already carries one **shadows** the outer boundary: subsequent statements attribute to the inner boundary only. This is a deliberate contract, not an implementation limit — a boundary is the *smallest unit that should be atomic*, and the innermost declaration is the most specific claim. The alternative (counting every statement toward all enclosing boundaries) would flag every composite use case that calls two individually-atomic sub-use-cases, and whether such a composition must be atomic is a design decision (outbox/saga territory), not something a counter can rule on.

The trade-off: an outer boundary that writes directly *and* calls an inner boundary that also writes is non-atomic as a whole, yet each boundary sees only one write — no violation. And nesting itself is usually not intended at all: it typically means two instrumentation layers overlap (a per-request middleware and a per-use-case wrapper both starting boundaries). Opt in to make nesting observable:

```go
detector := txnproof.New(
	txnproof.WithReporter(reporter),
	txnproof.WithNestedBoundaryDetection(),
)
```

Each nesting occurrence is delivered — immediately, at `StartBoundary` time, never as a `Violation` — to reporters implementing `NestedBoundaryReporter` (both `CollectingReporter` and `SlogReporter` do), carrying the outer and inner boundary names. In tests:

```go
reporter.RequireNoNestedBoundaries(t)
```

`ThrottlingReporter` deduplicates nesting reports per outer/inner name pair, so an overlapping middleware stack on a hot path reports once per interval, not once per request.

## Tying violations back to requests

A production `Violation` is only actionable if you can find the request that produced it. Boundary **attrs** are string-keyed values attached to a boundary and carried into every `Violation` it produces (`Violation.Attrs`); `SlogReporter` emits them as log attributes, and `CollectingReporter` exposes them on the stored violations.

Set up a detector-level extractor once — typically pulling trace/request IDs out of the context — and every boundary gets them for free. It runs once per boundary start, never per statement:

```go
// TxnProofMiddleware opens a boundary per request, named after the matched
// route pattern (resolved with mux.Handler — the mux populates r.Pattern
// only after routing, so an outer middleware cannot read it). Use route
// patterns, never raw URL paths: boundary names key the allowlist, the
// baseline, and throttling state, so they must stay a small, code-defined
// set — "/users/123" would leak one boundary name per user.
func TxnProofMiddleware(detector *txnproof.Detector, mux *http.ServeMux) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, pattern := mux.Handler(r)
		if pattern == "" { // unmatched request (404)
			pattern = r.Method + " " + r.URL.Path
		}
		ctx, b := detector.StartBoundary(r.Context(), pattern)
		defer b.Finish()
		mux.ServeHTTP(w, r.WithContext(ctx))
	})
}

func main() {
	detector := txnproof.New(
		txnproof.WithReporter(txnproof.NewSlogReporter(logger)),
		txnproof.WithBoundaryAttrsFunc(func(ctx context.Context) []txnproof.BoundaryAttr {
			return []txnproof.BoundaryAttr{
				txnproof.Attr("request_id", requestid.FromContext(ctx)),
				txnproof.Attr("trace_id", trace.SpanContextFromContext(ctx).TraceID().String()),
			}
		}),
	)

	mux := http.NewServeMux()
	mux.HandleFunc("POST /users", createUserHandler)

	// The attrs func reads the context as it is when the boundary starts, so
	// TxnProofMiddleware goes INSIDE the middleware that puts the request and
	// trace IDs on the context.
	handler := requestid.Middleware(TxnProofMiddleware(detector, mux))
	log.Fatal(http.ListenAndServe(":8080", handler))
}
```

Values the caller already has at hand go on a single boundary with `WithBoundaryAttrs`:

```go
ctx, b := detector.StartBoundary(ctx, "CreateUser",
	txnproof.WithBoundaryAttrs(txnproof.Attr("user_id", userID)))
defer b.Finish()
```

Both coexist: detector-level attrs come first, then per-boundary ones. Duplicate keys are kept in order, never deduplicated. Reporters built on `log/slog` can convert with `txnproof.SlogAttrs`.

With unbounded-write detection on, the extractor also runs for each unbounded write — at record time, against the statement's own context — and the result is delivered on `StatementRecord.Attrs`, so even detached writes stay traceable.

## Cross-checking against server logs

txnproof's detection is a client-side observation, with documented blind spots (detached contexts, heuristic classification, best-effort textual `BEGIN`/`COMMIT` tracking). For tests that run against a real database, the `crosscheck` subpackage verifies atomicity from the server's own logs — the authoritative record of which transaction each statement actually ran in. `crosscheck` is database-agnostic: it groups the logged write statements by server-side transaction identity and applies the same semantics as txnproof (reads never count; a rolled-back transaction still counts as a unit). A database-specific adapter supplies the parsing; `pgcheck` is the PostgreSQL adapter, and writing one for another database means implementing a single interface (`crosscheck.Parser`) — see the `crosscheck` package documentation for the contract.

### PostgreSQL: `pgcheck`

Configure the test database to log every statement with transaction identifiers, in the plain (stderr) log format with English message tags:

```
log_line_prefix = '%m [%p] %q%x %v '
log_statement = 'all'
lc_messages = 'C'
```

`%v` is the virtual transaction ID, assigned to every transaction including the implicit one of each auto-commit statement — so two auto-commit writes always show two different values, which is exactly what the cross-check catches, and `log_statement` logs each statement right after its transaction acquired one. `%x` (the real transaction ID) is a weaker fallback for prefixes without `%v`: reads never get one, and it reads `0` until a statement forces assignment — in particular, don't rely on `log_min_duration_statement` timing, because the duration line of an auto-commit statement is emitted after its implicit transaction already ended. A different `log_line_prefix` works via `pgcheck.WithLogLinePrefix` (translated automatically) as long as it contains `%x` and/or `%v`.

Delimit the scenario with marker statements and verify the log tail written during the test:

```go
func TestCreateUserIsAtomicOnRealPostgres(t *testing.T) {
	dsn := os.Getenv("TXNPROOF_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TXNPROOF_TEST_PG_DSN not set")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	logFile := os.Getenv("TXNPROOF_TEST_PG_LOG") // the server's current stderr log file
	offset := fileSize(t, logFile)              // remember where the log ends before the scenario

	mustExec(t, db, pgcheck.BeginMarker("create-user"))
	runCreateUserUseCase(t, db) // the scenario under test
	mustExec(t, db, pgcheck.EndMarker("create-user"))

	checker, err := pgcheck.New()
	if err != nil {
		t.Fatal(err)
	}
	tail := openAt(t, logFile, offset) // read only what the scenario appended
	defer tail.Close()
	if _, err := checker.VerifyScenario(tail, "create-user"); err != nil {
		t.Fatal(err) // e.g. "crosscheck: writes span 2 server-side transactions (want 1) — ..."
	}
}
```

The markers make the check robust against unrelated log lines before and after the scenario; reading from a recorded offset keeps earlier test runs out. Statements from other connections interleaved *inside* the scenario would still be counted, so run such tests against a dedicated (or quiet) database. Both the simple and the extended query protocol (as used by `pgx`) are understood; multi-line statements are reassembled. Rolled-back transactions count as a unit, matching txnproof's client-side semantics.

### MySQL: `mycheck`

MySQL puts no transaction identifier on log lines — there is nothing like PostgreSQL's `%v` for the parser to read. `mycheck` instead parses the **general query log**, which records every statement with its connection (thread) id, and reconstructs the transaction grouping per thread with a state machine over the statement stream: `BEGIN`/`START TRANSACTION` opens a transaction, `COMMIT`/`ROLLBACK` closes it (`ROLLBACK TO SAVEPOINT` does not), MySQL's implicit-commit statements (DDL and friends) close it, a connection that disconnects mid-transaction implicitly rolls back — the writes still count as a unit — and everything outside a transaction is its own auto-commit unit.

Be aware this is a **weaker guarantee than pgcheck's**: the grouping is inferred from the server's statement stream, not read from a server-assigned transaction id. It is still server-side truth about which statements actually ran, in what order, on which connection — exactly what catches detached-context writes and client-side classification misses — but a server behavior the state machine does not model (an unrecognized implicit-commit statement, a disabled autocommit mode) can mis-group. A `SET` touching `autocommit` inside the verified scenario is therefore a hard error rather than a guess. The binary log was rejected as the source: rolled-back transactions never reach it, which contradicts txnproof's core semantics.

Configure the test server to write the general query log to a file:

```
general_log = ON
log_output = 'FILE'
general_log_file = /path/to/general.log
log_timestamps = UTC
```

The general query log records every statement of every connection — use it in test environments, not production. Usage is the same as pgcheck: delimit the scenario with `mycheck.BeginMarker` / `mycheck.EndMarker` and run `mycheck.New().VerifyScenario(tail, label)` over the log tail. Both the plain query path and server-side prepared statements (logged as `Execute` entries with parameters substituted) are understood; multi-line statements are reassembled. The format was validated against MySQL 8.0, 8.4 (LTS), and 9.7; CI runs the `e2e-mysql/` suite across all three release lines.

## End-to-end self-verification

txnproof verifies itself with both lenses at once: the `e2e/` and `e2e-mysql/` modules (separate Go modules, excluded from the library's zero-dependency surface) run scenarios through a driver wrapped by txnproof against a real PostgreSQL / MySQL and require the client-side verdict and the server-log verdict (`pgcheck` / `mycheck`) to agree — including the tricky paths (rolled-back transactions, textual `BEGIN`/`COMMIT`, savepoints, the prepared-statement path). `e2e/run.sh` and `e2e-mysql/run.sh` spin up a throwaway server (no Docker needed) and run them; CI does the same on every push, across PostgreSQL 16–18.

## Performance

txnproof sits on the statement hot path, so it is built to stay out of the way. Its steady-state cost is **zero allocations per statement** and **one allocation per boundary**.

- **Per statement — 0 allocations.** Classifying a statement and tallying its write-unit both run without touching the heap, regardless of the SQL's letter case (the classifier uppercases the leading keyword into a stack buffer rather than via `strings.ToUpper`). There is no extra I/O; the underlying driver does the same work it always did.
- **Per boundary — 1 allocation.** The `Boundary` struct is the only unavoidable allocation. It is returned as a `context.Context` and mutated from driver goroutines through `ctx.Value`, so it provably escapes to the heap — a structural floor, not an oversight. Reaching zero would require `sync.Pool` recycling, which is deliberately avoided: a context can outlive `Finish` (e.g. a goroutine that captured it and runs a query afterward), and a recycled boundary would then be mutated on behalf of a stale context — cross-boundary contamination, unacceptable for a correctness tool.
- **Opt-in / rare paths cost more, by design.** The statement-record buffer for violation reports is allocated lazily on first use and bounded by `WithMaxRecordedStatements`; a boundary that spans more than four distinct write transactions spills its write-tx set into a small map. Neither is on the common, healthy path.

Measured on an Apple M4 Pro (`go test -bench . -benchmem`):

| Path | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| Classify `INSERT` (lowercase) | 10 | 0 | 0 |
| Classify data-modifying CTE (`WITH … DELETE … INSERT`) | 41 | 0 | 0 |
| Classify `SELECT` | 10 | 0 | 0 |
| Empty boundary (start + finish) | 30 | 192 | 1 |
| Boundary, 1 tx, 2 writes (healthy path) | 43 | 192 | 1 |
| Boundary, 8 distinct write txs (overflow) | 165 | 384 | 3 |

These numbers are not just documented but **enforced**: `bench_test.go` includes `testing.AllocsPerRun` guards (run by the normal `go test`) that fail if classification stops being allocation-free or a boundary starts allocating more than once, so a regression breaks CI rather than silently costing GC work in production.

## Semantics and limitations

- **Rolled-back transactions still count as a unit.** If a boundary runs tx A (rolled back) and then an auto-commit write, the boundary is structurally non-atomic and is reported.
- **Statement classification is heuristic** (leading-keyword based, with token scanning for data-modifying CTEs). `CALL`/`DO` are conservatively treated as writes. Override with `WithClassifier` if needed.
- **Isolation problems are out of scope.** A read-modify-write race (`SELECT`, compute, `UPDATE` without a transaction) involves only one write and is not detected. txnproof detects atomicity violations, not isolation violations.
- **Detached contexts are invisible.** A write whose context does not carry the boundary is not attributed to it (use unbounded-write detection to at least see them).
- **MySQL's implicit commits are invisible client-side.** MySQL implicitly commits an open transaction when a DDL statement (`CREATE`/`ALTER`/`DROP`, ...) runs inside it. txnproof's client-side transaction tracking is database-agnostic and does not model this: a boundary running `BEGIN` → write → DDL → write → `COMMIT` looks like one atomic unit to txnproof but actually spans three server-side transactions — a MySQL-specific false negative. Real-database tests with the [`mycheck` cross-check](#mysql-mycheck) do catch it: its state machine models implicit commits.
- **Cross-database writes are reported, not solved.** If one boundary writes to two databases, that is ≥2 units by definition — which is exactly the point: a single SQL transaction cannot make it atomic, so the report tells you an outbox/saga/compensation is needed, or an allowlist entry with a reason.
- **Nested boundaries shadow.** Starting a boundary on a context that already has one attributes subsequent statements to the inner boundary only — a deliberate contract (see [nested boundaries](#nested-boundaries)); writes split across an outer and an inner boundary are therefore judged separately, never combined. Use `WithNestedBoundaryDetection` to surface nesting occurrences.

## Examples

Runnable, zero-infrastructure examples (each backed by `NewNullDB`, so `go run .` works with no database) live under [`examples/`](examples/):

- [`examples/nethttp`](examples/nethttp/) — net/http middleware that opens a boundary per request, named after the method and `http.ServeMux` route pattern (e.g. `POST /users`).
- [`examples/graphql`](examples/graphql/) — GraphQL resolver middleware (via [graphql-go](https://github.com/graphql-go/graphql)) that opens a boundary per resolver, named `Mutation.createUser`-style.

## License

[MIT](LICENSE)

## Author

moznion (<moznion@mail.moznion.net>)
