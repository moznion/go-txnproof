# Guidelines for go-txnproof

## What this is

A `database/sql` driver middleware that detects non-atomic SQL execution:
multiple write statements inside one logical boundary (use case / request /
job) that are not wrapped in a single transaction. One core mechanism serves
three modes: unit tests (`NewNullDB`), real-database tests (wrap the real
driver), and production monitoring (pluggable `Reporter`s).

## Hard constraints

- **Zero dependencies outside the Go standard library.** Do not add any.
  Dev tools (golangci-lint, goimports) live in the separate
  `internal/tools/go.mod` module, invoked via
  `go tool -modfile=internal/tools/go.mod` (see Makefile). Never add `tool`
  directives or requires to the root `go.mod`: they bump its go directive
  and leak requirements into every child module that `replace`s the root,
  breaking their builds (this happened once).
- Minimum Go version: 1.22 (`log/slog` and `atomic.Uint64` are used).
- Before committing: `gofmt -l .` must be empty, `golangci-lint run ./...`
  clean, `go test -race -count=1 ./...` green (that also replays every fuzz
  seed corpus). CI enforces all three on stable/oldstable, plus a `make fuzz`
  sweep (see Fuzzing).

## Core semantics (deliberate decisions — do not change casually)

- **Atomic unit counting**: at boundary finish, each transaction that
  contained ≥1 write counts as 1 unit; each auto-commit write counts as 1
  unit. `WriteUnits >= 2` → `Violation`. Reads never count.
- **Rolled-back transactions still count as a unit**: a rollback proves a
  partial-write path structurally exists in the boundary.
- **Isolation is out of scope**: read-modify-write races with a single write
  are deliberately not detected (documented in README). Do not try to bolt
  isolation detection onto the write-unit counter.
- **Boundary is context-propagated**; nested boundaries shadow the outer one.
  Statements with no boundary in context are ignored unless
  `WithUnboundedWriteDetection()` is on (then reported separately, never as a
  Violation). Shadowing is a deliberate contract (innermost = most specific
  atomicity claim; join semantics would flag every composite of atomic
  sub-use-cases): do not change it. `WithNestedBoundaryDetection()` makes
  nesting occurrences observable (`NestedBoundaryReporter`, again never a
  Violation) — the shadow semantics themselves stay untouched.
- **Allowlist is opt-out with rot prevention**: entries carry a reason,
  track usage, and `UnusedEntries()` is meant to fail CI when stale — same
  discipline as unused `//nolint`.
- **In-code allow (`AllowNonAtomic` BoundaryOption)** is the call-site
  alternative: it wins before the central Allowlist, and its rot prevention
  is per execution — an allowed boundary that finishes with <2 write units
  triggers `StaleAllowReporter` (opt-in by interface, like
  `UnboundedWriteReporter`). Per-execution stale reports are inherently
  noisy in production (write count varies by code path); exact in
  deterministic tests via `RequireNoStaleAllows`.
- **`AllowNonAtomicHere(ctx, reason, counts...)`** is the same mark made from
  the site of the extra write instead of at boundary start (the boundary is
  usually started in middleware, far from the code that explains it). It must
  stay expression-identical to the option — same counts, same fall-through to
  the Allowlist, same StaleAllow — differing only in *where* it is declared;
  the parity is pinned by `TestAllowNonAtomicHereDecidesLikeTheOption`. Last
  mark wins (it can narrow or widen what the option declared); no boundary in
  ctx or an already finished boundary = silent no-op, matching "statements
  with no boundary are ignored". This is why `allowed`/`allowReason`/
  `allowUnits` live under `b.mu` and are **snapshotted inside the same
  critical section as the unit count** in `finishBoundary`: setting `finished`
  is what stops them changing. Deliberately a package function, not
  `FromContext(ctx) *Boundary` + method: exposing the live `*Boundary` to
  application code would expose `Finish`, and an early Finish silently drops
  every later statement (false negative), whereas the function form also needs
  no nil check at the call site.
- **Optional exact write-unit counts** narrow either allow mechanism
  (`AllowNonAtomic(reason, 2)` and `Allowlist.Add(name, reason, 2)`; several
  counts for boundaries whose count varies by code path). Both share
  `coversWriteUnits` so they decide identically — the two mechanisms must
  stay a choice of *where the exemption lives*, never of what it can
  express, so anything added to one belongs on the other. An uncovered count
  falls through as if unmarked (the in-code mark falls through to the
  Allowlist) and is reported as a Violation carrying
  `AllowedWriteUnits` (the declined counts — in-code mark first, else the
  Allowlist entry's), which is why no extra stale signal is emitted for it:
  the Violation itself explains that a mark exists and why it did not apply.
  A Violation is never emitted for an atomic execution (units <2) just
  because the declared count differs — `Violation` must keep meaning
  "not atomic", which everything from `RequireNoViolations` to the
  pgcheck/mycheck server-log agreement depends on; that case goes out
  through StaleAllow / `UnusedEntries()` instead. The unit is `Violation.WriteUnits`
  (transactions-that-wrote + auto-commit writes), not transactions; counts
  <2 can never match and are documented as permanently stale/unused rather
  than rejected (no panic; the existing rot channels surface them).
  `StaleAllow` deliberately does NOT carry the expected counts: adding a
  slice field would make the struct non-comparable (tests, including the
  fuzz model, compare it with `==`).
- An Allowlist entry pinned to counts that stops matching is reported by
  BOTH `UnusedEntries()` and a Violation. That is accepted: `used` keeps its
  narrow meaning ("actually suppressed a violation"), and the pair is
  documented as "review the boundary", not "delete the entry". A separate
  mismatched-entries API was considered and rejected — the Violation is the
  signal, as on the in-code path.
- **Statement recording cap (`WithMaxRecordedStatements`) truncates only the
  report payload, never the unit counting.**

## Driver-wrapper correctness notes

These prevent double-counting; keep them intact when touching `driver.go`:

- If the underlying conn lacks `ExecerContext`/`Execer`, return
  `driver.ErrSkip` **without recording** — database/sql falls back to the
  prepared-statement path, which is also wrapped and will record.
- Do not record on `driver.ErrBadConn` — database/sql retries on a fresh
  conn and the retry records.
- The per-connection transaction state lives in `Session` (`session.go`),
  which `wrappedConn` embeds — one state machine serves both the driver
  middleware and native-driver integrations, so the fuzz coverage of the
  driver path exercises the exported surface too. It needs no locking
  (database/sql guarantees single goroutine per driver.Conn, and Session
  documents the same serial-use contract for native callers); the boundary
  struct is the shared/locked one.
- Textual `BEGIN`/`COMMIT`/`ROLLBACK` executed as plain statements also
  update the transaction state (best effort); `ROLLBACK TO SAVEPOINT` must
  not end the tx.
- `wrappedStmt` classifies its query once at Prepare and caches the
  `StatementKind`; executions go through `observeKind` with the cached value
  and must never reclassify (statement-caching drivers would re-pay the
  data-modifying-CTE full-text scan per execution). Lock-free by contract:
  the classifier is fixed after `New` and `driver.Stmt` is single-goroutine.
  Classifiers are therefore documented as pure functions of the query text.

## Native-driver observation (`session.go`)

`Session` is the exported observation surface for connections that never go
through database/sql (pgx native, ORM hooks). Decisions to keep intact:

- **One Session per connection, used serially** — attribution is per
  connection; mixing connections in one Session merges unrelated
  transactions into one unit. The contract mirrors driver.Conn's.
- **`Observe` records submitted statements whether or not they succeed**
  (same as the driver middleware; only something like database/sql's
  ErrBadConn retry justifies skipping, and that is the integration's call).
- **`BeginTx`/`EndTx` exist for transactions that never surface as text**:
  driver-API transactions and protocol-level implicit ones. The pgx batch is
  the canonical case — pipelined up to a single Sync, PostgreSQL runs it as
  ONE implicit transaction, so an integration must bracket a batch with
  BeginTx/EndTx **only when the connection is idle at batch start** (a batch
  inside an explicit tx belongs to that tx; bracketing would split the outer
  unit). Pinned by `TestPgxNativeBatchIsOneImplicitTransaction` /
  `...BatchInsideTransactionJoinsIt` in e2e, cross-checked against the
  server log.
- The reference pgx integration lives in `e2e/pgxtracer.go` (linked from
  README); it is deliberately e2e-resident so it stays cross-checked, not a
  published subpackage — promoting it to one is a roadmap candidate if
  demand shows up.

## Classification

`DefaultClassifier` is leading-keyword based with token scanning for
data-modifying CTEs (`WITH ... INSERT/UPDATE/DELETE`). Known accepted
imprecisions (documented in README): write keywords inside string literals
can misfire for `WITH` statements; `CALL`/`DO` are conservatively writes;
`EXPLAIN ANALYZE` is treated as a read. Escape hatch: `WithClassifier`.

## Baseline / ratchet (`baseline.go`)

Adoption-debt companion to the Allowlist: a baseline entry means "known bug,
not fixed yet"; allowlist/`AllowNonAtomic` mean "intentional, forever".

- Identity is the **boundary name only** — counts, statement text, and
  timestamps vary per run and must never enter the key or the file.
- The file is deterministic (sorted, indented, trailing newline) JSON;
  `Save` is always an explicit call, never automatic; `LoadBaseline` errors
  on a missing file so first adoption cannot happen silently.
- Integration is a wrapper reporter (`NewBaselineReporter(baseline, next)`),
  so baseline filtering happens after allow marks / allowlist. If a
  `WithBaseline` Detector option is ever added, the ordering question must
  be decided explicitly.
- `UnusedEntries()` fails CI when a fixed boundary lingers — the ratchet
  only goes down. `Baseline` is mutex-guarded (shared via the reporter path,
  unlike `wrappedConn.txID`).

## Production reporting helpers (`throttle.go`, `attrs.go`)

- **When adding a new optional reporter interface** (the pattern behind
  `UnboundedWriteReporter` / `StaleAllowReporter` / `NestedBoundaryReporter`):
  the wrapper reporters (`ThrottlingReporter`, `BaselineReporter`) forward
  only the interfaces they themselves implement, so every new interface MUST
  also be implemented/forwarded there — otherwise wrapping silently swallows
  the new signal.

- `ThrottlingReporter` is a wrapper reporter (same pattern as
  `BaselineReporter`): per-boundary first-report-then-suppress-per-interval.
  Suppressed counts are cumulative snapshot methods, not callbacks (callbacks
  lose counts for keys that never re-violate). Unbounded writes are keyed by
  normalized statement text with a 1024-key cap that **fails open** (past the
  cap: forwarded unthrottled — never lose reports, only dedup). The key
  derivation is a single pass into a fixed-size buffer whose cost is bounded
  by the key cap, not the statement length (it runs on every report,
  suppressed ones included); it must stay byte-identical to
  normalize-then-truncate — an equivalence test pins this. Injectable
  `now func() time.Time` keeps tests sleep-free.
- Boundary attrs: `[]BoundaryAttr{Key, Value any}` deliberately not
  `[]slog.Attr` (non-slog reporters need plain pairs; `SlogAttrs` bridges).
  Detector-level `WithBoundaryAttrsFunc` is evaluated ONCE per boundary start
  (and once per unbounded write, at record time, with the statement's own
  context) — never per statement; keep it that way. Merge order: detector
  attrs first, then per-boundary; duplicates kept, never deduplicated.
  `boundary.attrs` is immutable after start, so it needs no locking.

## Server-log cross-check (`crosscheck/` + `pgcheck/` + `mycheck/`)

Server-side truth vs client-side observation; still zero-dependency (no DB
driver — package tests run on canned log fixtures only).

- `crosscheck` is the DB-agnostic core: `Statement` carries an **opaque
  `TxID string`** (equal iff same server-side transaction; format is the
  adapter's business). A write with empty `TxID` is a hard
  `MissingTxIDError`, not a guess. Adding a database = implementing
  `crosscheck.Parser` (`Parse(io.Reader) ([]Statement, error)`) and wrapping
  `MissingTxIDError` with DB-specific config advice — MySQL is the intended
  next adapter.
- `pgcheck` is the PostgreSQL adapter. Identity mapping: `%v` preferred,
  `"xid <n>"` fallback. **Any `.../0` vxid rendering (`0/0` on PG 18, `N/0`
  earlier) means "no vxid" and must be rejected** — treating it as an
  identity silently merges unrelated auto-commit writes (false negative;
  regression-tested).
- Recommended setup is `log_statement = 'all'` + `lc_messages = 'C'`, NOT
  `log_min_duration_statement = 0`: duration lines of auto-commit statements
  are emitted after the implicit tx ended, so `%x`/`%v` are already cleared
  there (verified empirically on PG 18). Localized log tags don't parse.
- Extended-protocol `parse`/`bind` lines are ignored deliberately (prepared
  statement reuse can cross transactions); tab-indented continuation lines
  are reassembled. Plain stderr format only (CSV/JSON unsupported).
- Scenario correlation is marker statements (`BeginMarker`/`EndMarker`) plus
  reading the log from a recorded offset; assumes a quiet database inside
  the marker window.
- `mycheck` is the MySQL adapter: parses the general query log FILE format
  (validated empirically on MySQL 8.0, 8.4, and 9.7;
  `ts\t<id right-aligned to 5> Cmd\targ`,
  raw continuation lines, restartable banner). MySQL has no per-line tx id,
  so grouping is **reconstructed per thread** by a state machine and TxIDs
  are synthesized ("thread N tx M" / "thread N stmt M") — documented as a
  weaker guarantee than pgcheck's server-assigned %v. Binlog was rejected:
  rolled-back txs never reach it (contradicts "rollback still counts").
- mycheck state-machine invariants: BEGIN-while-open = implicit commit;
  implicit-commit leading keywords (CREATE/ALTER/DROP/RENAME/TRUNCATE/
  GRANT/REVOKE/INSTALL/UNINSTALL/ANALYZE/CHECK/OPTIMIZE/REPAIR/FLUSH/CACHE/
  LOCK/UNLOCK, minus CREATE/DROP TEMPORARY) close the tx and form their own
  unit — conservative list, not exhaustive; Quit mid-tx = implicit rollback,
  writes keep their unit; `SET ... autocommit ...` inside the verified slice
  is a hard actionable error (never guess), ignored outside the markers —
  which is why mycheck's Verify/VerifyScenario compose Parse +
  crosscheck.Scenario + VerifyStatements instead of delegating wholesale.
- Prepared statements in the general log: `Execute` entries carry the
  substituted text and count; `Prepare` entries and the textual
  "EXECUTE stmt" Query line (KindOther) are ignored — no double-counting.
- **Client-side tracking deliberately does NOT model MySQL implicit
  commits**: the driver wrapper is database-agnostic, so `BEGIN → write →
  DDL → write → COMMIT` is a MySQL-specific false negative client-side
  (documented in README limitations). Do not bolt dialect-specific implicit
  commit rules onto `wrappedConn`; mycheck's state machine is the detection
  path for this.

## Fuzzing (`fuzz_test.go`, `fuzz_session_test.go`, `*/fuzz_test.go`)

A panic in txnproof takes its host application down with it — far worse than
a missed violation — so every surface that consumes text the library does not
control is fuzzed. `make fuzz` sweeps all targets (`FUZZTIME=30s` each by
default, `make fuzz FUZZTIME=5m` for a deeper run); CI runs the same sweep at
15s per target and prints any `testdata/fuzz/` crasher it finds. **A crasher
found anywhere must be committed as a seed** — that directory is the
regression corpus, and `go test` replays it.

Targets are written to assert the documented semantics, not just "did not
panic":

- `FuzzDetectorSession` is the important one: it decodes the fuzz input into a
  driver program (boundaries, transactions, textual tx control, prepared
  statements, queries) run against `NewNullDB`, and cross-checks every report
  against an **independent model** of the counting rules. The model
  deliberately re-implements the semantics instead of reusing the detector's
  bookkeeping, so a behavior change surfaces as a mismatch — a mutation test
  (dropping `writeTxN` from the unit count) confirms it fails. Statement kinds
  the model assumes are pinned by `TestFuzzStatementKinds`.
- `FuzzDefaultClassifier`: total, pure (the prepared-statement path caches the
  kind forever), blind to leading whitespace/comments, and case-insensitive
  for ASCII input only — Unicode case mapping can fold a non-identifier rune
  into an ASCII letter (U+0131 → 'I') and legitimately change the verdict.
- `FuzzUnboundedWriteKey` generalizes the normalize-then-truncate equivalence
  test that CLAUDE.md's throttle section requires.
- `FuzzThrottlingReporterAccounting`: forwarded + suppressed == submitted, per
  signal — the fail-open path past the key cap included.
- `pgcheck.FuzzParse` pins the `.../0` vxid rejection; `mycheck.FuzzParse`
  pins "every parsed statement carries a synthesized identity" (so a
  `MissingTxIDError` can never come out of its parse path), and
  `mycheck.FuzzThreadGrouping` pins that two threads never share one.
- Known non-goals asserted as such: a baseline round-trip is only exact for
  valid UTF-8 boundary names (JSON string encoding is lossy otherwise), and
  `crosscheck` marker slicing is undefined for labels crafted so one marker
  literal contains the other's.

## Examples (`examples/`) and e2e (`e2e/`)

- Each is its own Go module with a `replace` to the root, so the root module
  stays zero-dependency and root `go test ./...` does not cover them —
  run/vet them separately when touching them. CI has dedicated jobs for
  both: examples are built, vetted, AND smoke-run (`go run .` must emit the
  violation message); e2e runs via `e2e/run.sh` on a PostgreSQL version
  matrix (16/17/18 — the parser handles version-specific `%v` renderings,
  so keep the matrix).
- `examples/nethttp` derives boundary names via `mux.Handler(r)` lookup, NOT
  `http.Request.Pattern` — the mux populates r.Pattern only after routing,
  so an outer middleware always sees it empty (verified empirically). Route
  patterns, never raw URL paths: boundary names key allowlist/baseline/
  throttling state and must stay a small code-defined set.
- `e2e/` self-verifies txnproof: scenarios run through a wrapped pgx v5
  driver against a real PostgreSQL, and the client-side verdict must agree
  with the `pgcheck` server-log verdict. Scenarios: auto-commit ×2, single
  tx, rollback+write, textual BEGIN/COMMIT, prepared-statement
  no-double-count, ROLLBACK TO SAVEPOINT keeps the tx open, query-path
  writes (`INSERT ... RETURNING` via QueryRow), interleaved reads don't
  count, a server-side failed write still counts as a unit (only ErrBadConn
  skips recording), and concurrent boundaries attribute independently
  (client-side only — marker correlation assumes a quiet database).
  `e2e/run.sh` initdb's a throwaway cluster on a unix socket (no Docker);
  tests skip without `TXNPROOF_E2E_PG_DSN`/`TXNPROOF_E2E_PG_LOG`.
- `e2e-mysql/` mirrors `e2e/` (own module, go-sql-driver/mysql, run.sh with
  a throwaway socket-only mysqld — workdir must be short, unix socket paths
  cap at ~100 bytes). run.sh is the local path; the CI job instead runs the
  official mysql Docker image (matrix: 8.0 / 8.4 / 9) with the general log
  on a bind mount — a services: container can't pass mysqld arguments, and
  the log file must be chmod'd inside the container to be runner-readable.

## Sister project

- `github.com/moznion/go-txnproof-analyzer` (separate repo, ghq-adjacent):
  go/analysis static analyzer complementing the runtime library. It vendors
  `classify.go` in `internal/classify/` because go-txnproof is not yet
  published — once this repo is pushed, swap the vendored copy for a real
  dependency (instructions in that repo).

## Roadmap candidates (not yet implemented)

- (none at the moment)
