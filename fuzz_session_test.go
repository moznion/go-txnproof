package txnproof

// FuzzDetectorSession drives the whole middleware — driver wrapper, boundary
// bookkeeping, reporter fan-out — with randomly generated statement programs,
// and cross-checks every report against an independent model of the documented
// semantics. It is the target that matters most: this is the code that runs in
// the host application's request path, and both a panic and a wrong verdict
// originate here.
//
// The model deliberately re-implements the counting rules from the
// documentation (each transaction containing >=1 write is one unit, each
// auto-commit write is one unit, reads never count, textual BEGIN/COMMIT move
// the connection's transaction state) instead of reusing the detector's own
// bookkeeping, so a change of behavior shows up as a mismatch.

import (
	"context"
	"database/sql"
	"testing"
)

// fuzzOp is one instruction of the program a fuzz input encodes.
type fuzzOp byte

const (
	opStartBoundary fuzzOp = iota
	opStartAllowedBoundary
	opFinishBoundary
	opExecWrite
	opExecRead
	opExecCTEWrite
	opQueryWrite
	opQueryRead
	opBeginTx
	opCommitTx
	opRollbackTx
	opTextBegin
	opTextCommit
	opTextRollback
	opRollbackToSavepoint
	opPrepareExecWrite
	opPrepareQueryRead
	numFuzzOps
)

// fuzzStmt is the statement an op executes, together with the classification
// the model assumes for it. The kinds are spelled out rather than taken from
// DefaultClassifier so that the model does not inherit a classifier bug it is
// supposed to expose; TestFuzzStatementKinds pins them against the classifier.
type fuzzStmt struct {
	query string
	kind  StatementKind
}

var fuzzStmts = map[fuzzOp]fuzzStmt{
	opExecWrite:           {"INSERT INTO t VALUES (1)", KindWrite},
	opExecRead:            {"SELECT 1", KindRead},
	opExecCTEWrite:        {"WITH x AS (UPDATE t SET a = 1 RETURNING id) SELECT * FROM x", KindWrite},
	opQueryWrite:          {"INSERT INTO t VALUES (2) RETURNING id", KindWrite},
	opQueryRead:           {"SELECT * FROM t", KindRead},
	opTextBegin:           {"BEGIN", KindBegin},
	opTextCommit:          {"COMMIT", KindCommit},
	opTextRollback:        {"ROLLBACK", KindRollback},
	opRollbackToSavepoint: {"ROLLBACK TO SAVEPOINT sp", KindOther},
	opPrepareExecWrite:    {"DELETE FROM t WHERE id = 1", KindWrite},
	opPrepareQueryRead:    {"SELECT 2", KindRead},
}

var fuzzBoundaryNames = []string{"CreateUser", "SyncOrders", "Job"}

// TestFuzzStatementKinds guards the fuzz model's assumption that the sample
// statements classify as intended: a drift here would silently weaken
// FuzzDetectorSession instead of failing it.
func TestFuzzStatementKinds(t *testing.T) {
	for op, s := range fuzzStmts {
		if got := DefaultClassifier(s.query); got != s.kind {
			t.Errorf("op %d: DefaultClassifier(%q) = %v, model assumes %v", op, s.query, got, s.kind)
		}
	}
}

func FuzzDetectorSession(f *testing.F) {
	// Seeds spell out the canonical shapes: two auto-commit writes (a
	// violation), writes inside one transaction (atomic), a rolled-back
	// transaction plus a write (a violation), textual transaction control, a
	// prepared write, nested boundaries and an unbounded write.
	for _, seed := range [][]byte{
		{4, byte(opStartBoundary), byte(opExecWrite), byte(opExecWrite), byte(opFinishBoundary)},
		{4, byte(opStartBoundary), byte(opBeginTx), byte(opExecWrite), byte(opExecWrite), byte(opCommitTx), byte(opFinishBoundary)},
		{4, byte(opStartBoundary), byte(opBeginTx), byte(opExecWrite), byte(opRollbackTx), byte(opExecWrite), byte(opFinishBoundary)},
		{4, byte(opStartBoundary), byte(opTextBegin), byte(opExecWrite), byte(opRollbackToSavepoint), byte(opExecWrite), byte(opTextCommit), byte(opFinishBoundary)},
		{4, byte(opStartBoundary), byte(opPrepareExecWrite), byte(opPrepareExecWrite), byte(opFinishBoundary)},
		{4, byte(opStartAllowedBoundary), byte(opExecRead), byte(opFinishBoundary)},
		{4, byte(opStartBoundary), byte(opStartBoundary), byte(opExecWrite), byte(opFinishBoundary), byte(opExecWrite), byte(opFinishBoundary)},
		{0, byte(opExecWrite), byte(opQueryWrite), byte(opStartBoundary), byte(opExecCTEWrite), byte(opFinishBoundary)},
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, program []byte) {
		if len(program) == 0 {
			return
		}
		// The first byte configures the statement-recording cap (0 included:
		// truncation must never disturb the unit counting).
		maxRecorded := int(program[0] % 8)
		ops := program[1:]
		if len(ops) > 128 {
			ops = ops[:128]
		}

		s := newFuzzSession(t, maxRecorded)
		defer s.close()
		for _, b := range ops {
			s.step(fuzzOp(b%byte(numFuzzOps)), int(b/byte(numFuzzOps))%len(fuzzBoundaryNames))
		}
		s.finishAll()
	})
}

// modelBoundary mirrors one live *Boundary in the model.
type modelBoundary struct {
	handle     *Boundary
	ctx        context.Context
	name       string
	allowed    bool
	recorded   int                 // statements of every kind attributed to it
	writeTx    map[uint64]struct{} // transactions that contained a write
	autoCommit int                 // auto-commit writes, one unit each
}

func (m *modelBoundary) units() int { return len(m.writeTx) + m.autoCommit }

// fuzzSession runs one generated program against a real *sql.DB backed by the
// null driver, keeping the model state alongside it.
type fuzzSession struct {
	t           *testing.T
	det         *Detector
	cr          *CollectingReporter
	db          *sql.DB
	conn        *sql.Conn
	tx          *sql.Tx
	maxRecorded int

	stack []*modelBoundary
	txSeq uint64 // mirrors Detector.txSeq
	txID  uint64 // mirrors wrappedConn.txID: non-zero while inside a transaction

	seenViolations, seenStale, seenUnbounded, seenNested int
}

func newFuzzSession(t *testing.T, maxRecorded int) *fuzzSession {
	cr := NewCollectingReporter()
	det := New(
		WithReporter(cr),
		WithMaxRecordedStatements(maxRecorded),
		WithUnboundedWriteDetection(),
		WithNestedBoundaryDetection(),
	)
	db := det.NewNullDB()
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("open connection: %v", err)
	}
	return &fuzzSession{t: t, det: det, cr: cr, db: db, conn: conn, maxRecorded: maxRecorded}
}

func (s *fuzzSession) close() {
	if s.tx != nil {
		_ = s.tx.Rollback()
	}
	_ = s.conn.Close()
	_ = s.db.Close()
}

func (s *fuzzSession) top() *modelBoundary {
	if len(s.stack) == 0 {
		return nil
	}
	return s.stack[len(s.stack)-1]
}

// ctx is the context statements run with: the innermost live boundary, or a
// boundary-less background context.
func (s *fuzzSession) ctx() context.Context {
	if b := s.top(); b != nil {
		return b.ctx
	}
	return context.Background()
}

func (s *fuzzSession) step(op fuzzOp, nameIdx int) {
	switch op {
	case opStartBoundary, opStartAllowedBoundary:
		s.startBoundary(fuzzBoundaryNames[nameIdx], op == opStartAllowedBoundary)
	case opFinishBoundary:
		s.finishBoundary()
	case opBeginTx:
		s.beginTx()
	case opCommitTx:
		s.endTx(true)
	case opRollbackTx:
		s.endTx(false)
	case opExecWrite, opExecRead, opExecCTEWrite, opTextBegin, opTextCommit, opTextRollback, opRollbackToSavepoint:
		s.exec(fuzzStmts[op])
	case opQueryWrite, opQueryRead:
		s.query(fuzzStmts[op])
	case opPrepareExecWrite:
		s.prepared(fuzzStmts[op], true)
	case opPrepareQueryRead:
		s.prepared(fuzzStmts[op], false)
	}
}

func (s *fuzzSession) startBoundary(name string, allowed bool) {
	var opts []BoundaryOption
	if allowed {
		opts = append(opts, AllowNonAtomic("fuzz"))
	}
	outer := s.top()
	ctx, b := s.det.StartBoundary(s.ctx(), name, opts...)
	s.stack = append(s.stack, &modelBoundary{
		handle:  b,
		ctx:     ctx,
		name:    name,
		allowed: allowed,
		writeTx: map[uint64]struct{}{},
	})

	exp := reportExpectation{}
	if outer != nil {
		// Nesting is observable but never a violation; the shadow semantics
		// stay untouched.
		exp.nested = &NestedBoundary{Outer: outer.name, Inner: name}
	}
	s.verify(exp)
}

func (s *fuzzSession) finishBoundary() {
	b := s.top()
	if b == nil {
		return
	}
	s.stack = s.stack[:len(s.stack)-1]

	units := b.units()
	exp := reportExpectation{}
	switch {
	case units >= 2 && !b.allowed:
		recorded := min(b.recorded, s.maxRecorded)
		exp.violation = &Violation{
			Boundary:            b.name,
			WriteUnits:          units,
			Statements:          make([]StatementRecord, recorded),
			TruncatedStatements: b.recorded - recorded,
		}
	case units < 2 && b.allowed:
		// Rot prevention: an allow that suppressed nothing this execution.
		exp.stale = &StaleAllow{Boundary: b.name, Reason: "fuzz", WriteUnits: units}
	}
	b.handle.Finish()
	s.verify(exp)

	// Finish is documented as idempotent, and statements executed with a
	// finished boundary's context must not resurrect it.
	b.handle.Finish()
	s.verify(reportExpectation{})
}

func (s *fuzzSession) finishAll() {
	for len(s.stack) > 0 {
		s.finishBoundary()
	}
}

func (s *fuzzSession) beginTx() {
	if s.tx != nil {
		return
	}
	tx, err := s.conn.BeginTx(s.ctx(), nil)
	if err != nil {
		s.t.Fatalf("BeginTx: %v", err)
	}
	s.tx = tx
	// The wrapper assigns a fresh transaction id on every Begin, even if
	// textual transaction control already opened one.
	s.txSeq++
	s.txID = s.txSeq
	s.verify(reportExpectation{})
}

func (s *fuzzSession) endTx(commit bool) {
	if s.tx == nil {
		return
	}
	var err error
	if commit {
		err = s.tx.Commit()
	} else {
		err = s.tx.Rollback()
	}
	if err != nil {
		s.t.Fatalf("end transaction (commit=%v): %v", commit, err)
	}
	s.tx = nil
	s.txID = 0
	s.verify(reportExpectation{})
}

func (s *fuzzSession) exec(st fuzzStmt) {
	ctx := s.ctx()
	var err error
	if s.tx != nil {
		_, err = s.tx.ExecContext(ctx, st.query)
	} else {
		_, err = s.conn.ExecContext(ctx, st.query)
	}
	if err != nil {
		s.t.Fatalf("ExecContext(%q): %v", st.query, err)
	}
	s.record(st)
}

func (s *fuzzSession) query(st fuzzStmt) {
	ctx := s.ctx()
	var (
		rows *sql.Rows
		err  error
	)
	if s.tx != nil {
		rows, err = s.tx.QueryContext(ctx, st.query)
	} else {
		rows, err = s.conn.QueryContext(ctx, st.query)
	}
	if err != nil {
		s.t.Fatalf("QueryContext(%q): %v", st.query, err)
	}
	// Rows must be drained before the connection accepts the next statement.
	for rows.Next() {
	}
	if err := rows.Err(); err != nil {
		s.t.Fatalf("rows.Err() for %q: %v", st.query, err)
	}
	if err := rows.Close(); err != nil {
		s.t.Fatalf("rows.Close() for %q: %v", st.query, err)
	}
	s.record(st)
}

// prepared runs the statement through the prepared-statement path, which
// classifies once at Prepare and reuses the cached kind for the execution.
func (s *fuzzSession) prepared(st fuzzStmt, exec bool) {
	ctx := s.ctx()
	var (
		stmt *sql.Stmt
		err  error
	)
	if s.tx != nil {
		stmt, err = s.tx.PrepareContext(ctx, st.query)
	} else {
		stmt, err = s.conn.PrepareContext(ctx, st.query)
	}
	if err != nil {
		s.t.Fatalf("PrepareContext(%q): %v", st.query, err)
	}
	defer func() {
		if err := stmt.Close(); err != nil {
			s.t.Fatalf("stmt.Close() for %q: %v", st.query, err)
		}
	}()
	// Preparing alone must record nothing.
	s.verify(reportExpectation{})

	if exec {
		if _, err := stmt.ExecContext(ctx); err != nil {
			s.t.Fatalf("stmt.ExecContext(%q): %v", st.query, err)
		}
	} else {
		rows, err := stmt.QueryContext(ctx)
		if err != nil {
			s.t.Fatalf("stmt.QueryContext(%q): %v", st.query, err)
		}
		for rows.Next() {
		}
		if err := rows.Close(); err != nil {
			s.t.Fatalf("rows.Close() for %q: %v", st.query, err)
		}
	}
	s.record(st)
}

// record applies one executed statement to the model, mirroring
// wrappedConn.observeKind followed by Detector.record.
func (s *fuzzSession) record(st fuzzStmt) {
	switch st.kind {
	case KindBegin:
		if s.txID == 0 {
			s.txSeq++
			s.txID = s.txSeq
		}
	case KindCommit, KindRollback:
		s.txID = 0
	}

	exp := reportExpectation{}
	if b := s.top(); b != nil {
		b.recorded++
		if st.kind == KindWrite {
			if s.txID != 0 {
				b.writeTx[s.txID] = struct{}{}
			} else {
				b.autoCommit++
			}
		}
	} else if st.kind == KindWrite {
		// No boundary in context: reported separately, never as a violation.
		exp.unbounded = &StatementRecord{Query: st.query, Kind: st.kind, TxID: s.txID}
	}
	s.verify(exp)
}

// reportExpectation is what a single step is expected to produce. At most one
// report of each kind can result from one step.
type reportExpectation struct {
	violation *Violation
	stale     *StaleAllow
	unbounded *StatementRecord
	nested    *NestedBoundary
}

// verify checks the reports collected since the previous step against the
// expectation and advances the seen counters.
func (s *fuzzSession) verify(exp reportExpectation) {
	s.t.Helper()

	violations := s.cr.Violations()
	if got, want := len(violations)-s.seenViolations, boolToInt(exp.violation != nil); got != want {
		s.t.Fatalf("got %d new violation(s), want %d (%+v)", got, want, exp.violation)
	}
	if exp.violation != nil {
		got, want := violations[s.seenViolations], *exp.violation
		if got.Boundary != want.Boundary || got.WriteUnits != want.WriteUnits {
			s.t.Fatalf("violation = {%q, %d units}, want {%q, %d units}", got.Boundary, got.WriteUnits, want.Boundary, want.WriteUnits)
		}
		if len(got.Statements) != len(want.Statements) || got.TruncatedStatements != want.TruncatedStatements {
			s.t.Fatalf("violation %q recorded %d statement(s) (+%d truncated), want %d (+%d)",
				got.Boundary, len(got.Statements), got.TruncatedStatements, len(want.Statements), want.TruncatedStatements)
		}
	}
	s.seenViolations = len(violations)

	stale := s.cr.StaleAllows()
	if got, want := len(stale)-s.seenStale, boolToInt(exp.stale != nil); got != want {
		s.t.Fatalf("got %d new stale allow(s), want %d (%+v)", got, want, exp.stale)
	}
	if exp.stale != nil {
		if got := stale[s.seenStale]; got != *exp.stale {
			s.t.Fatalf("stale allow = %+v, want %+v", got, *exp.stale)
		}
	}
	s.seenStale = len(stale)

	unbounded := s.cr.UnboundedWrites()
	if got, want := len(unbounded)-s.seenUnbounded, boolToInt(exp.unbounded != nil); got != want {
		s.t.Fatalf("got %d new unbounded write(s), want %d (%+v)", got, want, exp.unbounded)
	}
	if exp.unbounded != nil {
		got := unbounded[s.seenUnbounded]
		if got.Query != exp.unbounded.Query || got.Kind != exp.unbounded.Kind || got.TxID != exp.unbounded.TxID {
			s.t.Fatalf("unbounded write = {%q, %v, tx %d}, want {%q, %v, tx %d}",
				got.Query, got.Kind, got.TxID, exp.unbounded.Query, exp.unbounded.Kind, exp.unbounded.TxID)
		}
	}
	s.seenUnbounded = len(unbounded)

	nested := s.cr.NestedBoundaries()
	if got, want := len(nested)-s.seenNested, boolToInt(exp.nested != nil); got != want {
		s.t.Fatalf("got %d new nested boundary report(s), want %d (%+v)", got, want, exp.nested)
	}
	if exp.nested != nil {
		got := nested[s.seenNested]
		if got.Outer != exp.nested.Outer || got.Inner != exp.nested.Inner {
			s.t.Fatalf("nested boundary = {%q in %q}, want {%q in %q}", got.Inner, got.Outer, exp.nested.Inner, exp.nested.Outer)
		}
	}
	s.seenNested = len(nested)
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
