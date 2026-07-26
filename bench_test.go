package txnproof

import (
	"context"
	"testing"
)

// The benchmarks below observe txnproof's per-statement and per-boundary
// overhead. The TestAllocs* tests turn the allocation counts they reveal into
// assertions so an accidental regression (e.g. reintroducing strings.ToUpper on
// the classify path, or an eager map/closure allocation per boundary) fails CI
// instead of quietly costing allocations in production.

const (
	benchInsertLower = "insert into users (id, name) values (1, 'a')"
	benchCTELower    = "with moved as (delete from src where id = 1 returning *) insert into dst select * from moved"
	benchSelectLower = "select id, name from users where id = 1"
)

func BenchmarkClassifyInsert(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		sink = DefaultClassifier(benchInsertLower)
	}
}

func BenchmarkClassifyCTE(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		sink = DefaultClassifier(benchCTELower)
	}
}

func BenchmarkClassifySelect(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		sink = DefaultClassifier(benchSelectLower)
	}
}

var sink StatementKind

// BenchmarkBoundaryEmpty measures a boundary that carries no statements: pure
// StartBoundary + Finish overhead (the boundary struct itself).
func BenchmarkBoundaryEmpty(b *testing.B) {
	det := New()
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, bd := det.StartBoundary(ctx, "Bench")
		bd.Finish()
	}
}

// BenchmarkBoundarySingleTx measures the healthy, non-violating path: one
// transaction with two writes, statement recording disabled so only the
// write-unit counter is exercised (the inline write-tx array, no map).
func BenchmarkBoundarySingleTx(b *testing.B) {
	det := New(WithMaxRecordedStatements(0))
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bctx, bd := det.StartBoundary(ctx, "Bench")
		det.record(bctx, "insert into t values (1)", KindWrite, 1)
		det.record(bctx, "update t set a = 1", KindWrite, 1)
		bd.Finish()
	}
}

// BenchmarkBoundaryManyTx crosses the inline write-tx capacity so the overflow
// map is allocated: the deliberately more expensive, less common path.
func BenchmarkBoundaryManyTx(b *testing.B) {
	det := New(WithMaxRecordedStatements(0))
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bctx, bd := det.StartBoundary(ctx, "Bench")
		for tx := uint64(1); tx <= 8; tx++ {
			det.record(bctx, "insert into t values (1)", KindWrite, tx)
		}
		bd.Finish()
	}
}

// BenchmarkBoundaryRecordingHealthy measures the default production path:
// statement recording on (the default cap), one transaction with a couple of
// writes interleaved with reads, finishing without a violation. The non-violating
// path must not snapshot the recorded statements.
func BenchmarkBoundaryRecordingHealthy(b *testing.B) {
	det := New()
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bctx, bd := det.StartBoundary(ctx, "Bench")
		det.record(bctx, "select * from t where id = 1", KindRead, 1)
		det.record(bctx, "insert into t values (1)", KindWrite, 1)
		det.record(bctx, "update t set a = 1 where id = 1", KindWrite, 1)
		det.record(bctx, "select count(*) from t", KindRead, 1)
		bd.Finish()
	}
}

// TestAllocsClassifyIsZero pins statement classification at zero allocations
// regardless of the query's letter case — the regression guard for the
// stack-buffer uppercasing on the hot path.
func TestAllocsClassifyIsZero(t *testing.T) {
	cases := map[string]string{
		"lowercase insert": benchInsertLower,
		"uppercase insert": "INSERT INTO users (id) VALUES (1)",
		"lowercase select": benchSelectLower,
		"lowercase CTE":    benchCTELower,
		"rollback to sp":   "rollback to savepoint sp1",
	}
	for name, q := range cases {
		t.Run(name, func(t *testing.T) {
			q := q
			if got := testing.AllocsPerRun(200, func() { sink = DefaultClassifier(q) }); got != 0 {
				t.Errorf("DefaultClassifier(%q) allocated %v times, want 0", q, got)
			}
		})
	}
}

// TestAllocsBoundaryEmpty pins an empty boundary at a single allocation: the
// Boundary struct, which must be heap-allocated because it is returned as a
// context.Context and mutated from driver goroutines. Reaching 0 would require
// pooling, which is unsafe (a context can outlive Finish).
func TestAllocsBoundaryEmpty(t *testing.T) {
	det := New()
	ctx := context.Background()
	got := testing.AllocsPerRun(200, func() {
		_, b := det.StartBoundary(ctx, "Bench")
		b.Finish()
	})
	if got > 1 {
		t.Errorf("empty boundary allocated %v times, want <= 1", got)
	}
}

// TestAllocsBoundarySingleTx pins the healthy transactional path (one tx, two
// writes, recording off) at a single allocation: only the Boundary struct, the
// write-unit counter itself staying allocation-free via the inline array.
func TestAllocsBoundarySingleTx(t *testing.T) {
	det := New(WithMaxRecordedStatements(0))
	ctx := context.Background()
	got := testing.AllocsPerRun(200, func() {
		bctx, b := det.StartBoundary(ctx, "Bench")
		det.record(bctx, "insert into t values (1)", KindWrite, 1)
		det.record(bctx, "update t set a = 1", KindWrite, 1)
		b.Finish()
	})
	if got > 1 {
		t.Errorf("single-tx boundary allocated %v times, want <= 1", got)
	}
}

// TestAllocsBoundaryRecordingHealthyNoSnapshot guards the healthy default path:
// with recording on, a boundary that finishes without a violation must allocate
// only the Boundary struct and the statement buffer (2), never the extra
// statement snapshot — that snapshot is deferred to the violation path.
func TestAllocsBoundaryRecordingHealthyNoSnapshot(t *testing.T) {
	det := New()
	ctx := context.Background()
	got := testing.AllocsPerRun(200, func() {
		bctx, b := det.StartBoundary(ctx, "Bench")
		det.record(bctx, "insert into t values (1)", KindWrite, 1)
		det.record(bctx, "select 1", KindRead, 1)
		b.Finish()
	})
	if got > 2 {
		t.Errorf("healthy recording boundary allocated %v times, want <= 2", got)
	}
}
