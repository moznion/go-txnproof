package txnproof

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
)

type requestIDCtxKey struct{}

func violateWithOpts(t *testing.T, det *Detector, ctx context.Context, name string, opts ...BoundaryOption) {
	t.Helper()
	db := det.NewNullDB()
	t.Cleanup(func() { _ = db.Close() })
	bctx, finish := det.StartBoundary(ctx, name, opts...)
	mustExec(t, db, bctx, "INSERT INTO users (id) VALUES (1)")
	mustExec(t, db, bctx, "UPDATE counters SET n = n + 1")
	finish()
}

func TestWithBoundaryAttrsAppearOnViolation(t *testing.T) {
	det, cr, db := setup(t)

	ctx, finish := det.StartBoundary(context.Background(), "CreateUser",
		WithBoundaryAttrs(Attr("user_id", 42), Attr("plan", "pro")))
	mustExec(t, db, ctx, "INSERT INTO users (id) VALUES (1)")
	mustExec(t, db, ctx, "UPDATE counters SET n = n + 1")
	finish()

	vs := cr.Violations()
	if len(vs) != 1 {
		t.Fatalf("expected 1 violation, got %d", len(vs))
	}
	want := []BoundaryAttr{{Key: "user_id", Value: 42}, {Key: "plan", Value: "pro"}}
	if got := vs[0].Attrs; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("unexpected attrs: %+v", got)
	}
}

func TestBoundaryAttrsFuncReadsContextAndMergesBeforePerBoundaryAttrs(t *testing.T) {
	cr := NewCollectingReporter()
	det := New(
		WithReporter(cr),
		WithBoundaryAttrsFunc(func(ctx context.Context) []BoundaryAttr {
			id, _ := ctx.Value(requestIDCtxKey{}).(string)
			return []BoundaryAttr{Attr("request_id", id), Attr("source", "detector")}
		}),
	)

	ctx := context.WithValue(context.Background(), requestIDCtxKey{}, "req-123")
	violateWithOpts(t, det, ctx, "CreateUser",
		WithBoundaryAttrs(Attr("user_id", 42), Attr("source", "boundary")))

	vs := cr.Violations()
	if len(vs) != 1 {
		t.Fatalf("expected 1 violation, got %d", len(vs))
	}
	got := vs[0].Attrs
	want := []BoundaryAttr{
		{Key: "request_id", Value: "req-123"},
		{Key: "source", Value: "detector"},
		{Key: "user_id", Value: 42},
		{Key: "source", Value: "boundary"}, // duplicate keys are kept in order
	}
	if len(got) != len(want) {
		t.Fatalf("expected %d attrs, got %+v", len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("attr[%d]: expected %+v, got %+v", i, want[i], got[i])
		}
	}
	for _, s := range vs[0].Statements {
		if s.Attrs != nil {
			t.Errorf("boundary statement records must not carry attrs, got %+v", s.Attrs)
		}
	}
}

func TestBoundaryAttrsFuncEvaluatedOncePerBoundary(t *testing.T) {
	calls := 0
	cr := NewCollectingReporter()
	det := New(
		WithReporter(cr),
		WithBoundaryAttrsFunc(func(context.Context) []BoundaryAttr {
			calls++
			return []BoundaryAttr{Attr("n", calls)}
		}),
	)
	db := det.NewNullDB()
	t.Cleanup(func() { _ = db.Close() })

	ctx, finish := det.StartBoundary(context.Background(), "CreateUser")
	for i := 0; i < 5; i++ {
		mustExec(t, db, ctx, "INSERT INTO users (id) VALUES (1)")
	}
	finish()

	if calls != 1 {
		t.Fatalf("attrs func must run once per boundary, ran %d times", calls)
	}
	if vs := cr.Violations(); len(vs) != 1 || len(vs[0].Attrs) != 1 || vs[0].Attrs[0] != Attr("n", 1) {
		t.Fatalf("unexpected violations: %+v", vs)
	}
}

func TestViolationWithoutAttrsHasNone(t *testing.T) {
	det, cr, db := setup(t)

	ctx, finish := det.StartBoundary(context.Background(), "CreateUser")
	mustExec(t, db, ctx, "INSERT INTO users (id) VALUES (1)")
	mustExec(t, db, ctx, "UPDATE counters SET n = n + 1")
	finish()

	vs := cr.Violations()
	if len(vs) != 1 {
		t.Fatalf("expected 1 violation, got %d", len(vs))
	}
	if vs[0].Attrs != nil {
		t.Fatalf("expected no attrs, got %+v", vs[0].Attrs)
	}
}

func TestUnboundedWriteCarriesAttrsFromRecordTimeContext(t *testing.T) {
	calls := 0
	cr := NewCollectingReporter()
	det := New(
		WithReporter(cr),
		WithUnboundedWriteDetection(),
		WithBoundaryAttrsFunc(func(ctx context.Context) []BoundaryAttr {
			calls++
			id, _ := ctx.Value(requestIDCtxKey{}).(string)
			return []BoundaryAttr{Attr("request_id", id)}
		}),
	)
	db := det.NewNullDB()
	t.Cleanup(func() { _ = db.Close() })

	ctx := context.WithValue(context.Background(), requestIDCtxKey{}, "req-detached")
	mustExec(t, db, ctx, "INSERT INTO users (id) VALUES (1)")
	mustQuery(t, db, ctx, "SELECT 1") // reads must not trigger evaluation

	uw := cr.UnboundedWrites()
	if len(uw) != 1 {
		t.Fatalf("expected 1 unbounded write, got %d", len(uw))
	}
	if len(uw[0].Attrs) != 1 || uw[0].Attrs[0] != Attr("request_id", "req-detached") {
		t.Fatalf("unexpected attrs on unbounded write: %+v", uw[0].Attrs)
	}
	if calls != 1 {
		t.Fatalf("attrs func must run once per unbounded write, ran %d times", calls)
	}
}

func TestSlogReporterIncludesAttrs(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	det := New(
		WithReporter(NewSlogReporter(logger)),
		WithUnboundedWriteDetection(),
		WithBoundaryAttrsFunc(func(context.Context) []BoundaryAttr {
			return []BoundaryAttr{Attr("trace_id", "trace-1")}
		}),
	)

	violateWithOpts(t, det, context.Background(), "CreateUser",
		WithBoundaryAttrs(Attr("user_id", 42)))
	if out := buf.String(); !strings.Contains(out, "trace_id=trace-1") || !strings.Contains(out, "user_id=42") {
		t.Errorf("violation log should carry attrs: %s", out)
	}

	buf.Reset()
	db := det.NewNullDB()
	t.Cleanup(func() { _ = db.Close() })
	mustExec(t, db, context.Background(), "INSERT INTO users (id) VALUES (1)")
	if out := buf.String(); !strings.Contains(out, "trace_id=trace-1") {
		t.Errorf("unbounded-write log should carry attrs: %s", out)
	}
}

func TestSlogAttrsConversion(t *testing.T) {
	got := SlogAttrs([]BoundaryAttr{Attr("k1", "v1"), Attr("k2", 2)})
	if len(got) != 2 {
		t.Fatalf("expected 2 slog attrs, got %d", len(got))
	}
	if !got[0].Equal(slog.Any("k1", "v1")) || !got[1].Equal(slog.Any("k2", 2)) {
		t.Fatalf("unexpected slog attrs: %+v", got)
	}
	if empty := SlogAttrs(nil); len(empty) != 0 {
		t.Fatalf("expected no slog attrs for nil input, got %+v", empty)
	}
}
