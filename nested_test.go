package txnproof

import (
	"context"
	"testing"
	"time"
)

func TestNestedBoundaryDetectionOffByDefault(t *testing.T) {
	det, cr, _ := setup(t)

	ctx, finishOuter := det.StartBoundary(context.Background(), "Outer")
	_, finishInner := det.StartBoundary(ctx, "Inner")
	finishInner()
	finishOuter()

	if got := cr.NestedBoundaries(); len(got) != 0 {
		t.Fatalf("expected no nested-boundary reports without the option, got %+v", got)
	}
}

func TestNestedBoundaryReported(t *testing.T) {
	det, cr, db := setup(t, WithNestedBoundaryDetection())

	err := det.InBoundary(context.Background(), "Outer", func(ctx context.Context) error {
		mustExec(t, db, ctx, "INSERT INTO a (id) VALUES (1)")
		return det.InBoundary(ctx, "Inner", func(ctx context.Context) error {
			mustExec(t, db, ctx, "INSERT INTO b (id) VALUES (1)")
			return nil
		})
	})
	if err != nil {
		t.Fatal(err)
	}

	got := cr.NestedBoundaries()
	if len(got) != 1 || got[0].Outer != "Outer" || got[0].Inner != "Inner" {
		t.Fatalf("expected one Outer/Inner nesting report, got %+v", got)
	}
	if got[0].Time.IsZero() {
		t.Error("expected the nesting report to carry a timestamp")
	}
	// Shadow semantics are unchanged: one write per boundary, no violation.
	if vs := cr.Violations(); len(vs) != 0 {
		t.Fatalf("expected shadow attribution to stay violation-free, got %+v", vs)
	}
}

func TestNestedBoundaryNotReportedWithoutNesting(t *testing.T) {
	det, cr, _ := setup(t, WithNestedBoundaryDetection())

	_, finish := det.StartBoundary(context.Background(), "Solo")
	finish()

	if got := cr.NestedBoundaries(); len(got) != 0 {
		t.Fatalf("expected no reports for a non-nested boundary, got %+v", got)
	}
}

func TestRequireNoNestedBoundaries(t *testing.T) {
	det, cr, _ := setup(t, WithNestedBoundaryDetection())

	ft := &fakeT{}
	cr.RequireNoNestedBoundaries(ft)
	if len(ft.errors) != 0 {
		t.Fatalf("expected no test errors before nesting, got %d", len(ft.errors))
	}

	ctx, finishOuter := det.StartBoundary(context.Background(), "Outer")
	_, finishInner := det.StartBoundary(ctx, "Inner")
	finishInner()
	finishOuter()

	cr.RequireNoNestedBoundaries(ft)
	if len(ft.errors) != 1 {
		t.Fatalf("expected 1 test error for the nesting, got %d", len(ft.errors))
	}
}

func TestThrottlingReporterThrottlesNestedBoundaries(t *testing.T) {
	cr := NewCollectingReporter()
	tr := NewThrottlingReporter(cr, time.Minute)
	det := New(WithReporter(tr), WithNestedBoundaryDetection())

	nest := func() {
		ctx, finishOuter := det.StartBoundary(context.Background(), "Outer")
		_, finishInner := det.StartBoundary(ctx, "Inner")
		finishInner()
		finishOuter()
	}
	nest()
	nest()

	if got := cr.NestedBoundaries(); len(got) != 1 {
		t.Fatalf("expected the second identical nesting to be suppressed, got %d reports", len(got))
	}
	if n := tr.SuppressedNestedBoundaries()["Outer\x00Inner"]; n != 1 {
		t.Fatalf("expected 1 suppressed nesting for the pair, got %d", n)
	}
}

func TestThrottlingReporterNestedNoopOnPlainNext(t *testing.T) {
	plain := ReporterFunc(func(context.Context, Violation) {})
	tr := NewThrottlingReporter(plain, time.Minute)
	det := New(WithReporter(tr), WithNestedBoundaryDetection())

	// Must not panic and must not track state for a next that cannot receive it.
	ctx, finishOuter := det.StartBoundary(context.Background(), "Outer")
	_, finishInner := det.StartBoundary(ctx, "Inner")
	finishInner()
	finishOuter()

	if n := len(tr.SuppressedNestedBoundaries()); n != 0 {
		t.Fatalf("expected no tracked state for a plain next reporter, got %d keys", n)
	}
}

func TestBaselineReporterForwardsNestedBoundaries(t *testing.T) {
	cr := NewCollectingReporter()
	br := NewBaselineReporter(NewBaseline(), cr)
	det := New(WithReporter(br), WithNestedBoundaryDetection())

	ctx, finishOuter := det.StartBoundary(context.Background(), "Outer")
	_, finishInner := det.StartBoundary(ctx, "Inner")
	finishInner()
	finishOuter()

	if got := cr.NestedBoundaries(); len(got) != 1 {
		t.Fatalf("expected the nesting to pass through BaselineReporter, got %d reports", len(got))
	}
}
