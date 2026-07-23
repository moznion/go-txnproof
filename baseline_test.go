package txnproof

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func setupWithBaseline(t *testing.T, b *Baseline) (*Detector, *CollectingReporter, *sql.DB) {
	t.Helper()
	cr := NewCollectingReporter()
	det := New(WithReporter(NewBaselineReporter(b, cr)), WithUnboundedWriteDetection())
	db := det.NewNullDB()
	t.Cleanup(func() { _ = db.Close() })
	return det, cr, db
}

func violate(t *testing.T, det *Detector, db *sql.DB, boundary string) {
	t.Helper()
	ctx, finish := det.StartBoundary(context.Background(), boundary)
	mustExec(t, db, ctx, "INSERT INTO a (id) VALUES (1)")
	mustExec(t, db, ctx, "INSERT INTO b (id) VALUES (1)")
	finish.Finish()
}

func TestBaselineSuppressesKnownAndPassesNewViolations(t *testing.T) {
	b := NewBaseline().Add("LegacyImport")
	det, cr, db := setupWithBaseline(t, b)

	violate(t, det, db, "LegacyImport")
	violate(t, det, db, "CreateUser")

	vs := cr.Violations()
	if len(vs) != 1 || vs[0].Boundary != "CreateUser" {
		t.Fatalf("expected only the new violation to pass through, got %+v", vs)
	}
	if unused := b.UnusedEntries(); len(unused) != 0 {
		t.Fatalf("entry suppressed a violation; must not be unused, got %v", unused)
	}
}

func TestBaselineUnusedEntries(t *testing.T) {
	b := NewBaseline().Add("FixedBoundary").Add("StillBroken")
	det, cr, db := setupWithBaseline(t, b)

	violate(t, det, db, "StillBroken")

	// FixedBoundary runs atomically now: its entry suppressed nothing.
	ctx, finish := det.StartBoundary(context.Background(), "FixedBoundary")
	mustExec(t, db, ctx, "INSERT INTO a (id) VALUES (1)")
	finish.Finish()

	if vs := cr.Violations(); len(vs) != 0 {
		t.Fatalf("expected no violations, got %+v", vs)
	}
	unused := b.UnusedEntries()
	if len(unused) != 1 || unused[0] != "FixedBoundary" {
		t.Fatalf("expected [FixedBoundary] as unused, got %v", unused)
	}
}

func TestBaselineSaveLoadRoundTrip(t *testing.T) {
	vs := []Violation{
		{Boundary: "SyncOrders", WriteUnits: 3},
		{Boundary: "CreateUser", WriteUnits: 2},
		{Boundary: "CreateUser", WriteUnits: 2}, // duplicate collapses
	}
	path := filepath.Join(t.TempDir(), "txnproof-baseline.json")
	if err := BaselineFromViolations(vs).Save(path); err != nil {
		t.Fatal(err)
	}

	loaded, err := LoadBaseline(path)
	if err != nil {
		t.Fatal(err)
	}
	got := loaded.Boundaries()
	want := []string{"CreateUser", "SyncOrders"}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("expected %v, got %v", want, got)
	}
}

func TestBaselineSaveIsDeterministic(t *testing.T) {
	dir := t.TempDir()
	p1 := filepath.Join(dir, "a.json")
	p2 := filepath.Join(dir, "b.json")
	if err := NewBaseline().Add("B").Add("A").Add("C").Save(p1); err != nil {
		t.Fatal(err)
	}
	if err := NewBaseline().Add("C").Add("A").Add("B").Save(p2); err != nil {
		t.Fatal(err)
	}
	d1, err := os.ReadFile(p1)
	if err != nil {
		t.Fatal(err)
	}
	d2, err := os.ReadFile(p2)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(d1, d2) {
		t.Fatalf("saved baselines differ:\n%s\n---\n%s", d1, d2)
	}
	if !strings.Contains(string(d1), "\"comment\"") || !strings.Contains(string(d1), "txnproof baseline") {
		t.Errorf("baseline file should carry an explanatory comment field:\n%s", d1)
	}
	if !bytes.HasSuffix(d1, []byte("\n")) {
		t.Error("baseline file should end with a newline")
	}
	// Sorted entries keep diffs clean.
	if a, b, c := bytes.Index(d1, []byte(`"A"`)), bytes.Index(d1, []byte(`"B"`)), bytes.Index(d1, []byte(`"C"`)); a >= b || b >= c {
		t.Errorf("boundaries should be sorted:\n%s", d1)
	}
}

func TestBaselineSaveEmpty(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty.json")
	if err := NewBaseline().Save(path); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"boundaries": []`) {
		t.Errorf("empty baseline should marshal boundaries as [], not null:\n%s", data)
	}
	loaded, err := LoadBaseline(path)
	if err != nil {
		t.Fatal(err)
	}
	if bs := loaded.Boundaries(); len(bs) != 0 {
		t.Fatalf("expected empty baseline, got %v", bs)
	}
}

func TestLoadBaselineMissingFileIsError(t *testing.T) {
	_, err := LoadBaseline(filepath.Join(t.TempDir(), "nope.json"))
	if !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("expected fs.ErrNotExist, got %v", err)
	}
}

func TestLoadBaselineMalformedFileIsError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.json")
	if err := os.WriteFile(path, []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadBaseline(path); err == nil {
		t.Fatal("expected an error for malformed baseline file")
	}
}

func TestBaselineReporterForwardsOptionalInterfaces(t *testing.T) {
	det, cr, db := setupWithBaseline(t, NewBaseline())

	// Unbounded write: no boundary on the context.
	if _, err := db.ExecContext(context.Background(), "INSERT INTO a (id) VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	if uw := cr.UnboundedWrites(); len(uw) != 1 {
		t.Fatalf("expected 1 unbounded write through the wrapper, got %d", len(uw))
	}

	// Stale allow: allowed boundary that does not violate.
	ctx, finish := det.StartBoundary(context.Background(), "BestEffortAudit",
		AllowNonAtomic("best-effort by design (TICKET-123)"))
	mustExec(t, db, ctx, "INSERT INTO a (id) VALUES (1)")
	finish.Finish()
	if sa := cr.StaleAllows(); len(sa) != 1 {
		t.Fatalf("expected 1 stale allow through the wrapper, got %d", len(sa))
	}
}

func TestNilBaselineSuppressesNothing(t *testing.T) {
	det, cr, db := setupWithBaseline(t, nil)
	violate(t, det, db, "CreateUser")
	if vs := cr.Violations(); len(vs) != 1 {
		t.Fatalf("expected 1 violation with nil baseline, got %+v", vs)
	}
}

func TestBaselineAdoptionWorkflow(t *testing.T) {
	path := filepath.Join(t.TempDir(), "txnproof-baseline.json")

	// First adoption: run without a baseline, capture and save violations.
	{
		det, cr, db := setup(t)
		violate(t, det, db, "LegacyImport")
		violate(t, det, db, "SyncOrders")
		if err := BaselineFromViolations(cr.Violations()).Save(path); err != nil {
			t.Fatal(err)
		}
	}

	// Subsequent runs: load the baseline; only new violations fail, and stale
	// entries surface for removal.
	b, err := LoadBaseline(path)
	if err != nil {
		t.Fatal(err)
	}
	det, cr, db := setupWithBaseline(t, b)
	violate(t, det, db, "LegacyImport") // still broken: tolerated
	violate(t, det, db, "CreateUser")   // new: fails

	ft := &fakeT{}
	cr.RequireNoViolations(ft)
	if len(ft.errors) != 1 {
		t.Fatalf("expected exactly the new violation to fail, got %d errors", len(ft.errors))
	}
	unused := b.UnusedEntries()
	if len(unused) != 1 || unused[0] != "SyncOrders" {
		t.Fatalf("expected [SyncOrders] as stale baseline entry, got %v", unused)
	}
}
