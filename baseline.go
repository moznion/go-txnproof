package txnproof

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"sync"
)

// baselineFileComment is written into every saved baseline file so human
// readers know what the file is and how it is meant to evolve.
const baselineFileComment = "txnproof baseline: boundaries that already executed non-atomic writes when txnproof was adopted. Their violations are tolerated until fixed; new violations still fail. Remove entries as boundaries get fixed (the ratchet only goes down); regenerate deliberately with Baseline.Save, never automatically."

// baselineFile is the on-disk JSON representation of a Baseline.
type baselineFile struct {
	Comment    string   `json:"comment"`
	Boundaries []string `json:"boundaries"`
}

// Baseline is the ratchet helper for adopting txnproof on an existing
// codebase: capture the current violations once (BaselineFromViolations +
// Save), commit the file, and from then on only new violations fail —
// baselined boundaries are tolerated until fixed.
//
// Entries are keyed on the boundary name alone. Write-unit counts and
// statement text vary by data and code path, so they would make the baseline
// unstable across runs; the boundary name is the stable identifier.
//
// To keep the ratchet going down, every entry tracks whether it actually
// suppressed a violation; check UnusedEntries in CI and fail when an entry no
// longer matches anything — the same discipline as Allowlist.UnusedEntries.
type Baseline struct {
	mu      sync.Mutex
	entries map[string]bool // boundary name -> suppressed a violation
}

// NewBaseline creates an empty Baseline.
func NewBaseline() *Baseline {
	return &Baseline{entries: map[string]bool{}}
}

// BaselineFromViolations builds a Baseline from the boundary names of the
// given violations (typically CollectingReporter.Violations after a full run
// without any baseline installed). Duplicate boundaries collapse into one
// entry.
func BaselineFromViolations(vs []Violation) *Baseline {
	b := NewBaseline()
	for _, v := range vs {
		b.entries[v.Boundary] = false
	}
	return b
}

// Add registers a boundary name in the baseline. Returns the Baseline for
// chaining. Prefer BaselineFromViolations + Save for the normal adoption
// flow; Add exists for programmatic construction.
func (b *Baseline) Add(boundary string) *Baseline {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.entries[boundary]; !ok {
		b.entries[boundary] = false
	}
	return b
}

// Boundaries returns the baselined boundary names, sorted.
func (b *Baseline) Boundaries() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]string, 0, len(b.entries))
	for name := range b.entries {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// covers reports whether the boundary is baselined, marking the entry used.
func (b *Baseline) covers(boundary string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.entries[boundary]; !ok {
		return false
	}
	b.entries[boundary] = true
	return true
}

// UnusedEntries returns the baselined boundary names that never suppressed a
// violation, sorted. A non-empty result in CI means those boundaries are
// fixed: remove their entries from the baseline file so the ratchet keeps
// going down.
func (b *Baseline) UnusedEntries() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []string
	for name, used := range b.entries {
		if !used {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}

// Save writes the baseline to path as deterministic, human-readable JSON:
// indented, boundaries sorted, with a comment field explaining the file, so
// diffs stay clean. Call it deliberately — on first adoption and on
// intentional regeneration — never on every run.
func (b *Baseline) Save(path string) error {
	f := baselineFile{
		Comment:    baselineFileComment,
		Boundaries: b.Boundaries(),
	}
	data, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		return fmt.Errorf("txnproof: marshal baseline: %w", err)
	}
	data = append(data, '\n')
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("txnproof: write baseline file: %w", err)
	}
	return nil
}

// LoadBaseline reads a baseline file written by Save. A missing file is an
// error (check with errors.Is against fs.ErrNotExist): creating the baseline
// must stay a deliberate Save call, not a silent fallback.
func LoadBaseline(path string) (*Baseline, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("txnproof: read baseline file: %w", err)
	}
	var f baselineFile
	if err := json.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("txnproof: parse baseline file %s: %w", path, err)
	}
	b := NewBaseline()
	for _, name := range f.Boundaries {
		b.entries[name] = false
	}
	return b, nil
}

// BaselineReporter filters violations through a Baseline before forwarding
// them to the wrapped Reporter: violations of baselined boundaries are
// swallowed (marking the entry used), everything else passes through.
// Unbounded-write and stale-allow reports are never baselined and are
// forwarded unchanged when the wrapped Reporter implements the corresponding
// interfaces.
type BaselineReporter struct {
	baseline *Baseline
	next     Reporter
}

var (
	_ Reporter               = (*BaselineReporter)(nil)
	_ UnboundedWriteReporter = (*BaselineReporter)(nil)
	_ StaleAllowReporter     = (*BaselineReporter)(nil)
	_ NestedBoundaryReporter = (*BaselineReporter)(nil)
)

// NewBaselineReporter wraps next so that violations of boundaries in baseline
// are suppressed. A nil baseline suppresses nothing.
func NewBaselineReporter(baseline *Baseline, next Reporter) *BaselineReporter {
	return &BaselineReporter{baseline: baseline, next: next}
}

func (r *BaselineReporter) Report(ctx context.Context, v Violation) {
	if r.baseline != nil && r.baseline.covers(v.Boundary) {
		return
	}
	r.next.Report(ctx, v)
}

func (r *BaselineReporter) ReportUnboundedWrite(ctx context.Context, s StatementRecord) {
	if ur, ok := r.next.(UnboundedWriteReporter); ok {
		ur.ReportUnboundedWrite(ctx, s)
	}
}

func (r *BaselineReporter) ReportStaleAllow(ctx context.Context, s StaleAllow) {
	if sr, ok := r.next.(StaleAllowReporter); ok {
		sr.ReportStaleAllow(ctx, s)
	}
}

func (r *BaselineReporter) ReportNestedBoundary(ctx context.Context, n NestedBoundary) {
	if nr, ok := r.next.(NestedBoundaryReporter); ok {
		nr.ReportNestedBoundary(ctx, n)
	}
}
