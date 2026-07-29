package txnproof

import (
	"sort"
	"sync"
)

// Allowlist suppresses violations for boundaries that are intentionally
// non-atomic (e.g. best-effort audit writes, writes spanning databases that a
// single transaction cannot cover).
//
// To keep the list from rotting, every entry tracks whether it actually
// suppressed a violation; check UnusedEntries in CI and fail when an entry no
// longer matches anything (the same discipline as unused //nolint directives).
type Allowlist struct {
	mu      sync.Mutex
	entries map[string]*allowlistEntry
}

type allowlistEntry struct {
	reason string
	// units are the exact write-unit counts the entry covers. Empty means the
	// entry is unconstrained (any violating count is suppressed).
	units []int
	used  bool
}

// NewAllowlist creates an empty Allowlist.
func NewAllowlist() *Allowlist {
	return &Allowlist{entries: map[string]*allowlistEntry{}}
}

// Add registers a boundary name as intentionally non-atomic. The reason should
// say why and reference a ticket. Returns the Allowlist for chaining.
//
// The optional exactWriteUnits pin how much non-atomicity the entry covers,
// exactly as for the in-code AllowNonAtomic mark: the entry then suppresses
// only boundaries finishing with one of the given write-unit counts, and any
// other count is reported as a Violation. Pass several counts for a boundary
// whose write count legitimately differs per code path. A write unit is one
// transaction that contained at least one write, or one auto-commit write (the
// same number reported as Violation.WriteUnits), so counts below 2 can never
// match a violation and leave the entry permanently unused.
func (a *Allowlist) Add(boundary, reason string, exactWriteUnits ...int) *Allowlist {
	a.mu.Lock()
	defer a.mu.Unlock()
	// Copied so that a caller's slice cannot change what the entry allows.
	a.entries[boundary] = &allowlistEntry{reason: reason, units: append([]int(nil), exactWriteUnits...)}
	return a
}

// allow reports whether the boundary is allowlisted for a violation of the
// given write-unit count, marking the entry used when it is. An entry
// constrained to exact counts does not suppress (and is not marked used) for a
// count it does not cover: that violation is unreviewed and must be reported.
// In that case the entry's counts are returned as declined, so the Violation
// can say why the entry did not apply.
func (a *Allowlist) allow(boundary string, units int) (allowed bool, declined []int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	e, ok := a.entries[boundary]
	if !ok {
		return false, nil
	}
	if !coversWriteUnits(e.units, units) {
		return false, e.units
	}
	e.used = true
	return true, nil
}

// UnusedEntries returns the boundary names that never suppressed a violation,
// sorted. A non-empty result in CI means the allowlist has stale entries that
// should be removed — or, for an entry constrained to exact write-unit counts,
// that the boundary now violates with a count the entry does not cover (it is
// then reported as a Violation as well, and the entry needs reviewing rather
// than deleting).
func (a *Allowlist) UnusedEntries() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	var out []string
	for name, e := range a.entries {
		if !e.used {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}
