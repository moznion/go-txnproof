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
	used   bool
}

// NewAllowlist creates an empty Allowlist.
func NewAllowlist() *Allowlist {
	return &Allowlist{entries: map[string]*allowlistEntry{}}
}

// Add registers a boundary name as intentionally non-atomic. The reason should
// say why and reference a ticket. Returns the Allowlist for chaining.
func (a *Allowlist) Add(boundary, reason string) *Allowlist {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.entries[boundary] = &allowlistEntry{reason: reason}
	return a
}

// allow reports whether the boundary is allowlisted, marking the entry used.
func (a *Allowlist) allow(boundary string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	e, ok := a.entries[boundary]
	if ok {
		e.used = true
	}
	return ok
}

// UnusedEntries returns the boundary names that never suppressed a violation,
// sorted. A non-empty result in CI means the allowlist has stale entries that
// should be removed.
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
