package txnproof

import (
	"context"
	"log/slog"
)

// BoundaryAttr is one string-keyed contextual value attached to a boundary
// and carried into every Violation the boundary produces. Use it to tie a
// report back to the execution that produced it (trace ID, request ID,
// user ID).
type BoundaryAttr struct {
	Key   string
	Value any
}

// Attr constructs a BoundaryAttr.
func Attr(key string, value any) BoundaryAttr { return BoundaryAttr{Key: key, Value: value} }

// SlogAttrs converts boundary attrs to log/slog attrs, for reporters built on
// slog. SlogReporter already applies it to the attrs it receives.
func SlogAttrs(attrs []BoundaryAttr) []slog.Attr {
	out := make([]slog.Attr, len(attrs))
	for i, a := range attrs {
		out[i] = slog.Any(a.Key, a.Value)
	}
	return out
}

// WithBoundaryAttrs attaches static attrs to a single boundary at
// StartBoundary / InBoundary — for values the caller already has at hand:
//
//	ctx, finish := detector.StartBoundary(ctx, "CreateUser",
//		txnproof.WithBoundaryAttrs(txnproof.Attr("user_id", userID)))
//
// They are appended after any attrs produced by WithBoundaryAttrsFunc.
// Duplicate keys are kept in order, never deduplicated.
func WithBoundaryAttrs(attrs ...BoundaryAttr) BoundaryOption {
	return func(b *boundary) { b.attrs = append(b.attrs, attrs...) }
}

// WithBoundaryAttrsFunc installs a detector-level extractor that derives
// attrs from the context — the middleware-friendly way to stamp every
// boundary with trace/request IDs: set it up once and every Violation carries
// them for free.
//
// f is evaluated once per boundary at StartBoundary (never per statement),
// with the context StartBoundary received; its attrs come first, followed by
// any per-boundary WithBoundaryAttrs. When unbounded-write detection is on,
// f is also evaluated once per unbounded write at record time (with the
// statement's context) and the result is delivered on the StatementRecord.
func WithBoundaryAttrsFunc(f func(ctx context.Context) []BoundaryAttr) Option {
	return func(d *Detector) { d.attrsFunc = f }
}
