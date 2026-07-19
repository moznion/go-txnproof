// Command graphql shows how to wire go-txnproof into a GraphQL server built
// with github.com/graphql-go/graphql: WithBoundary wraps a resolver so it
// runs inside a txnproof boundary named "<ParentType>.<field>" (e.g.
// "Mutation.createUser"), and WrapResolvers applies that to every resolver
// of a type generically — apply it to each object type's fields and the
// whole schema is covered.
//
// The example runs against detector.NewNullDB(), so it needs zero
// infrastructure: every statement succeeds and only the
// statement/transaction timeline is observed. In a real application you
// would wrap your actual driver instead (see the root README) — the
// resolver wiring is identical.
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"

	"github.com/graphql-go/graphql"
	txnproof "github.com/moznion/go-txnproof"
)

// WithBoundary wraps one resolver so it runs inside a txnproof boundary named
// "<ParentType>.<field>". Statements must be executed with p.Context to be
// attributed to the boundary, so the wrapper swaps in the boundary context
// before delegating.
func WithBoundary(detector *txnproof.Detector, resolve graphql.FieldResolveFn) graphql.FieldResolveFn {
	return func(p graphql.ResolveParams) (any, error) {
		name := fmt.Sprintf("%s.%s", p.Info.ParentType.Name(), p.Info.FieldName)
		var out any
		err := detector.InBoundary(p.Context, name, func(ctx context.Context) error {
			p.Context = ctx
			var resolveErr error
			out, resolveErr = resolve(p)
			return resolveErr
		})
		return out, err
	}
}

// WrapResolvers applies WithBoundary to every resolver in fields. This is
// the generic per-resolver wiring: call it once for each object type when
// building the schema and every resolver gets its own boundary.
func WrapResolvers(detector *txnproof.Detector, fields graphql.Fields) graphql.Fields {
	for _, f := range fields {
		if f.Resolve != nil {
			f.Resolve = WithBoundary(detector, f.Resolve)
		}
	}
	return fields
}

func buildSchema(detector *txnproof.Detector, db *sql.DB) (graphql.Schema, error) {
	queryType := graphql.NewObject(graphql.ObjectConfig{
		Name: "Query",
		Fields: WrapResolvers(detector, graphql.Fields{
			"user": &graphql.Field{
				Type: graphql.String,
				// Reads never count as write units; this resolver is
				// always clean no matter how many SELECTs it runs.
				Resolve: func(p graphql.ResolveParams) (any, error) {
					rows, err := db.QueryContext(p.Context, "SELECT name FROM users WHERE id = 1")
					if err != nil {
						return nil, err
					}
					defer func() { _ = rows.Close() }()
					return "alice", nil
				},
			},
		}),
	})

	mutationType := graphql.NewObject(graphql.ObjectConfig{
		Name: "Mutation",
		Fields: WrapResolvers(detector, graphql.Fields{
			// createUser performs two auto-commit writes: structurally
			// non-atomic. A crash between the two INSERTs leaves a user
			// without its audit record — txnproof reports the boundary
			// "Mutation.createUser" as a violation.
			"createUser": &graphql.Field{
				Type: graphql.Boolean,
				Resolve: func(p graphql.ResolveParams) (any, error) {
					ctx := p.Context
					if _, err := db.ExecContext(ctx, "INSERT INTO users (name) VALUES ('alice')"); err != nil {
						return nil, err
					}
					if _, err := db.ExecContext(ctx, "INSERT INTO audit_log (event) VALUES ('user created')"); err != nil {
						return nil, err
					}
					return true, nil
				},
			},
			// createOrder performs the same two writes inside a single
			// transaction: one atomic unit, so txnproof stays silent.
			"createOrder": &graphql.Field{
				Type: graphql.Boolean,
				Resolve: func(p graphql.ResolveParams) (any, error) {
					ctx := p.Context
					tx, err := db.BeginTx(ctx, nil)
					if err != nil {
						return nil, err
					}
					defer func() { _ = tx.Rollback() }() // no-op after Commit
					if _, err := tx.ExecContext(ctx, "INSERT INTO orders (item) VALUES ('book')"); err != nil {
						return nil, err
					}
					if _, err := tx.ExecContext(ctx, "INSERT INTO audit_log (event) VALUES ('order created')"); err != nil {
						return nil, err
					}
					if err := tx.Commit(); err != nil {
						return nil, err
					}
					return true, nil
				},
			},
		}),
	})

	return graphql.NewSchema(graphql.SchemaConfig{Query: queryType, Mutation: mutationType})
}

func main() {
	// Report violations through slog. Timestamps are stripped only to keep
	// this example's output stable; in production use your regular logger.
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if len(groups) == 0 && a.Key == slog.TimeKey {
				return slog.Attr{}
			}
			return a
		},
	}))
	detector := txnproof.New(txnproof.WithReporter(txnproof.NewSlogReporter(logger)))
	db := detector.NewNullDB()
	defer func() { _ = db.Close() }()

	schema, err := buildSchema(detector, db)
	if err != nil {
		fmt.Fprintln(os.Stderr, "schema:", err)
		os.Exit(1)
	}

	execute(schema, "mutation { createUser }", "two auto-commit INSERTs -> violation expected")
	execute(schema, "mutation { createOrder }", "both INSERTs in one transaction -> clean")
	execute(schema, "{ user }", "read-only resolver -> clean")
}

func execute(schema graphql.Schema, request, comment string) {
	fmt.Printf("-> %s (%s)\n", request, comment)
	result := graphql.Do(graphql.Params{
		Schema:        schema,
		RequestString: request,
		Context:       context.Background(),
	})
	if len(result.Errors) > 0 {
		fmt.Fprintln(os.Stderr, "errors:", result.Errors)
		os.Exit(1)
	}
	fmt.Printf("   data: %v\n", result.Data)
}
