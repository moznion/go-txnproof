# GraphQL resolver middleware example

Wires go-txnproof into a GraphQL server built with
[`github.com/graphql-go/graphql`](https://github.com/graphql-go/graphql)
(a runtime schema library, no codegen — the dependency lives only in this
example module):

- `WithBoundary` wraps one resolver so it runs inside a txnproof boundary
  named `"<ParentType>.<field>"` (e.g. `Mutation.createUser`). Statements
  are attributed via `p.Context`, so the wrapper swaps in the boundary
  context before delegating.
- `WrapResolvers` applies `WithBoundary` to every resolver of a type — the
  generic per-resolver wiring: call it once per object type when building
  the schema and the whole schema is covered.

The example runs against `detector.NewNullDB()`, so no database is needed.
Three operations are exercised:

- `mutation { createUser }` — two auto-commit `INSERT`s: non-atomic, reported.
- `mutation { createOrder }` — the same two writes inside one transaction: clean.
- `{ user }` — read-only resolver: reads never count, clean.

## Run

```console
go run .
```

Expected output:

```
-> mutation { createUser } (two auto-commit INSERTs -> violation expected)
level=ERROR msg="txnproof: non-atomic SQL execution detected" boundary=Mutation.createUser write_units=2 writes="[[auto-commit] INSERT INTO users (name) VALUES ('alice') [auto-commit] INSERT INTO audit_log (event) VALUES ('user created')]"
   data: map[createUser:true]
-> mutation { createOrder } (both INSERTs in one transaction -> clean)
   data: map[createOrder:true]
-> { user } (read-only resolver -> clean)
   data: map[user:alice]
```

The same pattern applies to other GraphQL libraries: gqlgen exposes
`AroundFields`/`AroundOperations` extension hooks, and graph-gophers supports
resolver middleware — anywhere you can intercept a resolver call with its
`context.Context`, start a boundary named after the operation or field and
finish it when the resolver returns.

In a real application, keep the wiring as-is and wrap your actual driver
instead of using `NewNullDB` (see the root README's Quick start).
