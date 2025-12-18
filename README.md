# edamame

[![CI Status](https://github.com/zoobzio/edamame/workflows/CI/badge.svg)](https://github.com/zoobzio/edamame/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/zoobzio/edamame/graph/badge.svg?branch=main)](https://codecov.io/gh/zoobzio/edamame)
[![Go Report Card](https://goreportcard.com/badge/github.com/zoobzio/edamame)](https://goreportcard.com/report/github.com/zoobzio/edamame)
[![CodeQL](https://github.com/zoobzio/edamame/workflows/CodeQL/badge.svg)](https://github.com/zoobzio/edamame/security/code-scanning)
[![Go Reference](https://pkg.go.dev/badge/github.com/zoobzio/edamame.svg)](https://pkg.go.dev/github.com/zoobzio/edamame)
[![License](https://img.shields.io/github/license/zoobzio/edamame)](LICENSE)
[![Go Version](https://img.shields.io/github/go-mod/go-version/zoobzio/edamame)](go.mod)
[![Release](https://img.shields.io/github/v/release/zoobzio/edamame)](https://github.com/zoobzio/edamame/releases)

Declarative database capabilities for Go with introspection and LLM integration.

Define queries as specs, register them as capabilities, execute with params—edamame handles the SQL.

## Three Primitives

```go
// Factory - coordinator for a model type
factory, _ := edamame.New[User](db, "users")

// Capability - named, reusable operation
factory.AddQuery(edamame.QueryCapability{
    Name: "active-users",
    Spec: edamame.QuerySpec{
        Where: []edamame.ConditionSpec{
            {Field: "active", Operator: "=", Param: "active"},
        },
    },
})

// Execution - run with params
users, _ := factory.ExecQuery(ctx, "active-users", map[string]any{
    "active": true,
})
```

No SQL strings, no builder chains—just specs and execution.

## Installation

```bash
go get github.com/zoobzio/edamame
```

Requires Go 1.24+ and PostgreSQL.

## Quick Start

```go
package main

import (
    "context"
    "fmt"
    "github.com/jmoiron/sqlx"
    "github.com/zoobzio/edamame"
    _ "github.com/lib/pq"
)

type User struct {
    ID    int    `db:"id" type:"integer" constraints:"primarykey"`
    Email string `db:"email" type:"text" constraints:"notnull,unique"`
    Name  string `db:"name" type:"text"`
    Age   *int   `db:"age" type:"integer"`
}

func main() {
    db, _ := sqlx.Connect("postgres", "postgres://localhost/mydb?sslmode=disable")

    // Create factory with default capabilities
    factory, _ := edamame.New[User](db, "users")

    // Add custom capability
    factory.AddQuery(edamame.QueryCapability{
        Name:        "adults",
        Description: "Find users 18 and older",
        Spec: edamame.QuerySpec{
            Where: []edamame.ConditionSpec{
                {Field: "age", Operator: ">=", Param: "min_age"},
            },
            OrderBy: []edamame.OrderBySpec{
                {Field: "name", Direction: "asc"},
            },
        },
    })

    // Execute
    ctx := context.Background()
    users, _ := factory.ExecQuery(ctx, "adults", map[string]any{
        "min_age": 18,
    })

    for _, u := range users {
        fmt.Printf("%s (%d)\n", u.Name, *u.Age)
    }
}
```

## Default Capabilities

Every factory comes with these out of the box:

| Name | Type | Description |
|------|------|-------------|
| `query` | Query | Select all records |
| `select` | Select | Select by primary key |
| `delete` | Delete | Delete by primary key |
| `count` | Aggregate | Count all records |

```go
// Use defaults immediately
users, _ := factory.ExecQuery(ctx, "query", nil)
user, _ := factory.ExecSelect(ctx, "select", map[string]any{"id": 123})
count, _ := factory.ExecAggregate(ctx, "count", nil)
```

## LLM Integration

Export specs as JSON for AI assistants:

```go
json, _ := factory.SpecJSON()
// Provide to LLM as context, execute responses safely
```

```json
{
  "table": "users",
  "queries": [
    {"name": "adults", "description": "Find users 18 and older", "params": [
      {"name": "min_age", "type": "integer", "required": true}
    ]}
  ]
}
```

The LLM picks capabilities and params—your code validates and executes.

## Why edamame?

- **Declarative** — Define what, not how. Specs are pure data.
- **Type-safe** — Generic factories with compile-time safety
- **Introspectable** — Export specs as JSON for documentation or LLMs
- **Parameterized** — All queries use sqlx named params, no injection
- **Minimal** — No ORM, no migrations, no magic. Just capabilities.
- **Testable** — Test capability registration without a database

## Documentation

Full documentation is available in the [docs/](docs/) directory:

- [Overview](docs/1.overview.md) — What edamame is and why

### Learn
- [Quickstart](docs/2.learn/1.quickstart.md) — Get started in minutes
- [Core Concepts](docs/2.learn/2.concepts.md) — Factories, capabilities, specs
- [Architecture](docs/2.learn/3.architecture.md) — How edamame works with cereal

### Guides
- [Capabilities](docs/3.guides/1.capabilities.md) — Adding custom queries, updates, deletes
- [Testing](docs/3.guides/2.testing.md) — Unit tests, integration tests, benchmarks

### Cookbook
- [LLM Integration](docs/4.cookbook/1.llm-integration.md) — Using specs with AI assistants

### Reference
- [API Reference](docs/5.reference/1.api.md) — Complete function and type documentation

## Contributing

Contributions welcome! Please ensure:
- Tests pass: `make test`
- Code is formatted: `go fmt ./...`
- No lint errors: `make lint`

See [CONTRIBUTING.md](CONTRIBUTING.md) for details.

## License

MIT License — see [LICENSE](LICENSE) for details.
