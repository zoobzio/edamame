# edamame

[![CI Status](https://github.com/zoobzio/edamame/workflows/CI/badge.svg)](https://github.com/zoobzio/edamame/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/zoobzio/edamame/graph/badge.svg?branch=main)](https://codecov.io/gh/zoobzio/edamame)
[![Go Report Card](https://goreportcard.com/badge/github.com/zoobzio/edamame)](https://goreportcard.com/report/github.com/zoobzio/edamame)
[![CodeQL](https://github.com/zoobzio/edamame/workflows/CodeQL/badge.svg)](https://github.com/zoobzio/edamame/security/code-scanning)
[![Go Reference](https://pkg.go.dev/badge/github.com/zoobzio/edamame.svg)](https://pkg.go.dev/github.com/zoobzio/edamame)
[![License](https://img.shields.io/github/license/zoobzio/edamame)](LICENSE)
[![Go Version](https://img.shields.io/github/go-mod/go-version/zoobzio/edamame)](go.mod)
[![Release](https://img.shields.io/github/v/release/zoobzio/edamame)](https://github.com/zoobzio/edamame/releases)

Capability-driven query factories for Go with JSON-serializable specs and LLM integration.

## The Problem

Database access in Go typically means scattered query logic or monolithic ORMs:

```go
// Scattered: SQL strings everywhere, no discoverability
rows, _ := db.Query("SELECT * FROM users WHERE age >= $1 ORDER BY name", minAge)

// ORM: Magic methods, hard to introspect, no safe way to expose to LLMs
db.Where("age >= ?", minAge).Order("name").Find(&users)
```

When you need to expose query capabilities to an LLM or API consumer, there's no structured way to describe what operations are available, what parameters they accept, or how to validate inputs.

## The Solution

Edamame registers queries as named capabilities with JSON-serializable specs:

```go
import "github.com/zoobzio/astql/pkg/postgres"

factory, _ := edamame.New[User](db, "users", postgres.New())

// Register a capability
factory.AddQuery(edamame.QueryCapability{
    Name:        "adults",
    Description: "Find users 18 and older",
    Spec: edamame.QuerySpec{
        Where:   []edamame.ConditionSpec{{Field: "age", Operator: ">=", Param: "min_age"}},
        OrderBy: []edamame.OrderBySpec{{Field: "name", Direction: "asc"}},
    },
})

// Execute by name
users, _ := factory.ExecQuery(ctx, "adults", map[string]any{"min_age": 18})

// Export for LLMs
json, _ := factory.SpecJSON()
```

You get:

- **Named capabilities** — queries registered once, executed by name
- **JSON specs** — export what's available, with params and types
- **LLM-safe execution** — AI picks capability + params, you validate and execute
- **Type-safe results** — queries return `*User` or `[]*User`, not `interface{}`

## Features

- **Declarative** — define what, not how. Specs are pure data.
- **Type-safe** — generic factories with compile-time safety via [cereal](https://github.com/zoobzio/cereal)
- **Introspectable** — export specs as JSON for documentation or LLMs
- **Parameterized** — all queries use sqlx named params, no injection
- **Multi-operation** — queries, selects, updates, deletes, aggregates, compounds
- **Testable** — test capability registration without a database

## Use Cases

- [Integrate with LLMs](docs/4.cookbook/1.llm-integration.md) — expose capabilities to AI assistants safely

## Install

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
    "github.com/zoobzio/astql/pkg/postgres"
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
    factory, _ := edamame.New[User](db, "users", postgres.New())

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
    users, _ := factory.ExecQuery(ctx, "adults", map[string]any{"min_age": 18})

    for _, u := range users {
        fmt.Printf("%s (%d)\n", u.Name, *u.Age)
    }
}
```

## API Reference

| Function | Purpose |
|----------|---------|
| `New[T](db, table, renderer)` | Create factory for type T |
| `AddQuery(cap)` | Register query capability |
| `AddSelect(cap)` | Register single-record select |
| `AddUpdate(cap)` | Register update capability |
| `AddDelete(cap)` | Register delete capability |
| `AddAggregate(cap)` | Register aggregate (count, sum, avg, min, max) |
| `ExecQuery(ctx, name, params)` | Execute query, return `[]*T` |
| `ExecSelect(ctx, name, params)` | Execute select, return `*T` |
| `ExecUpdate(ctx, name, params)` | Execute update, return `*T` |
| `ExecDelete(ctx, name, params)` | Execute delete, return count |
| `ExecAggregate(ctx, name, params)` | Execute aggregate, return value |
| `ExecInsert(ctx, record)` | Insert record, return `*T` |
| `Spec()` | Get all capabilities as struct |
| `SpecJSON()` | Get all capabilities as JSON |

See [API Reference](docs/5.reference/1.api.md) for complete documentation.

## Default Capabilities

Every factory includes these out of the box:

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

## Introspection

Export capabilities for LLMs or API documentation:

```go
spec := factory.Spec()
// spec.Table: "users"
// spec.Queries: [{Name: "adults", Params: [{Name: "min_age", Type: "integer", Required: true}]}]

json, _ := factory.SpecJSON()
```

```json
{
  "table": "users",
  "queries": [
    {
      "name": "adults",
      "description": "Find users 18 and older",
      "params": [
        {"name": "min_age", "type": "integer", "required": true}
      ]
    }
  ]
}
```

The LLM picks capabilities and params—your code validates and executes.

## Documentation

- [Overview](docs/1.overview.md) — what edamame does and why

### Learn
- [Quickstart](docs/2.learn/1.quickstart.md) — get started in minutes
- [Core Concepts](docs/2.learn/2.concepts.md) — factories, capabilities, specs
- [Architecture](docs/2.learn/3.architecture.md) — how edamame works with cereal

### Guides
- [Capabilities](docs/3.guides/1.capabilities.md) — adding custom queries, updates, deletes
- [Testing](docs/3.guides/2.testing.md) — unit tests, integration tests, benchmarks

### Cookbook
- [LLM Integration](docs/4.cookbook/1.llm-integration.md) — using specs with AI assistants

### Reference
- [API Reference](docs/5.reference/1.api.md) — complete function and type documentation

## Contributing

Contributions welcome! Please ensure:
- Tests pass: `make test`
- Code is formatted: `go fmt ./...`
- No lint errors: `make lint`

See [CONTRIBUTING.md](CONTRIBUTING.md) for details.

## License

MIT License — see [LICENSE](LICENSE) for details.
