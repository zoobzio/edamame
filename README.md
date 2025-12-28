# edamame

[![CI Status](https://github.com/zoobzio/edamame/workflows/CI/badge.svg)](https://github.com/zoobzio/edamame/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/zoobzio/edamame/graph/badge.svg?branch=main)](https://codecov.io/gh/zoobzio/edamame)
[![Go Report Card](https://goreportcard.com/badge/github.com/zoobzio/edamame)](https://goreportcard.com/report/github.com/zoobzio/edamame)
[![CodeQL](https://github.com/zoobzio/edamame/workflows/CodeQL/badge.svg)](https://github.com/zoobzio/edamame/security/code-scanning)
[![Go Reference](https://pkg.go.dev/badge/github.com/zoobzio/edamame.svg)](https://pkg.go.dev/github.com/zoobzio/edamame)
[![License](https://img.shields.io/github/license/zoobzio/edamame)](LICENSE)
[![Go Version](https://img.shields.io/github/go-mod/go-version/zoobzio/edamame)](go.mod)
[![Release](https://img.shields.io/github/v/release/zoobzio/edamame)](https://github.com/zoobzio/edamame/releases)

Runtime query factory for Go.

Define database queries as data, register them at runtime, reconfigure without rebuilding.

## Queries as Data

Edamame treats queries as specs—pure data structures you can create, store, and swap at runtime.

```go
// A query is just data
spec := edamame.QuerySpec{
    Where:   []edamame.ConditionSpec{{Field: "status", Operator: "=", Param: "status"}},
    OrderBy: []edamame.OrderBySpec{{Field: "created_at", Direction: "desc"}},
    Limit:   ptr(50),
}

// Register it
factory.AddQuery(edamame.QueryCapability{Name: "by-status", Spec: spec})

// Execute by name
users, _ := factory.ExecQuery(ctx, "by-status", map[string]any{"status": "active"})

// Later: hot-swap without rebuild
factory.RemoveQuery("by-status")
factory.AddQuery(edamame.QueryCapability{Name: "by-status", Spec: optimizedSpec})
```

No code generation. No rebuild. Change queries in production.

## Install

```bash
go get github.com/zoobzio/edamame
```

Requires Go 1.24+. Supports PostgreSQL, MySQL, SQLite, and SQL Server via [astql](https://github.com/zoobzio/astql).

## Quick Start

```go
package main

import (
    "context"
    "fmt"

    "github.com/jmoiron/sqlx"
    _ "github.com/lib/pq" // or mysql, sqlite3, mssql driver
    "github.com/zoobzio/astql/pkg/postgres" // or mysql, sqlite, mssql
    "github.com/zoobzio/edamame"
)

type User struct {
    ID     int    `db:"id" type:"integer" constraints:"primarykey"`
    Email  string `db:"email" type:"text" constraints:"notnull,unique"`
    Name   string `db:"name" type:"text"`
    Status string `db:"status" type:"text"`
}

func main() {
    db, _ := sqlx.Connect("postgres", "postgres://localhost/mydb?sslmode=disable")
    ctx := context.Background()

    // Create factory (auto-registers select, query, delete, count)
    factory, _ := edamame.New[User](db, "users", postgres.New())

    // Use built-in capabilities
    users, _ := factory.ExecQuery(ctx, "query", nil)
    user, _ := factory.ExecSelect(ctx, "select", map[string]any{"id": 1})
    count, _ := factory.ExecAggregate(ctx, "count", nil)

    // Add custom capability
    factory.AddQuery(edamame.QueryCapability{
        Name: "active",
        Spec: edamame.QuerySpec{
            Where: []edamame.ConditionSpec{
                {Field: "status", Operator: "=", Param: "status"},
            },
        },
    })

    active, _ := factory.ExecQuery(ctx, "active", map[string]any{"status": "active"})
    fmt.Printf("%d users, %d active\n", len(users), len(active))
}
```

## Runtime Reconfiguration

The real value: modify query behavior without touching code.

```go
// Load specs from config, database, or remote source
specs := loadQuerySpecs("queries.json")

for _, s := range specs {
    factory.AddQuery(s)
}

// Query running slow? Swap it out
factory.RemoveQuery("expensive-query")
factory.AddQuery(optimizedVersion)

// Export what's available
json, _ := factory.SpecJSON()
```

Specs are JSON-serializable. Store them anywhere. Load them anytime.

## Why edamame?

- **No build cycle** — define and modify queries at runtime
- **Hot reconfiguration** — swap underperforming queries in production
- **Specs are data** — JSON-serializable, storable, versionable
- **Type-safe** — generic `Factory[T]` with compile-time safety via [cereal](https://github.com/zoobzio/cereal)
- **Named capabilities** — queries registered once, executed by name
- **Thread-safe** — concurrent reads, serialized writes

## Documentation

### Learn
- [Quickstart](docs/2.learn/1.quickstart.md)
- [Core Concepts](docs/2.learn/2.concepts.md)
- [Architecture](docs/2.learn/3.architecture.md)

### Guides
- [Capabilities](docs/3.guides/1.capabilities.md)
- [Testing](docs/3.guides/2.testing.md)

### Reference
- [API Reference](docs/5.reference/1.api.md)

## Contributing

```bash
make test
make lint
```

See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

MIT
