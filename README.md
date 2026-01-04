# edamame

[![CI Status](https://github.com/zoobzio/edamame/workflows/CI/badge.svg)](https://github.com/zoobzio/edamame/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/zoobzio/edamame/graph/badge.svg?branch=main)](https://codecov.io/gh/zoobzio/edamame)
[![Go Report Card](https://goreportcard.com/badge/github.com/zoobzio/edamame)](https://goreportcard.com/report/github.com/zoobzio/edamame)
[![CodeQL](https://github.com/zoobzio/edamame/workflows/CodeQL/badge.svg)](https://github.com/zoobzio/edamame/security/code-scanning)
[![Go Reference](https://pkg.go.dev/badge/github.com/zoobzio/edamame.svg)](https://pkg.go.dev/github.com/zoobzio/edamame)
[![License](https://img.shields.io/github/license/zoobzio/edamame)](LICENSE)
[![Go Version](https://img.shields.io/github/go-mod/go-version/zoobzio/edamame)](go.mod)
[![Release](https://img.shields.io/github/v/release/zoobzio/edamame)](https://github.com/zoobzio/edamame/releases)

Statement-driven query exec for Go.

Define database queries as typed statements, execute them without magic strings.

## Queries as Data

Edamame treats queries as specs—pure data structures wrapped in typed statements.

```go
// Define statements as package-level variables
var (
    QueryAll = edamame.NewQueryStatement("query-all", "Query all users", edamame.QuerySpec{})

    ByStatus = edamame.NewQueryStatement("by-status", "Query users by status", edamame.QuerySpec{
        Where:   []edamame.ConditionSpec{{Field: "status", Operator: "=", Param: "status"}},
        OrderBy: []edamame.OrderBySpec{{Field: "created_at", Direction: "desc"}},
        Limit:   ptr(50),
    })

    SelectByID = edamame.NewSelectStatement("select-by-id", "Select user by ID", edamame.SelectSpec{
        Where: []edamame.ConditionSpec{{Field: "id", Operator: "=", Param: "id"}},
    })
)

// Execute with type safety
users, _ := exec.ExecQuery(ctx, ByStatus, map[string]any{"status": "active"})
user, _ := exec.ExecSelect(ctx, SelectByID, map[string]any{"id": 123})
```

Type-safe. No magic strings. Compile-time guarantees.

## Install

```bash
go get github.com/zoobzio/edamame
```

Requires Go 1.24+. Supports PostgreSQL, MariaDB, SQLite, and SQL Server via [astql](https://github.com/zoobzio/astql).

## Quick Start

```go
package main

import (
    "context"
    "fmt"

    "github.com/jmoiron/sqlx"
    _ "github.com/lib/pq" // or mariadb, sqlite3, mssql driver
    "github.com/zoobzio/astql/pkg/postgres" // or mariadb, sqlite, mssql
    "github.com/zoobzio/edamame"
)

type User struct {
    ID     int    `db:"id" type:"integer" constraints:"primarykey"`
    Email  string `db:"email" type:"text" constraints:"notnull,unique"`
    Name   string `db:"name" type:"text"`
    Status string `db:"status" type:"text"`
}

// Define statements
var (
    QueryAll = edamame.NewQueryStatement("query-all", "Query all users", edamame.QuerySpec{})

    SelectByID = edamame.NewSelectStatement("select-by-id", "Select user by ID", edamame.SelectSpec{
        Where: []edamame.ConditionSpec{{Field: "id", Operator: "=", Param: "id"}},
    })

    CountAll = edamame.NewAggregateStatement("count-all", "Count all users", edamame.AggCount, edamame.AggregateSpec{})

    ActiveUsers = edamame.NewQueryStatement("active", "Query active users", edamame.QuerySpec{
        Where: []edamame.ConditionSpec{
            {Field: "status", Operator: "=", Param: "status"},
        },
    })
)

func main() {
    db, _ := sqlx.Connect("postgres", "postgres://localhost/mydb?sslmode=disable")
    ctx := context.Background()

    // Create exec
    exec, _ := edamame.New[User](db, "users", postgres.New())

    // Execute statements
    users, _ := exec.ExecQuery(ctx, QueryAll, nil)
    user, _ := exec.ExecSelect(ctx, SelectByID, map[string]any{"id": 1})
    count, _ := exec.ExecAggregate(ctx, CountAll, nil)
    active, _ := exec.ExecQuery(ctx, ActiveUsers, map[string]any{"status": "active"})

    fmt.Printf("%d users, user #1: %s, %.0f total, %d active\n", len(users), user.Name, count, len(active))
}
```

## Statement Types

```go
// Multi-record queries
var QueryAdults = edamame.NewQueryStatement("adults", "Find adult users", edamame.QuerySpec{
    Where: []edamame.ConditionSpec{{Field: "age", Operator: ">=", Param: "min_age"}},
    OrderBy: []edamame.OrderBySpec{{Field: "name", Direction: "asc"}},
})

// Single-record selects
var SelectByEmail = edamame.NewSelectStatement("by-email", "Select user by email", edamame.SelectSpec{
    Where: []edamame.ConditionSpec{{Field: "email", Operator: "=", Param: "email"}},
})

// Updates
var UpdateName = edamame.NewUpdateStatement("update-name", "Update user name", edamame.UpdateSpec{
    Set:   map[string]string{"name": "new_name"},
    Where: []edamame.ConditionSpec{{Field: "id", Operator: "=", Param: "id"}},
})

// Deletes
var DeleteInactive = edamame.NewDeleteStatement("delete-inactive", "Delete inactive users", edamame.DeleteSpec{
    Where: []edamame.ConditionSpec{{Field: "status", Operator: "=", Param: "status"}},
})

// Aggregates
var SumAges = edamame.NewAggregateStatement("sum-ages", "Sum all ages", edamame.AggSum, edamame.AggregateSpec{
    Field: "age",
})
```

## Why edamame?

- **Type-safe** — Generic `Executor[T]` with compile-time safety via [soy](https://github.com/zoobzio/soy)
- **No magic strings** — Typed statements, not string keys
- **Declarative** — Specs are data, statements wrap them with identity
- **Compile-time guarantees** — Pass wrong statement type? Compiler catches it
- **Thread-safe** — Concurrent execution, no shared mutable state

## Documentation

### Learn
- [Quickstart](docs/2.learn/1.quickstart.md)
- [Core Concepts](docs/2.learn/2.concepts.md)
- [Architecture](docs/2.learn/3.architecture.md)

### Guides
- [Statements](docs/3.guides/1.capabilities.md)
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
