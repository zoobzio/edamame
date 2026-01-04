// Package edamame provides a statement-driven query executor built on soy.
//
// Edamame wraps soy to offer a declarative, type-safe API for database
// operations. Define statements as package-level variables and pass them
// directly to execution methods.
//
// # Quick Start
//
// Define your model with struct tags:
//
//	type User struct {
//	    ID    int    `db:"id" type:"integer" constraints:"primarykey"`
//	    Email string `db:"email" type:"text" constraints:"notnull,unique"`
//	    Name  string `db:"name" type:"text"`
//	    Age   *int   `db:"age" type:"integer"`
//	}
//
// Define statements:
//
//	var ByEmail = edamame.NewSelectStatement(
//	    "by-email",
//	    "Select user by email",
//	    edamame.SelectSpec{
//	        Where: []edamame.ConditionSpec{
//	            {Field: "email", Operator: "=", Param: "email"},
//	        },
//	    },
//	)
//
// Create an Executor and execute:
//
//	import "github.com/zoobzio/astql/pkg/postgres"
//
//	exec, err := edamame.New[User](db, "users", postgres.New())
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	user, err := exec.ExecSelect(ctx, ByEmail, map[string]any{"email": "user@example.com"})
package edamame

import (
	"context"
	"fmt"

	"github.com/jmoiron/sqlx"
	"github.com/zoobzio/astql"
	"github.com/zoobzio/capitan"
	"github.com/zoobzio/soy"
)

// Executor provides a statement-driven query API for a specific model type.
// It wraps soy with typed statements for compile-time safety.
type Executor[T any] struct {
	db  sqlx.ExtContext
	soy *soy.Soy[T]
}

// New creates a new Executor for type T with the given database connection, table name, and renderer.
//
// The db parameter accepts sqlx.ExtContext, which is satisfied by both *sqlx.DB and *sqlx.Tx,
// enabling transaction support by passing a transaction instead of a database connection.
func New[T any](db sqlx.ExtContext, tableName string, renderer astql.Renderer) (*Executor[T], error) {
	c, err := soy.New[T](db, tableName, renderer)
	if err != nil {
		return nil, fmt.Errorf("edamame: failed to create soy instance: %w", err)
	}

	e := &Executor[T]{
		db:  db,
		soy: c,
	}

	capitan.Emit(context.Background(), ExecutorCreated,
		KeyTable.Field(tableName))

	return e, nil
}

// Soy returns the underlying soy instance for advanced usage.
func (e *Executor[T]) Soy() *soy.Soy[T] {
	return e.soy
}

// TableName returns the table name for this executor.
func (e *Executor[T]) TableName() string {
	return e.soy.TableName()
}

// RenderQuery renders a query statement to SQL for inspection or debugging.
func (e *Executor[T]) RenderQuery(stmt QueryStatement) (string, error) {
	q, err := e.queryFromSpec(stmt.spec)
	if err != nil {
		return "", err
	}
	result, err := q.Render()
	if err != nil {
		return "", err
	}
	return result.SQL, nil
}

// RenderSelect renders a select statement to SQL for inspection or debugging.
func (e *Executor[T]) RenderSelect(stmt SelectStatement) (string, error) {
	s, err := e.selectFromSpec(stmt.spec)
	if err != nil {
		return "", err
	}
	result, err := s.Render()
	if err != nil {
		return "", err
	}
	return result.SQL, nil
}

// RenderUpdate renders an update statement to SQL for inspection or debugging.
func (e *Executor[T]) RenderUpdate(stmt UpdateStatement) (string, error) {
	u := e.modifyFromSpec(stmt.spec)
	result, err := u.Render()
	if err != nil {
		return "", err
	}
	return result.SQL, nil
}

// RenderDelete renders a delete statement to SQL for inspection or debugging.
func (e *Executor[T]) RenderDelete(stmt DeleteStatement) (string, error) {
	d := e.removeFromSpec(stmt.spec)
	result, err := d.Render()
	if err != nil {
		return "", err
	}
	return result.SQL, nil
}

// RenderAggregate renders an aggregate statement to SQL for inspection or debugging.
func (e *Executor[T]) RenderAggregate(stmt AggregateStatement) (string, error) {
	var agg *soy.Aggregate[T]
	switch stmt.fn {
	case AggSum:
		agg = e.sumFromSpec(stmt.spec)
	case AggAvg:
		agg = e.avgFromSpec(stmt.spec)
	case AggMin:
		agg = e.minFromSpec(stmt.spec)
	case AggMax:
		agg = e.maxFromSpec(stmt.spec)
	default:
		agg = e.countFromSpec(stmt.spec)
	}
	result, err := agg.Render()
	if err != nil {
		return "", err
	}
	return result.SQL, nil
}

// RenderCompound renders a compound query to SQL for inspection or debugging.
func (e *Executor[T]) RenderCompound(spec CompoundQuerySpec) (string, error) {
	c, err := e.compoundFromSpec(spec)
	if err != nil {
		return "", err
	}
	result, err := c.Render()
	if err != nil {
		return "", err
	}
	return result.SQL, nil
}
