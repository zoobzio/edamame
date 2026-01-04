package edamame

import (
	"context"

	"github.com/jmoiron/sqlx"
	"github.com/zoobzio/atom"
	"github.com/zoobzio/soy"
)

// Query returns a soy Query builder for the given statement.
func (e *Executor[T]) Query(stmt QueryStatement) (*soy.Query[T], error) {
	return e.queryFromSpec(stmt.spec)
}

// Select returns a soy Select builder for the given statement.
func (e *Executor[T]) Select(stmt SelectStatement) (*soy.Select[T], error) {
	return e.selectFromSpec(stmt.spec)
}

// Update returns a soy Update builder for the given statement.
func (e *Executor[T]) Update(stmt UpdateStatement) *soy.Update[T] {
	return e.modifyFromSpec(stmt.spec)
}

// Delete returns a soy Delete builder for the given statement.
func (e *Executor[T]) Delete(stmt DeleteStatement) *soy.Delete[T] {
	return e.removeFromSpec(stmt.spec)
}

// Aggregate returns a soy Aggregate builder for the given statement.
func (e *Executor[T]) Aggregate(stmt AggregateStatement) *soy.Aggregate[T] {
	switch stmt.fn {
	case AggSum:
		return e.sumFromSpec(stmt.spec)
	case AggAvg:
		return e.avgFromSpec(stmt.spec)
	case AggMin:
		return e.minFromSpec(stmt.spec)
	case AggMax:
		return e.maxFromSpec(stmt.spec)
	default:
		return e.countFromSpec(stmt.spec)
	}
}

// Insert returns a soy Create builder for inserting records.
// This uses the underlying soy.Insert() directly since inserts
// are driven by struct fields rather than specs.
func (e *Executor[T]) Insert() *soy.Create[T] {
	return e.soy.Insert()
}

// Compound returns a soy Compound builder from a CompoundQuerySpec.
// Compound queries combine multiple queries using set operations (UNION, INTERSECT, EXCEPT).
func (e *Executor[T]) Compound(spec CompoundQuerySpec) (*soy.Compound[T], error) {
	return e.compoundFromSpec(spec)
}

// ExecQuery executes a query statement directly.
func (e *Executor[T]) ExecQuery(ctx context.Context, stmt QueryStatement, params map[string]any) ([]*T, error) {
	q, err := e.Query(stmt)
	if err != nil {
		return nil, err
	}
	return q.Exec(ctx, params)
}

// ExecQueryTx executes a query statement within a transaction.
func (e *Executor[T]) ExecQueryTx(ctx context.Context, tx *sqlx.Tx, stmt QueryStatement, params map[string]any) ([]*T, error) {
	q, err := e.Query(stmt)
	if err != nil {
		return nil, err
	}
	return q.ExecTx(ctx, tx, params)
}

// ExecSelect executes a select statement directly.
func (e *Executor[T]) ExecSelect(ctx context.Context, stmt SelectStatement, params map[string]any) (*T, error) {
	s, err := e.Select(stmt)
	if err != nil {
		return nil, err
	}
	return s.Exec(ctx, params)
}

// ExecSelectTx executes a select statement within a transaction.
func (e *Executor[T]) ExecSelectTx(ctx context.Context, tx *sqlx.Tx, stmt SelectStatement, params map[string]any) (*T, error) {
	s, err := e.Select(stmt)
	if err != nil {
		return nil, err
	}
	return s.ExecTx(ctx, tx, params)
}

// ExecUpdate executes an update statement directly.
func (e *Executor[T]) ExecUpdate(ctx context.Context, stmt UpdateStatement, params map[string]any) (*T, error) {
	u := e.Update(stmt)
	return u.Exec(ctx, params)
}

// ExecUpdateTx executes an update statement within a transaction.
func (e *Executor[T]) ExecUpdateTx(ctx context.Context, tx *sqlx.Tx, stmt UpdateStatement, params map[string]any) (*T, error) {
	u := e.Update(stmt)
	return u.ExecTx(ctx, tx, params)
}

// ExecDelete executes a delete statement directly.
func (e *Executor[T]) ExecDelete(ctx context.Context, stmt DeleteStatement, params map[string]any) (int64, error) {
	d := e.Delete(stmt)
	return d.Exec(ctx, params)
}

// ExecDeleteTx executes a delete statement within a transaction.
func (e *Executor[T]) ExecDeleteTx(ctx context.Context, tx *sqlx.Tx, stmt DeleteStatement, params map[string]any) (int64, error) {
	d := e.Delete(stmt)
	return d.ExecTx(ctx, tx, params)
}

// ExecAggregate executes an aggregate statement directly.
func (e *Executor[T]) ExecAggregate(ctx context.Context, stmt AggregateStatement, params map[string]any) (float64, error) {
	a := e.Aggregate(stmt)
	return a.Exec(ctx, params)
}

// ExecAggregateTx executes an aggregate statement within a transaction.
func (e *Executor[T]) ExecAggregateTx(ctx context.Context, tx *sqlx.Tx, stmt AggregateStatement, params map[string]any) (float64, error) {
	a := e.Aggregate(stmt)
	return a.ExecTx(ctx, tx, params)
}

// ExecInsert executes an insert directly.
func (e *Executor[T]) ExecInsert(ctx context.Context, record *T) (*T, error) {
	return e.Insert().Exec(ctx, record)
}

// ExecInsertTx executes an insert within a transaction.
func (e *Executor[T]) ExecInsertTx(ctx context.Context, tx *sqlx.Tx, record *T) (*T, error) {
	return e.Insert().ExecTx(ctx, tx, record)
}

// ExecInsertBatch inserts multiple records.
// Returns the count of successfully inserted records.
func (e *Executor[T]) ExecInsertBatch(ctx context.Context, records []*T) (int64, error) {
	return e.Insert().ExecBatch(ctx, records)
}

// ExecInsertBatchTx inserts multiple records within a transaction.
func (e *Executor[T]) ExecInsertBatchTx(ctx context.Context, tx *sqlx.Tx, records []*T) (int64, error) {
	return e.Insert().ExecBatchTx(ctx, tx, records)
}

// ExecCompound executes a compound query directly.
func (e *Executor[T]) ExecCompound(ctx context.Context, spec CompoundQuerySpec, params map[string]any) ([]*T, error) {
	c, err := e.Compound(spec)
	if err != nil {
		return nil, err
	}
	return c.Exec(ctx, params)
}

// ExecCompoundTx executes a compound query within a transaction.
func (e *Executor[T]) ExecCompoundTx(ctx context.Context, tx *sqlx.Tx, spec CompoundQuerySpec, params map[string]any) ([]*T, error) {
	c, err := e.Compound(spec)
	if err != nil {
		return nil, err
	}
	return c.ExecTx(ctx, tx, params)
}

// ExecUpdateBatch executes an update statement with multiple parameter sets.
// Returns the total count of affected rows.
func (e *Executor[T]) ExecUpdateBatch(ctx context.Context, stmt UpdateStatement, batchParams []map[string]any) (int64, error) {
	u := e.Update(stmt)
	return u.ExecBatch(ctx, batchParams)
}

// ExecUpdateBatchTx executes an update statement with multiple parameter sets within a transaction.
func (e *Executor[T]) ExecUpdateBatchTx(ctx context.Context, tx *sqlx.Tx, stmt UpdateStatement, batchParams []map[string]any) (int64, error) {
	u := e.Update(stmt)
	return u.ExecBatchTx(ctx, tx, batchParams)
}

// ExecDeleteBatch executes a delete statement with multiple parameter sets.
// Returns the total count of deleted rows.
func (e *Executor[T]) ExecDeleteBatch(ctx context.Context, stmt DeleteStatement, batchParams []map[string]any) (int64, error) {
	d := e.Delete(stmt)
	return d.ExecBatch(ctx, batchParams)
}

// ExecDeleteBatchTx executes a delete statement with multiple parameter sets within a transaction.
func (e *Executor[T]) ExecDeleteBatchTx(ctx context.Context, tx *sqlx.Tx, stmt DeleteStatement, batchParams []map[string]any) (int64, error) {
	d := e.Delete(stmt)
	return d.ExecBatchTx(ctx, tx, batchParams)
}

// ExecQueryAtom executes a query statement and returns results as Atoms.
// This enables type-erased execution where T is not known at consumption time.
func (e *Executor[T]) ExecQueryAtom(ctx context.Context, stmt QueryStatement, params map[string]any) ([]*atom.Atom, error) {
	q, err := e.Query(stmt)
	if err != nil {
		return nil, err
	}
	return q.ExecAtom(ctx, params)
}

// ExecSelectAtom executes a select statement and returns the result as an Atom.
// This enables type-erased execution where T is not known at consumption time.
func (e *Executor[T]) ExecSelectAtom(ctx context.Context, stmt SelectStatement, params map[string]any) (*atom.Atom, error) {
	s, err := e.Select(stmt)
	if err != nil {
		return nil, err
	}
	return s.ExecAtom(ctx, params)
}

// ExecInsertAtom executes an insert and returns the result as an Atom.
// This enables type-erased execution where T is not known at consumption time.
func (e *Executor[T]) ExecInsertAtom(ctx context.Context, params map[string]any) (*atom.Atom, error) {
	return e.Insert().ExecAtom(ctx, params)
}
