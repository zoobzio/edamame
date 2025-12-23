package edamame

import (
	"context"
	"fmt"

	"github.com/jmoiron/sqlx"
	"github.com/zoobzio/capitan"
	"github.com/zoobzio/cereal"
)

// Query returns a cereal Query builder for the named capability.
// Returns an error if the capability doesn't exist or has an invalid spec.
func (f *Factory[T]) Query(name string) (*cereal.Query[T], error) {
	f.mu.RLock()
	queryCap, exists := f.queries[name]
	f.mu.RUnlock()

	if !exists {
		capitan.Emit(context.Background(), CapabilityNotFound,
			KeyTable.Field(f.cereal.TableName()),
			KeyCapability.Field(name),
			KeyType.Field("query"))
		return nil, fmt.Errorf("query capability %q not found", name)
	}

	q, err := f.queryFromSpec(queryCap.Spec)
	if err != nil {
		return nil, fmt.Errorf("query capability %q: %w", name, err)
	}
	return q, nil
}

// Select returns a cereal Select builder for the named capability.
// Returns an error if the capability doesn't exist or has an invalid spec.
func (f *Factory[T]) Select(name string) (*cereal.Select[T], error) {
	f.mu.RLock()
	selectCap, exists := f.selects[name]
	f.mu.RUnlock()

	if !exists {
		capitan.Emit(context.Background(), CapabilityNotFound,
			KeyTable.Field(f.cereal.TableName()),
			KeyCapability.Field(name),
			KeyType.Field("select"))
		return nil, fmt.Errorf("select capability %q not found", name)
	}

	s, err := f.selectFromSpec(selectCap.Spec)
	if err != nil {
		return nil, fmt.Errorf("select capability %q: %w", name, err)
	}
	return s, nil
}

// Update returns a cereal Update builder for the named capability.
// Returns an error if the capability doesn't exist.
func (f *Factory[T]) Update(name string) (*cereal.Update[T], error) {
	f.mu.RLock()
	updateCap, exists := f.updates[name]
	f.mu.RUnlock()

	if !exists {
		capitan.Emit(context.Background(), CapabilityNotFound,
			KeyTable.Field(f.cereal.TableName()),
			KeyCapability.Field(name),
			KeyType.Field("update"))
		return nil, fmt.Errorf("update capability %q not found", name)
	}

	return f.modifyFromSpec(updateCap.Spec), nil
}

// Delete returns a cereal Delete builder for the named capability.
// Returns an error if the capability doesn't exist.
func (f *Factory[T]) Delete(name string) (*cereal.Delete[T], error) {
	f.mu.RLock()
	deleteCap, exists := f.deletes[name]
	f.mu.RUnlock()

	if !exists {
		capitan.Emit(context.Background(), CapabilityNotFound,
			KeyTable.Field(f.cereal.TableName()),
			KeyCapability.Field(name),
			KeyType.Field("delete"))
		return nil, fmt.Errorf("delete capability %q not found", name)
	}

	return f.removeFromSpec(deleteCap.Spec), nil
}

// Aggregate returns a cereal Aggregate builder for the named capability.
// Returns an error if the capability doesn't exist.
func (f *Factory[T]) Aggregate(name string) (*cereal.Aggregate[T], error) {
	f.mu.RLock()
	aggCap, exists := f.aggregates[name]
	f.mu.RUnlock()

	if !exists {
		capitan.Emit(context.Background(), CapabilityNotFound,
			KeyTable.Field(f.cereal.TableName()),
			KeyCapability.Field(name),
			KeyType.Field("aggregate"))
		return nil, fmt.Errorf("aggregate capability %q not found", name)
	}

	// Dispatch to appropriate aggregate function
	switch aggCap.Func {
	case AggSum:
		return f.sumFromSpec(aggCap.Spec), nil
	case AggAvg:
		return f.avgFromSpec(aggCap.Spec), nil
	case AggMin:
		return f.minFromSpec(aggCap.Spec), nil
	case AggMax:
		return f.maxFromSpec(aggCap.Spec), nil
	default:
		return f.countFromSpec(aggCap.Spec), nil
	}
}

// Insert returns a cereal Create builder for inserting records.
// This uses the underlying cereal.Insert() directly since inserts
// are driven by struct fields rather than specs.
func (f *Factory[T]) Insert() *cereal.Create[T] {
	return f.cereal.Insert()
}

// Compound returns a cereal Compound builder from a CompoundQuerySpec.
// Compound queries combine multiple queries using set operations (UNION, INTERSECT, EXCEPT).
// Unlike other operations, compound queries are not registered as capabilities - they are
// constructed directly from a spec since they represent ad-hoc combinations.
func (f *Factory[T]) Compound(spec CompoundQuerySpec) (*cereal.Compound[T], error) {
	return f.compoundFromSpec(spec)
}

// ExecQuery executes a named query capability directly.
// Convenience method that combines Query() and Exec().
func (f *Factory[T]) ExecQuery(ctx context.Context, name string, params map[string]any) ([]*T, error) {
	q, err := f.Query(name)
	if err != nil {
		return nil, err
	}
	return q.Exec(ctx, params)
}

// ExecQueryTx executes a named query capability within a transaction.
func (f *Factory[T]) ExecQueryTx(ctx context.Context, tx *sqlx.Tx, name string, params map[string]any) ([]*T, error) {
	q, err := f.Query(name)
	if err != nil {
		return nil, err
	}
	return q.ExecTx(ctx, tx, params)
}

// ExecSelect executes a named select capability directly.
// Convenience method that combines Select() and Exec().
func (f *Factory[T]) ExecSelect(ctx context.Context, name string, params map[string]any) (*T, error) {
	s, err := f.Select(name)
	if err != nil {
		return nil, err
	}
	return s.Exec(ctx, params)
}

// ExecSelectTx executes a named select capability within a transaction.
func (f *Factory[T]) ExecSelectTx(ctx context.Context, tx *sqlx.Tx, name string, params map[string]any) (*T, error) {
	s, err := f.Select(name)
	if err != nil {
		return nil, err
	}
	return s.ExecTx(ctx, tx, params)
}

// ExecUpdate executes a named update capability directly.
// Convenience method that combines Update() and Exec().
func (f *Factory[T]) ExecUpdate(ctx context.Context, name string, params map[string]any) (*T, error) {
	u, err := f.Update(name)
	if err != nil {
		return nil, err
	}
	return u.Exec(ctx, params)
}

// ExecUpdateTx executes a named update capability within a transaction.
func (f *Factory[T]) ExecUpdateTx(ctx context.Context, tx *sqlx.Tx, name string, params map[string]any) (*T, error) {
	u, err := f.Update(name)
	if err != nil {
		return nil, err
	}
	return u.ExecTx(ctx, tx, params)
}

// ExecDelete executes a named delete capability directly.
// Convenience method that combines Delete() and Exec().
func (f *Factory[T]) ExecDelete(ctx context.Context, name string, params map[string]any) (int64, error) {
	d, err := f.Delete(name)
	if err != nil {
		return 0, err
	}
	return d.Exec(ctx, params)
}

// ExecDeleteTx executes a named delete capability within a transaction.
func (f *Factory[T]) ExecDeleteTx(ctx context.Context, tx *sqlx.Tx, name string, params map[string]any) (int64, error) {
	d, err := f.Delete(name)
	if err != nil {
		return 0, err
	}
	return d.ExecTx(ctx, tx, params)
}

// ExecAggregate executes a named aggregate capability directly.
// Convenience method that combines Aggregate() and Exec().
func (f *Factory[T]) ExecAggregate(ctx context.Context, name string, params map[string]any) (float64, error) {
	a, err := f.Aggregate(name)
	if err != nil {
		return 0, err
	}
	return a.Exec(ctx, params)
}

// ExecAggregateTx executes a named aggregate capability within a transaction.
func (f *Factory[T]) ExecAggregateTx(ctx context.Context, tx *sqlx.Tx, name string, params map[string]any) (float64, error) {
	a, err := f.Aggregate(name)
	if err != nil {
		return 0, err
	}
	return a.ExecTx(ctx, tx, params)
}

// ExecInsert executes an insert directly.
// Convenience method that combines Insert() and Exec().
func (f *Factory[T]) ExecInsert(ctx context.Context, record *T) (*T, error) {
	return f.Insert().Exec(ctx, record)
}

// ExecInsertTx executes an insert within a transaction.
func (f *Factory[T]) ExecInsertTx(ctx context.Context, tx *sqlx.Tx, record *T) (*T, error) {
	return f.Insert().ExecTx(ctx, tx, record)
}

// ExecInsertBatch inserts multiple records.
// Returns the count of successfully inserted records.
func (f *Factory[T]) ExecInsertBatch(ctx context.Context, records []*T) (int64, error) {
	return f.Insert().ExecBatch(ctx, records)
}

// ExecInsertBatchTx inserts multiple records within a transaction.
func (f *Factory[T]) ExecInsertBatchTx(ctx context.Context, tx *sqlx.Tx, records []*T) (int64, error) {
	return f.Insert().ExecBatchTx(ctx, tx, records)
}

// ExecCompound executes a compound query directly.
// Convenience method that combines Compound() and Exec().
func (f *Factory[T]) ExecCompound(ctx context.Context, spec CompoundQuerySpec, params map[string]any) ([]*T, error) {
	c, err := f.Compound(spec)
	if err != nil {
		return nil, err
	}
	return c.Exec(ctx, params)
}

// ExecCompoundTx executes a compound query within a transaction.
func (f *Factory[T]) ExecCompoundTx(ctx context.Context, tx *sqlx.Tx, spec CompoundQuerySpec, params map[string]any) ([]*T, error) {
	c, err := f.Compound(spec)
	if err != nil {
		return nil, err
	}
	return c.ExecTx(ctx, tx, params)
}

// ExecUpdateBatch executes a named update capability with multiple parameter sets.
// Returns the total count of affected rows.
func (f *Factory[T]) ExecUpdateBatch(ctx context.Context, name string, batchParams []map[string]any) (int64, error) {
	u, err := f.Update(name)
	if err != nil {
		return 0, err
	}
	return u.ExecBatch(ctx, batchParams)
}

// ExecUpdateBatchTx executes a named update capability with multiple parameter sets within a transaction.
func (f *Factory[T]) ExecUpdateBatchTx(ctx context.Context, tx *sqlx.Tx, name string, batchParams []map[string]any) (int64, error) {
	u, err := f.Update(name)
	if err != nil {
		return 0, err
	}
	return u.ExecBatchTx(ctx, tx, batchParams)
}

// ExecDeleteBatch executes a named delete capability with multiple parameter sets.
// Returns the total count of deleted rows.
func (f *Factory[T]) ExecDeleteBatch(ctx context.Context, name string, batchParams []map[string]any) (int64, error) {
	d, err := f.Delete(name)
	if err != nil {
		return 0, err
	}
	return d.ExecBatch(ctx, batchParams)
}

// ExecDeleteBatchTx executes a named delete capability with multiple parameter sets within a transaction.
func (f *Factory[T]) ExecDeleteBatchTx(ctx context.Context, tx *sqlx.Tx, name string, batchParams []map[string]any) (int64, error) {
	d, err := f.Delete(name)
	if err != nil {
		return 0, err
	}
	return d.ExecBatchTx(ctx, tx, batchParams)
}
