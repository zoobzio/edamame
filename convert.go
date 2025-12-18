package edamame

import (
	"fmt"
	"strings"

	"github.com/zoobzio/cereal"
)

// Constants for conflict actions and row locking modes.
const (
	conflictActionUpdate  = "update"
	conflictActionNothing = "nothing"
	lockModeUpdate        = "update"
	lockModeNoKeyUpdate   = "no_key_update"
	lockModeShare         = "share"
	lockModeKeyShare      = "key_share"
	logicOR               = "OR"
	opIsNull              = "IS NULL"
	opIsNotNull           = "IS NOT NULL"
)

// toCondition converts a simple ConditionSpec to a cereal.Condition.
func (c ConditionSpec) toCondition() cereal.Condition {
	if c.IsNull {
		if c.Operator == opIsNotNull {
			return cereal.NotNull(c.Field)
		}
		return cereal.Null(c.Field)
	}
	return cereal.C(c.Field, c.Operator, c.Param)
}

// toConditions converts a slice of ConditionSpecs to cereal.Conditions.
// This flattens simple conditions from groups for use with WhereAnd/WhereOr.
func toConditions(specs []ConditionSpec) []cereal.Condition {
	conditions := make([]cereal.Condition, 0, len(specs))
	for _, spec := range specs {
		if !spec.IsGroup() {
			conditions = append(conditions, spec.toCondition())
		}
	}
	return conditions
}

// queryFromSpec builds a cereal.Query from a QuerySpec.
// Returns an error if the spec contains invalid values.
func (f *Factory[T]) queryFromSpec(spec QuerySpec) (*cereal.Query[T], error) {
	q := f.cereal.Query()

	// Add fields if specified
	if len(spec.Fields) > 0 {
		q = q.Fields(spec.Fields...)
	}

	// Add WHERE conditions
	for _, cond := range spec.Where {
		q = applyConditionToQuery(q, cond)
	}

	// Add ORDER BY clauses
	for _, orderBy := range spec.OrderBy {
		switch {
		case orderBy.IsExpression():
			q = q.OrderByExpr(orderBy.Field, orderBy.Operator, orderBy.Param, orderBy.Direction)
		case orderBy.HasNulls():
			q = q.OrderByNulls(orderBy.Field, orderBy.Direction, orderBy.Nulls)
		default:
			q = q.OrderBy(orderBy.Field, orderBy.Direction)
		}
	}

	// Add GROUP BY if specified
	if len(spec.GroupBy) > 0 {
		q = q.GroupBy(spec.GroupBy...)
	}

	// Add HAVING conditions (simple field-based)
	for _, cond := range spec.Having {
		if !cond.IsGroup() {
			q = q.Having(cond.Field, cond.Operator, cond.Param)
		}
	}

	// Add HAVING aggregate conditions
	for _, agg := range spec.HavingAgg {
		q = q.HavingAgg(agg.Func, agg.Field, agg.Operator, agg.Param)
	}

	// Add LIMIT if specified
	if spec.Limit != nil {
		q = q.Limit(*spec.Limit)
	}

	// Add OFFSET if specified
	if spec.Offset != nil {
		q = q.Offset(*spec.Offset)
	}

	// Add DISTINCT if specified
	if spec.Distinct {
		q = q.Distinct()
	}

	// Add DISTINCT ON if specified (PostgreSQL)
	if len(spec.DistinctOn) > 0 {
		q = q.DistinctOn(spec.DistinctOn...)
	}

	// Add row locking if specified
	q, err := applyForLocking(q, spec.ForLocking)
	if err != nil {
		return nil, err
	}

	return q, nil
}

// applyConditionToQuery applies a ConditionSpec to a Query builder.
// Handles both simple conditions and condition groups (AND/OR).
func applyConditionToQuery[T any](q *cereal.Query[T], cond ConditionSpec) *cereal.Query[T] {
	if cond.IsGroup() {
		conditions := toConditions(cond.Group)
		if strings.EqualFold(cond.Logic, logicOR) {
			return q.WhereOr(conditions...)
		}
		return q.WhereAnd(conditions...)
	}

	// Simple condition
	if cond.IsNull {
		if cond.Operator == opIsNull {
			return q.WhereNull(cond.Field)
		}
		return q.WhereNotNull(cond.Field)
	}
	return q.Where(cond.Field, cond.Operator, cond.Param)
}

// applyForLocking applies row locking to a Query based on the spec.
// Returns an error if an invalid lock mode is specified.
func applyForLocking[T any](q *cereal.Query[T], forLocking string) (*cereal.Query[T], error) {
	if forLocking == "" {
		return q, nil
	}
	switch strings.ToLower(forLocking) {
	case lockModeUpdate:
		return q.ForUpdate(), nil
	case lockModeNoKeyUpdate:
		return q.ForNoKeyUpdate(), nil
	case lockModeShare:
		return q.ForShare(), nil
	case lockModeKeyShare:
		return q.ForKeyShare(), nil
	default:
		return nil, fmt.Errorf("invalid lock mode %q: must be one of update, no_key_update, share, key_share", forLocking)
	}
}

// selectFromSpec builds a cereal.Select from a SelectSpec.
// Returns an error if the spec contains invalid values.
func (f *Factory[T]) selectFromSpec(spec SelectSpec) (*cereal.Select[T], error) {
	s := f.cereal.Select()

	// Add fields if specified
	if len(spec.Fields) > 0 {
		s = s.Fields(spec.Fields...)
	}

	// Add WHERE conditions
	for _, cond := range spec.Where {
		s = applyConditionToSelect(s, cond)
	}

	// Add ORDER BY clauses
	for _, orderBy := range spec.OrderBy {
		switch {
		case orderBy.IsExpression():
			s = s.OrderByExpr(orderBy.Field, orderBy.Operator, orderBy.Param, orderBy.Direction)
		case orderBy.HasNulls():
			s = s.OrderByNulls(orderBy.Field, orderBy.Direction, orderBy.Nulls)
		default:
			s = s.OrderBy(orderBy.Field, orderBy.Direction)
		}
	}

	// Add GROUP BY if specified
	if len(spec.GroupBy) > 0 {
		s = s.GroupBy(spec.GroupBy...)
	}

	// Add HAVING conditions (simple field-based)
	for _, cond := range spec.Having {
		if !cond.IsGroup() {
			s = s.Having(cond.Field, cond.Operator, cond.Param)
		}
	}

	// Add HAVING aggregate conditions
	for _, agg := range spec.HavingAgg {
		s = s.HavingAgg(agg.Func, agg.Field, agg.Operator, agg.Param)
	}

	// Add LIMIT if specified
	if spec.Limit != nil {
		s = s.Limit(*spec.Limit)
	}

	// Add OFFSET if specified
	if spec.Offset != nil {
		s = s.Offset(*spec.Offset)
	}

	// Add DISTINCT if specified
	if spec.Distinct {
		s = s.Distinct()
	}

	// Add DISTINCT ON if specified (PostgreSQL)
	if len(spec.DistinctOn) > 0 {
		s = s.DistinctOn(spec.DistinctOn...)
	}

	// Add row locking if specified
	s, err := applyForLockingToSelect(s, spec.ForLocking)
	if err != nil {
		return nil, err
	}

	return s, nil
}

// applyConditionToSelect applies a ConditionSpec to a Select builder.
// Handles both simple conditions and condition groups (AND/OR).
func applyConditionToSelect[T any](s *cereal.Select[T], cond ConditionSpec) *cereal.Select[T] {
	if cond.IsGroup() {
		conditions := toConditions(cond.Group)
		if strings.EqualFold(cond.Logic, logicOR) {
			return s.WhereOr(conditions...)
		}
		return s.WhereAnd(conditions...)
	}

	// Simple condition
	if cond.IsNull {
		if cond.Operator == opIsNull {
			return s.WhereNull(cond.Field)
		}
		return s.WhereNotNull(cond.Field)
	}
	return s.Where(cond.Field, cond.Operator, cond.Param)
}

// applyForLockingToSelect applies row locking to a Select based on the spec.
// Returns an error if an invalid lock mode is specified.
func applyForLockingToSelect[T any](s *cereal.Select[T], forLocking string) (*cereal.Select[T], error) {
	if forLocking == "" {
		return s, nil
	}
	switch strings.ToLower(forLocking) {
	case lockModeUpdate:
		return s.ForUpdate(), nil
	case lockModeNoKeyUpdate:
		return s.ForNoKeyUpdate(), nil
	case lockModeShare:
		return s.ForShare(), nil
	case lockModeKeyShare:
		return s.ForKeyShare(), nil
	default:
		return nil, fmt.Errorf("invalid lock mode %q: must be one of update, no_key_update, share, key_share", forLocking)
	}
}

// modifyFromSpec builds a cereal.Update from an UpdateSpec.
func (f *Factory[T]) modifyFromSpec(spec UpdateSpec) *cereal.Update[T] {
	u := f.cereal.Modify()

	// Add SET clauses
	for field, param := range spec.Set {
		u = u.Set(field, param)
	}

	// Add WHERE conditions
	for _, cond := range spec.Where {
		u = applyConditionToUpdate(u, cond)
	}

	return u
}

// applyConditionToUpdate applies a ConditionSpec to an Update builder.
// Handles both simple conditions and condition groups (AND/OR).
func applyConditionToUpdate[T any](u *cereal.Update[T], cond ConditionSpec) *cereal.Update[T] {
	if cond.IsGroup() {
		conditions := toConditions(cond.Group)
		if strings.EqualFold(cond.Logic, logicOR) {
			return u.WhereOr(conditions...)
		}
		return u.WhereAnd(conditions...)
	}

	// Simple condition
	if cond.IsNull {
		if cond.Operator == opIsNull {
			return u.WhereNull(cond.Field)
		}
		return u.WhereNotNull(cond.Field)
	}
	return u.Where(cond.Field, cond.Operator, cond.Param)
}

// removeFromSpec builds a cereal.Delete from a DeleteSpec.
func (f *Factory[T]) removeFromSpec(spec DeleteSpec) *cereal.Delete[T] {
	d := f.cereal.Remove()

	// Add WHERE conditions
	for _, cond := range spec.Where {
		d = applyConditionToDelete(d, cond)
	}

	return d
}

// applyConditionToDelete applies a ConditionSpec to a Delete builder.
// Handles both simple conditions and condition groups (AND/OR).
func applyConditionToDelete[T any](d *cereal.Delete[T], cond ConditionSpec) *cereal.Delete[T] {
	if cond.IsGroup() {
		conditions := toConditions(cond.Group)
		if strings.EqualFold(cond.Logic, logicOR) {
			return d.WhereOr(conditions...)
		}
		return d.WhereAnd(conditions...)
	}

	// Simple condition
	if cond.IsNull {
		if cond.Operator == opIsNull {
			return d.WhereNull(cond.Field)
		}
		return d.WhereNotNull(cond.Field)
	}
	return d.Where(cond.Field, cond.Operator, cond.Param)
}

// countFromSpec builds a cereal.Aggregate (COUNT) from an AggregateSpec.
func (f *Factory[T]) countFromSpec(spec AggregateSpec) *cereal.Aggregate[T] {
	agg := f.cereal.Count()

	// Add WHERE conditions
	for _, cond := range spec.Where {
		agg = applyConditionToAggregate(agg, cond)
	}

	return agg
}

// sumFromSpec builds a cereal.Aggregate (SUM) from an AggregateSpec.
func (f *Factory[T]) sumFromSpec(spec AggregateSpec) *cereal.Aggregate[T] {
	agg := f.cereal.Sum(spec.Field)

	// Add WHERE conditions
	for _, cond := range spec.Where {
		agg = applyConditionToAggregate(agg, cond)
	}

	return agg
}

// avgFromSpec builds a cereal.Aggregate (AVG) from an AggregateSpec.
func (f *Factory[T]) avgFromSpec(spec AggregateSpec) *cereal.Aggregate[T] {
	agg := f.cereal.Avg(spec.Field)

	// Add WHERE conditions
	for _, cond := range spec.Where {
		agg = applyConditionToAggregate(agg, cond)
	}

	return agg
}

// minFromSpec builds a cereal.Aggregate (MIN) from an AggregateSpec.
func (f *Factory[T]) minFromSpec(spec AggregateSpec) *cereal.Aggregate[T] {
	agg := f.cereal.Min(spec.Field)

	// Add WHERE conditions
	for _, cond := range spec.Where {
		agg = applyConditionToAggregate(agg, cond)
	}

	return agg
}

// maxFromSpec builds a cereal.Aggregate (MAX) from an AggregateSpec.
func (f *Factory[T]) maxFromSpec(spec AggregateSpec) *cereal.Aggregate[T] {
	agg := f.cereal.Max(spec.Field)

	// Add WHERE conditions
	for _, cond := range spec.Where {
		agg = applyConditionToAggregate(agg, cond)
	}

	return agg
}

// applyConditionToAggregate applies a ConditionSpec to an Aggregate builder.
// Handles both simple conditions and condition groups (AND/OR).
func applyConditionToAggregate[T any](agg *cereal.Aggregate[T], cond ConditionSpec) *cereal.Aggregate[T] {
	if cond.IsGroup() {
		conditions := toConditions(cond.Group)
		if strings.EqualFold(cond.Logic, logicOR) {
			return agg.WhereOr(conditions...)
		}
		return agg.WhereAnd(conditions...)
	}

	// Simple condition
	if cond.IsNull {
		if cond.Operator == opIsNull {
			return agg.WhereNull(cond.Field)
		}
		return agg.WhereNotNull(cond.Field)
	}
	return agg.Where(cond.Field, cond.Operator, cond.Param)
}

// insertFromSpec builds a cereal.Create from a CreateSpec.
// Returns an error if an invalid conflict action is specified.
func (f *Factory[T]) insertFromSpec(spec CreateSpec) (*cereal.Create[T], error) {
	create := f.cereal.Insert()

	// If no conflict handling, return as-is
	if len(spec.OnConflict) == 0 {
		return create, nil
	}

	// Add ON CONFLICT
	conflict := create.OnConflict(spec.OnConflict...)

	// Apply conflict action
	switch strings.ToLower(spec.ConflictAction) {
	case "":
		return nil, fmt.Errorf("conflict action required when on_conflict columns specified: must be one of nothing, update")
	case conflictActionNothing:
		return conflict.DoNothing(), nil
	case conflictActionUpdate:
		update := conflict.DoUpdate()
		for field, param := range spec.ConflictSet {
			update = update.Set(field, param)
		}
		return update.Build(), nil
	default:
		return nil, fmt.Errorf("invalid conflict action %q: must be one of nothing, update", spec.ConflictAction)
	}
}

// compoundFromSpec builds a cereal.Compound from a CompoundQuerySpec.
func (f *Factory[T]) compoundFromSpec(spec CompoundQuerySpec) (*cereal.Compound[T], error) {
	// Build base query
	base, err := f.queryFromSpec(spec.Base)
	if err != nil {
		return nil, fmt.Errorf("base query: %w", err)
	}

	// If no operands, return error
	if len(spec.Operands) == 0 {
		return nil, fmt.Errorf("compound query requires at least one operand")
	}

	// Build first operand to create compound
	firstOperand := spec.Operands[0]
	firstQuery, err := f.queryFromSpec(firstOperand.Query)
	if err != nil {
		return nil, fmt.Errorf("operand 0: %w", err)
	}

	var compound *cereal.Compound[T]
	switch strings.ToLower(firstOperand.Operation) {
	case "union":
		compound = base.Union(firstQuery)
	case "union_all":
		compound = base.UnionAll(firstQuery)
	case "intersect":
		compound = base.Intersect(firstQuery)
	case "intersect_all":
		compound = base.IntersectAll(firstQuery)
	case "except":
		compound = base.Except(firstQuery)
	case "except_all":
		compound = base.ExceptAll(firstQuery)
	default:
		return nil, fmt.Errorf("invalid set operation %q, must be one of: union, union_all, intersect, intersect_all, except, except_all", firstOperand.Operation)
	}

	// Add remaining operands
	for i := 1; i < len(spec.Operands); i++ {
		operand := spec.Operands[i]
		query, err := f.queryFromSpec(operand.Query)
		if err != nil {
			return nil, fmt.Errorf("operand %d: %w", i, err)
		}

		switch strings.ToLower(operand.Operation) {
		case "union":
			compound = compound.Union(query)
		case "union_all":
			compound = compound.UnionAll(query)
		case "intersect":
			compound = compound.Intersect(query)
		case "intersect_all":
			compound = compound.IntersectAll(query)
		case "except":
			compound = compound.Except(query)
		case "except_all":
			compound = compound.ExceptAll(query)
		default:
			return nil, fmt.Errorf("invalid set operation %q at index %d", operand.Operation, i)
		}
	}

	// Add ORDER BY clauses
	for _, orderBy := range spec.OrderBy {
		compound = compound.OrderBy(orderBy.Field, orderBy.Direction)
	}

	// Add LIMIT if specified
	if spec.Limit != nil {
		compound = compound.Limit(*spec.Limit)
	}

	// Add OFFSET if specified
	if spec.Offset != nil {
		compound = compound.Offset(*spec.Offset)
	}

	return compound, nil
}
