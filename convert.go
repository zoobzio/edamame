package edamame

import (
	"fmt"
	"strings"

	"github.com/zoobzio/soy"
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
	selectExprCount       = "count"
)

// toCondition converts a simple ConditionSpec to a soy.Condition.
func (c ConditionSpec) toCondition() soy.Condition {
	if c.IsNull {
		if c.Operator == opIsNotNull {
			return soy.NotNull(c.Field)
		}
		return soy.Null(c.Field)
	}
	return soy.C(c.Field, c.Operator, c.Param)
}

// toConditions converts a slice of ConditionSpecs to soy.Conditions.
// This flattens simple conditions from groups for use with WhereAnd/WhereOr.
func toConditions(specs []ConditionSpec) []soy.Condition {
	conditions := make([]soy.Condition, 0, len(specs))
	for i := range specs {
		if !specs[i].IsGroup() {
			conditions = append(conditions, specs[i].toCondition())
		}
	}
	return conditions
}

// queryFromSpec builds a soy.Query from a QuerySpec.
// Returns an error if the spec contains invalid values.
func (f *Factory[T]) queryFromSpec(spec QuerySpec) (*soy.Query[T], error) {
	q := f.soy.Query()

	// Add fields if specified
	if len(spec.Fields) > 0 {
		q = q.Fields(spec.Fields...)
	}

	// Add select expressions if specified
	for i := range spec.SelectExprs {
		q = applySelectExprToQuery(q, spec.SelectExprs[i])
	}

	// Add WHERE conditions
	for i := range spec.Where {
		q = applyConditionToQuery(q, spec.Where[i])
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
	for i := range spec.Having {
		if !spec.Having[i].IsGroup() {
			q = q.Having(spec.Having[i].Field, spec.Having[i].Operator, spec.Having[i].Param)
		}
	}

	// Add HAVING aggregate conditions
	for _, agg := range spec.HavingAgg {
		q = q.HavingAgg(agg.Func, agg.Field, agg.Operator, agg.Param)
	}

	// Add LIMIT (parameterized takes precedence)
	if spec.LimitParam != "" {
		q = q.LimitParam(spec.LimitParam)
	} else if spec.Limit != nil {
		q = q.Limit(*spec.Limit)
	}

	// Add OFFSET (parameterized takes precedence)
	if spec.OffsetParam != "" {
		q = q.OffsetParam(spec.OffsetParam)
	} else if spec.Offset != nil {
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
// Handles simple conditions, condition groups (AND/OR), BETWEEN, and field comparisons.
func applyConditionToQuery[T any](q *soy.Query[T], cond ConditionSpec) *soy.Query[T] {
	if cond.IsGroup() {
		conditions := toConditions(cond.Group)
		if strings.EqualFold(cond.Logic, logicOR) {
			return q.WhereOr(conditions...)
		}
		return q.WhereAnd(conditions...)
	}

	// BETWEEN conditions
	if cond.IsBetween() {
		return q.WhereBetween(cond.Field, cond.LowParam, cond.HighParam)
	}
	if cond.IsNotBetween() {
		return q.WhereNotBetween(cond.Field, cond.LowParam, cond.HighParam)
	}

	// Field-to-field comparison
	if cond.IsFieldComparison() {
		return q.WhereFields(cond.Field, cond.Operator, cond.RightField)
	}

	// NULL conditions
	if cond.IsNull {
		if cond.Operator == opIsNull {
			return q.WhereNull(cond.Field)
		}
		return q.WhereNotNull(cond.Field)
	}

	// Simple field-operator-param condition
	return q.Where(cond.Field, cond.Operator, cond.Param)
}

// applySelectExprToQuery applies a SelectExprSpec to a Query builder.
// Handles string, math, date, aggregate, and conditional functions.
// nolint:dupl // Intentionally similar to applySelectExprToSelect - they operate on different builder types without common interface.
func applySelectExprToQuery[T any](q *soy.Query[T], expr SelectExprSpec) *soy.Query[T] {
	switch strings.ToLower(expr.Func) {
	// String functions
	case "upper":
		return q.SelectUpper(expr.Field, expr.Alias)
	case "lower":
		return q.SelectLower(expr.Field, expr.Alias)
	case "length":
		return q.SelectLength(expr.Field, expr.Alias)
	case "trim":
		return q.SelectTrim(expr.Field, expr.Alias)
	case "ltrim":
		return q.SelectLTrim(expr.Field, expr.Alias)
	case "rtrim":
		return q.SelectRTrim(expr.Field, expr.Alias)
	case "substring":
		if len(expr.Params) >= 2 {
			return q.SelectSubstring(expr.Field, expr.Params[0], expr.Params[1], expr.Alias)
		}
	case "replace":
		if len(expr.Params) >= 2 {
			return q.SelectReplace(expr.Field, expr.Params[0], expr.Params[1], expr.Alias)
		}
	case "concat":
		return q.SelectConcat(expr.Alias, expr.Fields...)

	// Math functions
	case "abs":
		return q.SelectAbs(expr.Field, expr.Alias)
	case "ceil":
		return q.SelectCeil(expr.Field, expr.Alias)
	case "floor":
		return q.SelectFloor(expr.Field, expr.Alias)
	case "round":
		return q.SelectRound(expr.Field, expr.Alias)
	case "sqrt":
		return q.SelectSqrt(expr.Field, expr.Alias)
	case "power":
		if len(expr.Params) >= 1 {
			return q.SelectPower(expr.Field, expr.Params[0], expr.Alias)
		}

	// Date/Time functions
	case "now":
		return q.SelectNow(expr.Alias)
	case "current_date":
		return q.SelectCurrentDate(expr.Alias)
	case "current_time":
		return q.SelectCurrentTime(expr.Alias)
	case "current_timestamp":
		return q.SelectCurrentTimestamp(expr.Alias)

	// Type casting
	case "cast":
		return q.SelectCast(expr.Field, soy.CastType(expr.CastType), expr.Alias)

	// Aggregate functions (inline in SELECT)
	case "count_star":
		return q.SelectCountStar(expr.Alias)
	case selectExprCount:
		if expr.Filter != nil {
			return q.SelectCountFilter(expr.Field, expr.Filter.Field, expr.Filter.Operator, expr.Filter.Param, expr.Alias)
		}
		return q.SelectCount(expr.Field, expr.Alias)
	case "count_distinct":
		if expr.Filter != nil {
			return q.SelectCountDistinctFilter(expr.Field, expr.Filter.Field, expr.Filter.Operator, expr.Filter.Param, expr.Alias)
		}
		return q.SelectCountDistinct(expr.Field, expr.Alias)
	case "sum":
		if expr.Filter != nil {
			return q.SelectSumFilter(expr.Field, expr.Filter.Field, expr.Filter.Operator, expr.Filter.Param, expr.Alias)
		}
		return q.SelectSum(expr.Field, expr.Alias)
	case "avg":
		if expr.Filter != nil {
			return q.SelectAvgFilter(expr.Field, expr.Filter.Field, expr.Filter.Operator, expr.Filter.Param, expr.Alias)
		}
		return q.SelectAvg(expr.Field, expr.Alias)
	case "min":
		if expr.Filter != nil {
			return q.SelectMinFilter(expr.Field, expr.Filter.Field, expr.Filter.Operator, expr.Filter.Param, expr.Alias)
		}
		return q.SelectMin(expr.Field, expr.Alias)
	case "max":
		if expr.Filter != nil {
			return q.SelectMaxFilter(expr.Field, expr.Filter.Field, expr.Filter.Operator, expr.Filter.Param, expr.Alias)
		}
		return q.SelectMax(expr.Field, expr.Alias)

	// Conditional functions
	case "coalesce":
		return q.SelectCoalesce(expr.Alias, expr.Params...)
	case "nullif":
		if len(expr.Params) >= 2 {
			return q.SelectNullIf(expr.Params[0], expr.Params[1], expr.Alias)
		}
	}

	// Unknown function - return unchanged
	return q
}

// applyForLocking applies row locking to a Query based on the spec.
// Returns an error if an invalid lock mode is specified.
func applyForLocking[T any](q *soy.Query[T], forLocking string) (*soy.Query[T], error) {
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

// selectFromSpec builds a soy.Select from a SelectSpec.
// Returns an error if the spec contains invalid values.
func (f *Factory[T]) selectFromSpec(spec SelectSpec) (*soy.Select[T], error) {
	s := f.soy.Select()

	// Add fields if specified
	if len(spec.Fields) > 0 {
		s = s.Fields(spec.Fields...)
	}

	// Add select expressions if specified
	for i := range spec.SelectExprs {
		s = applySelectExprToSelect(s, spec.SelectExprs[i])
	}

	// Add WHERE conditions
	for i := range spec.Where {
		s = applyConditionToSelect(s, spec.Where[i])
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
	for i := range spec.Having {
		if !spec.Having[i].IsGroup() {
			s = s.Having(spec.Having[i].Field, spec.Having[i].Operator, spec.Having[i].Param)
		}
	}

	// Add HAVING aggregate conditions
	for i := range spec.HavingAgg {
		s = s.HavingAgg(spec.HavingAgg[i].Func, spec.HavingAgg[i].Field, spec.HavingAgg[i].Operator, spec.HavingAgg[i].Param)
	}

	// Add LIMIT (parameterized takes precedence)
	if spec.LimitParam != "" {
		s = s.LimitParam(spec.LimitParam)
	} else if spec.Limit != nil {
		s = s.Limit(*spec.Limit)
	}

	// Add OFFSET (parameterized takes precedence)
	if spec.OffsetParam != "" {
		s = s.OffsetParam(spec.OffsetParam)
	} else if spec.Offset != nil {
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
// Handles simple conditions, condition groups (AND/OR), BETWEEN, and field comparisons.
func applyConditionToSelect[T any](s *soy.Select[T], cond ConditionSpec) *soy.Select[T] {
	if cond.IsGroup() {
		conditions := toConditions(cond.Group)
		if strings.EqualFold(cond.Logic, logicOR) {
			return s.WhereOr(conditions...)
		}
		return s.WhereAnd(conditions...)
	}

	// BETWEEN conditions
	if cond.IsBetween() {
		return s.WhereBetween(cond.Field, cond.LowParam, cond.HighParam)
	}
	if cond.IsNotBetween() {
		return s.WhereNotBetween(cond.Field, cond.LowParam, cond.HighParam)
	}

	// Field-to-field comparison
	if cond.IsFieldComparison() {
		return s.WhereFields(cond.Field, cond.Operator, cond.RightField)
	}

	// NULL conditions
	if cond.IsNull {
		if cond.Operator == opIsNull {
			return s.WhereNull(cond.Field)
		}
		return s.WhereNotNull(cond.Field)
	}

	// Simple field-operator-param condition
	return s.Where(cond.Field, cond.Operator, cond.Param)
}

// applySelectExprToSelect applies a SelectExprSpec to a Select builder.
// Handles string, math, date, aggregate, and conditional functions.
// nolint:dupl // Intentionally similar to applySelectExprToQuery - they operate on different builder types without common interface.
func applySelectExprToSelect[T any](s *soy.Select[T], expr SelectExprSpec) *soy.Select[T] {
	switch strings.ToLower(expr.Func) {
	// String functions
	case "upper":
		return s.SelectUpper(expr.Field, expr.Alias)
	case "lower":
		return s.SelectLower(expr.Field, expr.Alias)
	case "length":
		return s.SelectLength(expr.Field, expr.Alias)
	case "trim":
		return s.SelectTrim(expr.Field, expr.Alias)
	case "ltrim":
		return s.SelectLTrim(expr.Field, expr.Alias)
	case "rtrim":
		return s.SelectRTrim(expr.Field, expr.Alias)
	case "substring":
		if len(expr.Params) >= 2 {
			return s.SelectSubstring(expr.Field, expr.Params[0], expr.Params[1], expr.Alias)
		}
	case "replace":
		if len(expr.Params) >= 2 {
			return s.SelectReplace(expr.Field, expr.Params[0], expr.Params[1], expr.Alias)
		}
	case "concat":
		return s.SelectConcat(expr.Alias, expr.Fields...)

	// Math functions
	case "abs":
		return s.SelectAbs(expr.Field, expr.Alias)
	case "ceil":
		return s.SelectCeil(expr.Field, expr.Alias)
	case "floor":
		return s.SelectFloor(expr.Field, expr.Alias)
	case "round":
		return s.SelectRound(expr.Field, expr.Alias)
	case "sqrt":
		return s.SelectSqrt(expr.Field, expr.Alias)
	case "power":
		if len(expr.Params) >= 1 {
			return s.SelectPower(expr.Field, expr.Params[0], expr.Alias)
		}

	// Date/Time functions
	case "now":
		return s.SelectNow(expr.Alias)
	case "current_date":
		return s.SelectCurrentDate(expr.Alias)
	case "current_time":
		return s.SelectCurrentTime(expr.Alias)
	case "current_timestamp":
		return s.SelectCurrentTimestamp(expr.Alias)

	// Type casting
	case "cast":
		return s.SelectCast(expr.Field, soy.CastType(expr.CastType), expr.Alias)

	// Aggregate functions (inline in SELECT)
	case "count_star":
		return s.SelectCountStar(expr.Alias)
	case selectExprCount:
		if expr.Filter != nil {
			return s.SelectCountFilter(expr.Field, expr.Filter.Field, expr.Filter.Operator, expr.Filter.Param, expr.Alias)
		}
		return s.SelectCount(expr.Field, expr.Alias)
	case "count_distinct":
		if expr.Filter != nil {
			return s.SelectCountDistinctFilter(expr.Field, expr.Filter.Field, expr.Filter.Operator, expr.Filter.Param, expr.Alias)
		}
		return s.SelectCountDistinct(expr.Field, expr.Alias)
	case "sum":
		if expr.Filter != nil {
			return s.SelectSumFilter(expr.Field, expr.Filter.Field, expr.Filter.Operator, expr.Filter.Param, expr.Alias)
		}
		return s.SelectSum(expr.Field, expr.Alias)
	case "avg":
		if expr.Filter != nil {
			return s.SelectAvgFilter(expr.Field, expr.Filter.Field, expr.Filter.Operator, expr.Filter.Param, expr.Alias)
		}
		return s.SelectAvg(expr.Field, expr.Alias)
	case "min":
		if expr.Filter != nil {
			return s.SelectMinFilter(expr.Field, expr.Filter.Field, expr.Filter.Operator, expr.Filter.Param, expr.Alias)
		}
		return s.SelectMin(expr.Field, expr.Alias)
	case "max":
		if expr.Filter != nil {
			return s.SelectMaxFilter(expr.Field, expr.Filter.Field, expr.Filter.Operator, expr.Filter.Param, expr.Alias)
		}
		return s.SelectMax(expr.Field, expr.Alias)

	// Conditional functions
	case "coalesce":
		return s.SelectCoalesce(expr.Alias, expr.Params...)
	case "nullif":
		if len(expr.Params) >= 2 {
			return s.SelectNullIf(expr.Params[0], expr.Params[1], expr.Alias)
		}
	}

	// Unknown function - return unchanged
	return s
}

// applyForLockingToSelect applies row locking to a Select based on the spec.
// Returns an error if an invalid lock mode is specified.
func applyForLockingToSelect[T any](s *soy.Select[T], forLocking string) (*soy.Select[T], error) {
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

// modifyFromSpec builds a soy.Update from an UpdateSpec.
func (f *Factory[T]) modifyFromSpec(spec UpdateSpec) *soy.Update[T] {
	u := f.soy.Modify()

	// Add SET clauses
	for field, param := range spec.Set {
		u = u.Set(field, param)
	}

	// Add WHERE conditions
	for i := range spec.Where {
		u = applyConditionToUpdate(u, spec.Where[i])
	}

	return u
}

// applyConditionToUpdate applies a ConditionSpec to an Update builder.
// Handles simple conditions, condition groups (AND/OR), and BETWEEN.
// Note: WhereFields is not supported for Update operations.
func applyConditionToUpdate[T any](u *soy.Update[T], cond ConditionSpec) *soy.Update[T] {
	if cond.IsGroup() {
		conditions := toConditions(cond.Group)
		if strings.EqualFold(cond.Logic, logicOR) {
			return u.WhereOr(conditions...)
		}
		return u.WhereAnd(conditions...)
	}

	// BETWEEN conditions
	if cond.IsBetween() {
		return u.WhereBetween(cond.Field, cond.LowParam, cond.HighParam)
	}
	if cond.IsNotBetween() {
		return u.WhereNotBetween(cond.Field, cond.LowParam, cond.HighParam)
	}

	// NULL conditions
	if cond.IsNull {
		if cond.Operator == opIsNull {
			return u.WhereNull(cond.Field)
		}
		return u.WhereNotNull(cond.Field)
	}

	// Simple field-operator-param condition
	return u.Where(cond.Field, cond.Operator, cond.Param)
}

// removeFromSpec builds a soy.Delete from a DeleteSpec.
func (f *Factory[T]) removeFromSpec(spec DeleteSpec) *soy.Delete[T] {
	d := f.soy.Remove()

	// Add WHERE conditions
	for i := range spec.Where {
		d = applyConditionToDelete(d, spec.Where[i])
	}

	return d
}

// applyConditionToDelete applies a ConditionSpec to a Delete builder.
// Handles simple conditions, condition groups (AND/OR), BETWEEN, and field comparisons.
func applyConditionToDelete[T any](d *soy.Delete[T], cond ConditionSpec) *soy.Delete[T] {
	if cond.IsGroup() {
		conditions := toConditions(cond.Group)
		if strings.EqualFold(cond.Logic, logicOR) {
			return d.WhereOr(conditions...)
		}
		return d.WhereAnd(conditions...)
	}

	// BETWEEN conditions
	if cond.IsBetween() {
		return d.WhereBetween(cond.Field, cond.LowParam, cond.HighParam)
	}
	if cond.IsNotBetween() {
		return d.WhereNotBetween(cond.Field, cond.LowParam, cond.HighParam)
	}

	// Field-to-field comparison
	if cond.IsFieldComparison() {
		return d.WhereFields(cond.Field, cond.Operator, cond.RightField)
	}

	// NULL conditions
	if cond.IsNull {
		if cond.Operator == opIsNull {
			return d.WhereNull(cond.Field)
		}
		return d.WhereNotNull(cond.Field)
	}

	// Simple field-operator-param condition
	return d.Where(cond.Field, cond.Operator, cond.Param)
}

// countFromSpec builds a soy.Aggregate (COUNT) from an AggregateSpec.
func (f *Factory[T]) countFromSpec(spec AggregateSpec) *soy.Aggregate[T] {
	agg := f.soy.Count()

	// Add WHERE conditions
	for i := range spec.Where {
		agg = applyConditionToAggregate(agg, spec.Where[i])
	}

	return agg
}

// sumFromSpec builds a soy.Aggregate (SUM) from an AggregateSpec.
func (f *Factory[T]) sumFromSpec(spec AggregateSpec) *soy.Aggregate[T] {
	agg := f.soy.Sum(spec.Field)

	// Add WHERE conditions
	for i := range spec.Where {
		agg = applyConditionToAggregate(agg, spec.Where[i])
	}

	return agg
}

// avgFromSpec builds a soy.Aggregate (AVG) from an AggregateSpec.
func (f *Factory[T]) avgFromSpec(spec AggregateSpec) *soy.Aggregate[T] {
	agg := f.soy.Avg(spec.Field)

	// Add WHERE conditions
	for i := range spec.Where {
		agg = applyConditionToAggregate(agg, spec.Where[i])
	}

	return agg
}

// minFromSpec builds a soy.Aggregate (MIN) from an AggregateSpec.
func (f *Factory[T]) minFromSpec(spec AggregateSpec) *soy.Aggregate[T] {
	agg := f.soy.Min(spec.Field)

	// Add WHERE conditions
	for i := range spec.Where {
		agg = applyConditionToAggregate(agg, spec.Where[i])
	}

	return agg
}

// maxFromSpec builds a soy.Aggregate (MAX) from an AggregateSpec.
func (f *Factory[T]) maxFromSpec(spec AggregateSpec) *soy.Aggregate[T] {
	agg := f.soy.Max(spec.Field)

	// Add WHERE conditions
	for i := range spec.Where {
		agg = applyConditionToAggregate(agg, spec.Where[i])
	}

	return agg
}

// applyConditionToAggregate applies a ConditionSpec to an Aggregate builder.
// Handles simple conditions, condition groups (AND/OR), BETWEEN, and field comparisons.
func applyConditionToAggregate[T any](agg *soy.Aggregate[T], cond ConditionSpec) *soy.Aggregate[T] {
	if cond.IsGroup() {
		conditions := toConditions(cond.Group)
		if strings.EqualFold(cond.Logic, logicOR) {
			return agg.WhereOr(conditions...)
		}
		return agg.WhereAnd(conditions...)
	}

	// BETWEEN conditions
	if cond.IsBetween() {
		return agg.WhereBetween(cond.Field, cond.LowParam, cond.HighParam)
	}
	if cond.IsNotBetween() {
		return agg.WhereNotBetween(cond.Field, cond.LowParam, cond.HighParam)
	}

	// Field-to-field comparison
	if cond.IsFieldComparison() {
		return agg.WhereFields(cond.Field, cond.Operator, cond.RightField)
	}

	// NULL conditions
	if cond.IsNull {
		if cond.Operator == opIsNull {
			return agg.WhereNull(cond.Field)
		}
		return agg.WhereNotNull(cond.Field)
	}

	// Simple field-operator-param condition
	return agg.Where(cond.Field, cond.Operator, cond.Param)
}

// insertFromSpec builds a soy.Create from a CreateSpec.
// Returns an error if an invalid conflict action is specified.
func (f *Factory[T]) insertFromSpec(spec CreateSpec) (*soy.Create[T], error) {
	create := f.soy.Insert()

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

// compoundFromSpec builds a soy.Compound from a CompoundQuerySpec.
func (f *Factory[T]) compoundFromSpec(spec CompoundQuerySpec) (*soy.Compound[T], error) {
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

	var compound *soy.Compound[T]
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
