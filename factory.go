// Package edamame provides a capability-driven query factory built on cereal.
//
// Edamame wraps cereal to offer a declarative, introspectable API for database
// operations. It automatically registers CRUD capabilities and allows custom
// query patterns to be defined and discovered at runtime.
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
// Create a Factory instance (CRUD capabilities are registered automatically):
//
//	factory, err := edamame.New[User](db, "users")
//	if err != nil {
//	    log.Fatal(err)
//	}
//
// Use built-in capabilities:
//
//	// Select single record by primary key
//	sel, err := factory.Select("select")
//	user, err := sel.Exec(ctx, map[string]any{"id": 123})
//
//	// Query all records
//	q, err := factory.Query("query")
//	users, err := q.Exec(ctx, nil)
//
//	// Insert a new record
//	inserted, err := factory.Insert().Exec(ctx, &user)
//
//	// Delete by primary key
//	del, err := factory.Delete("delete")
//	deleted, err := del.Exec(ctx, map[string]any{"id": 123})
//
//	// Count all records
//	agg, err := factory.Aggregate("count")
//	count, err := agg.Exec(ctx, nil)
//
// Add custom capabilities:
//
//	factory.AddQuery(edamame.QueryCapability{
//	    Name:        "active-by-age",
//	    Description: "Find active users above minimum age",
//	    Spec: QuerySpec{
//	        Where: []ConditionSpec{
//	            {Field: "status", Operator: "=", Param: "status"},
//	            {Field: "age", Operator: ">=", Param: "min_age"},
//	        },
//	        OrderBy: []OrderBySpec{{Field: "name", Direction: "asc"}},
//	    },
//	})
//
// Introspect capabilities for LLM integration:
//
//	spec := factory.Spec()
//	json, _ := factory.SpecJSON()
package edamame

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/jmoiron/sqlx"
	"github.com/zoobzio/capitan"
	"github.com/zoobzio/cereal"
)

// Factory provides a capability-driven query API for a specific model type.
// It wraps cereal with named, introspectable query capabilities.
type Factory[T any] struct {
	cereal     *cereal.Cereal[T]
	primaryKey string

	queries    map[string]QueryCapability
	selects    map[string]SelectCapability
	updates    map[string]UpdateCapability
	deletes    map[string]DeleteCapability
	aggregates map[string]AggregateCapability

	mu sync.RWMutex
}

// New creates a new Factory for type T with the given database connection and table name.
// CRUD capabilities are registered automatically based on struct metadata.
func New[T any](db *sqlx.DB, tableName string) (*Factory[T], error) {
	c, err := cereal.New[T](db, tableName)
	if err != nil {
		return nil, fmt.Errorf("edamame: failed to create cereal instance: %w", err)
	}

	f := &Factory[T]{
		cereal:     c,
		queries:    make(map[string]QueryCapability),
		selects:    make(map[string]SelectCapability),
		updates:    make(map[string]UpdateCapability),
		deletes:    make(map[string]DeleteCapability),
		aggregates: make(map[string]AggregateCapability),
	}

	// Find primary key from metadata
	pk, err := f.findPrimaryKey()
	if err != nil {
		return nil, fmt.Errorf("edamame: %w", err)
	}
	f.primaryKey = pk

	// Register default CRUD capabilities
	f.registerDefaults()

	capitan.Emit(context.Background(), FactoryCreated,
		KeyTable.Field(tableName))

	return f, nil
}

// Cereal returns the underlying cereal instance for advanced usage.
func (f *Factory[T]) Cereal() *cereal.Cereal[T] {
	return f.cereal
}

// RenderQuery renders a query capability to SQL for inspection or debugging.
func (f *Factory[T]) RenderQuery(name string) (string, error) {
	q, err := f.Query(name)
	if err != nil {
		return "", err
	}
	result, err := q.Render()
	if err != nil {
		return "", err
	}
	return result.SQL, nil
}

// RenderSelect renders a select capability to SQL for inspection or debugging.
func (f *Factory[T]) RenderSelect(name string) (string, error) {
	s, err := f.Select(name)
	if err != nil {
		return "", err
	}
	result, err := s.Render()
	if err != nil {
		return "", err
	}
	return result.SQL, nil
}

// RenderUpdate renders an update capability to SQL for inspection or debugging.
func (f *Factory[T]) RenderUpdate(name string) (string, error) {
	u, err := f.Update(name)
	if err != nil {
		return "", err
	}
	result, err := u.Render()
	if err != nil {
		return "", err
	}
	return result.SQL, nil
}

// RenderDelete renders a delete capability to SQL for inspection or debugging.
func (f *Factory[T]) RenderDelete(name string) (string, error) {
	d, err := f.Delete(name)
	if err != nil {
		return "", err
	}
	result, err := d.Render()
	if err != nil {
		return "", err
	}
	return result.SQL, nil
}

// RenderAggregate renders an aggregate capability to SQL for inspection or debugging.
func (f *Factory[T]) RenderAggregate(name string) (string, error) {
	a, err := f.Aggregate(name)
	if err != nil {
		return "", err
	}
	result, err := a.Render()
	if err != nil {
		return "", err
	}
	return result.SQL, nil
}

// TableName returns the table name for this factory.
func (f *Factory[T]) TableName() string {
	return f.cereal.TableName()
}

// findPrimaryKey extracts the primary key field from struct metadata.
// Returns an error if no primary key constraint is found.
func (f *Factory[T]) findPrimaryKey() (string, error) {
	meta := f.cereal.Metadata()
	for _, field := range meta.Fields {
		constraints := strings.ToLower(field.Tags["constraints"])
		if strings.Contains(constraints, "primarykey") || strings.Contains(constraints, "primary_key") {
			return field.Tags["db"], nil
		}
	}
	return "", fmt.Errorf("no primary key constraint found for table %q: add `constraints:\"primarykey\"` to your struct", f.cereal.TableName())
}

// registerDefaults sets up the standard CRUD capabilities.
func (f *Factory[T]) registerDefaults() {
	pk := f.primaryKey
	pkType := f.fieldType(pk)

	// SELECT by primary key (single record)
	f.selects["select"] = SelectCapability{
		Name:        "select",
		Description: fmt.Sprintf("Select a single %s by primary key", f.cereal.TableName()),
		Spec: SelectSpec{
			Where: []ConditionSpec{
				{Field: pk, Operator: "=", Param: pk},
			},
		},
		Params: []ParamSpec{
			{Name: pk, Type: pkType, Required: true, Description: "Primary key value"},
		},
		Tags: []string{"crud", "read"},
	}

	// QUERY all records (multiple)
	f.queries["query"] = QueryCapability{
		Name:        "query",
		Description: fmt.Sprintf("Query all %s records", f.cereal.TableName()),
		Spec:        QuerySpec{},
		Params:      []ParamSpec{},
		Tags:        []string{"crud", "read"},
	}

	// DELETE by primary key
	f.deletes["delete"] = DeleteCapability{
		Name:        "delete",
		Description: fmt.Sprintf("Delete a %s by primary key", f.cereal.TableName()),
		Spec: DeleteSpec{
			Where: []ConditionSpec{
				{Field: pk, Operator: "=", Param: pk},
			},
		},
		Params: []ParamSpec{
			{Name: pk, Type: pkType, Required: true, Description: "Primary key value"},
		},
		Tags: []string{"crud", "write"},
	}

	// COUNT all records
	f.aggregates["count"] = AggregateCapability{
		Name:        "count",
		Description: fmt.Sprintf("Count all %s records", f.cereal.TableName()),
		Spec:        AggregateSpec{},
		Func:        AggCount,
		Params:      []ParamSpec{},
		Tags:        []string{"crud", "read", "aggregate"},
	}
}

// fieldType returns the type string for a field from metadata.
func (f *Factory[T]) fieldType(fieldName string) string {
	meta := f.cereal.Metadata()
	for _, field := range meta.Fields {
		if field.Tags["db"] == fieldName {
			if t := field.Tags["type"]; t != "" {
				return t
			}
			return "any"
		}
	}
	return "any"
}

// AddQuery registers a custom query capability.
func (f *Factory[T]) AddQuery(cap QueryCapability) {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Derive params from spec if not provided
	if len(cap.Params) == 0 {
		cap.Params = f.deriveQueryParams(cap.Spec)
	}

	f.queries[cap.Name] = cap

	capitan.Emit(context.Background(), CapabilityAdded,
		KeyTable.Field(f.cereal.TableName()),
		KeyCapability.Field(cap.Name),
		KeyType.Field("query"))
}

// AddSelect registers a custom select capability.
func (f *Factory[T]) AddSelect(cap SelectCapability) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if len(cap.Params) == 0 {
		cap.Params = f.deriveSelectParams(cap.Spec)
	}

	f.selects[cap.Name] = cap

	capitan.Emit(context.Background(), CapabilityAdded,
		KeyTable.Field(f.cereal.TableName()),
		KeyCapability.Field(cap.Name),
		KeyType.Field("select"))
}

// AddUpdate registers a custom update capability.
func (f *Factory[T]) AddUpdate(cap UpdateCapability) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if len(cap.Params) == 0 {
		cap.Params = f.deriveUpdateParams(cap.Spec)
	}

	f.updates[cap.Name] = cap

	capitan.Emit(context.Background(), CapabilityAdded,
		KeyTable.Field(f.cereal.TableName()),
		KeyCapability.Field(cap.Name),
		KeyType.Field("update"))
}

// AddDelete registers a custom delete capability.
func (f *Factory[T]) AddDelete(cap DeleteCapability) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if len(cap.Params) == 0 {
		cap.Params = f.deriveParams(cap.Spec.Where)
	}

	f.deletes[cap.Name] = cap

	capitan.Emit(context.Background(), CapabilityAdded,
		KeyTable.Field(f.cereal.TableName()),
		KeyCapability.Field(cap.Name),
		KeyType.Field("delete"))
}

// AddAggregate registers a custom aggregate capability.
func (f *Factory[T]) AddAggregate(cap AggregateCapability) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if len(cap.Params) == 0 {
		cap.Params = f.deriveParams(cap.Spec.Where)
	}

	f.aggregates[cap.Name] = cap

	capitan.Emit(context.Background(), CapabilityAdded,
		KeyTable.Field(f.cereal.TableName()),
		KeyCapability.Field(cap.Name),
		KeyType.Field("aggregate"))
}

// deriveParams extracts parameter specifications from WHERE conditions.
// Handles nested condition groups (AND/OR) recursively.
func (f *Factory[T]) deriveParams(conditions []ConditionSpec) []ParamSpec {
	if len(conditions) == 0 {
		return []ParamSpec{}
	}

	params := make([]ParamSpec, 0, len(conditions))
	seen := make(map[string]bool)

	f.collectParams(conditions, seen, &params)

	return params
}

// deriveQueryParams extracts params from all parts of a QuerySpec.
func (f *Factory[T]) deriveQueryParams(spec QuerySpec) []ParamSpec {
	seen := make(map[string]bool)
	params := make([]ParamSpec, 0)

	// WHERE conditions
	f.collectParams(spec.Where, seen, &params)

	// HAVING conditions
	f.collectParams(spec.Having, seen, &params)

	// HAVING aggregate conditions
	for _, h := range spec.HavingAgg {
		if h.Param != "" && !seen[h.Param] {
			seen[h.Param] = true
			params = append(params, ParamSpec{
				Name:     h.Param,
				Type:     "any",
				Required: true,
			})
		}
	}

	// ORDER BY expressions (for vector distance params)
	for _, o := range spec.OrderBy {
		if o.IsExpression() && !seen[o.Param] {
			seen[o.Param] = true
			params = append(params, ParamSpec{
				Name:     o.Param,
				Type:     f.fieldType(o.Field),
				Required: true,
			})
		}
	}

	return params
}

// deriveSelectParams extracts params from all parts of a SelectSpec.
func (f *Factory[T]) deriveSelectParams(spec SelectSpec) []ParamSpec {
	seen := make(map[string]bool)
	params := make([]ParamSpec, 0)

	// WHERE conditions
	f.collectParams(spec.Where, seen, &params)

	// HAVING conditions
	f.collectParams(spec.Having, seen, &params)

	// HAVING aggregate conditions
	for _, h := range spec.HavingAgg {
		if h.Param != "" && !seen[h.Param] {
			seen[h.Param] = true
			params = append(params, ParamSpec{
				Name:     h.Param,
				Type:     "any",
				Required: true,
			})
		}
	}

	// ORDER BY expressions (for vector distance params)
	for _, o := range spec.OrderBy {
		if o.IsExpression() && !seen[o.Param] {
			seen[o.Param] = true
			params = append(params, ParamSpec{
				Name:     o.Param,
				Type:     f.fieldType(o.Field),
				Required: true,
			})
		}
	}

	return params
}

// collectParams recursively collects params from conditions, including nested groups.
func (f *Factory[T]) collectParams(conditions []ConditionSpec, seen map[string]bool, params *[]ParamSpec) {
	for _, cond := range conditions {
		// Handle condition groups (AND/OR)
		if cond.IsGroup() {
			f.collectParams(cond.Group, seen, params)
			continue
		}

		// Simple condition
		if cond.Param == "" || seen[cond.Param] {
			continue
		}
		seen[cond.Param] = true

		*params = append(*params, ParamSpec{
			Name:     cond.Param,
			Type:     f.fieldType(cond.Field),
			Required: true,
		})
	}
}

// deriveUpdateParams extracts params from both SET and WHERE clauses.
// Handles nested condition groups (AND/OR) recursively.
func (f *Factory[T]) deriveUpdateParams(spec UpdateSpec) []ParamSpec {
	seen := make(map[string]bool)
	params := make([]ParamSpec, 0)

	// SET params
	for field, param := range spec.Set {
		if seen[param] {
			continue
		}
		seen[param] = true

		params = append(params, ParamSpec{
			Name:     param,
			Type:     f.fieldType(field),
			Required: true,
		})
	}

	// WHERE params (including nested groups)
	f.collectParams(spec.Where, seen, &params)

	return params
}

// RemoveQuery removes a query capability by name.
func (f *Factory[T]) RemoveQuery(name string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()

	if _, exists := f.queries[name]; exists {
		delete(f.queries, name)
		capitan.Emit(context.Background(), CapabilityRemoved,
			KeyTable.Field(f.cereal.TableName()),
			KeyCapability.Field(name),
			KeyType.Field("query"))
		return true
	}
	return false
}

// RemoveSelect removes a select capability by name.
func (f *Factory[T]) RemoveSelect(name string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()

	if _, exists := f.selects[name]; exists {
		delete(f.selects, name)
		capitan.Emit(context.Background(), CapabilityRemoved,
			KeyTable.Field(f.cereal.TableName()),
			KeyCapability.Field(name),
			KeyType.Field("select"))
		return true
	}
	return false
}

// RemoveUpdate removes an update capability by name.
func (f *Factory[T]) RemoveUpdate(name string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()

	if _, exists := f.updates[name]; exists {
		delete(f.updates, name)
		capitan.Emit(context.Background(), CapabilityRemoved,
			KeyTable.Field(f.cereal.TableName()),
			KeyCapability.Field(name),
			KeyType.Field("update"))
		return true
	}
	return false
}

// RemoveDelete removes a delete capability by name.
func (f *Factory[T]) RemoveDelete(name string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()

	if _, exists := f.deletes[name]; exists {
		delete(f.deletes, name)
		capitan.Emit(context.Background(), CapabilityRemoved,
			KeyTable.Field(f.cereal.TableName()),
			KeyCapability.Field(name),
			KeyType.Field("delete"))
		return true
	}
	return false
}

// RemoveAggregate removes an aggregate capability by name.
func (f *Factory[T]) RemoveAggregate(name string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()

	if _, exists := f.aggregates[name]; exists {
		delete(f.aggregates, name)
		capitan.Emit(context.Background(), CapabilityRemoved,
			KeyTable.Field(f.cereal.TableName()),
			KeyCapability.Field(name),
			KeyType.Field("aggregate"))
		return true
	}
	return false
}

// HasQuery checks if a query capability exists.
func (f *Factory[T]) HasQuery(name string) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	_, exists := f.queries[name]
	return exists
}

// HasSelect checks if a select capability exists.
func (f *Factory[T]) HasSelect(name string) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	_, exists := f.selects[name]
	return exists
}

// HasUpdate checks if an update capability exists.
func (f *Factory[T]) HasUpdate(name string) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	_, exists := f.updates[name]
	return exists
}

// HasDelete checks if a delete capability exists.
func (f *Factory[T]) HasDelete(name string) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	_, exists := f.deletes[name]
	return exists
}

// HasAggregate checks if an aggregate capability exists.
func (f *Factory[T]) HasAggregate(name string) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	_, exists := f.aggregates[name]
	return exists
}

// GetQuery returns a query capability by name.
func (f *Factory[T]) GetQuery(name string) (QueryCapability, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	cap, exists := f.queries[name]
	return cap, exists
}

// GetSelect returns a select capability by name.
func (f *Factory[T]) GetSelect(name string) (SelectCapability, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	cap, exists := f.selects[name]
	return cap, exists
}

// GetUpdate returns an update capability by name.
func (f *Factory[T]) GetUpdate(name string) (UpdateCapability, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	cap, exists := f.updates[name]
	return cap, exists
}

// GetDelete returns a delete capability by name.
func (f *Factory[T]) GetDelete(name string) (DeleteCapability, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	cap, exists := f.deletes[name]
	return cap, exists
}

// GetAggregate returns an aggregate capability by name.
func (f *Factory[T]) GetAggregate(name string) (AggregateCapability, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	cap, exists := f.aggregates[name]
	return cap, exists
}

// ListQueries returns all registered query capability names.
func (f *Factory[T]) ListQueries() []string {
	f.mu.RLock()
	defer f.mu.RUnlock()

	names := make([]string, 0, len(f.queries))
	for name := range f.queries {
		names = append(names, name)
	}
	return names
}

// ListSelects returns all registered select capability names.
func (f *Factory[T]) ListSelects() []string {
	f.mu.RLock()
	defer f.mu.RUnlock()

	names := make([]string, 0, len(f.selects))
	for name := range f.selects {
		names = append(names, name)
	}
	return names
}

// ListUpdates returns all registered update capability names.
func (f *Factory[T]) ListUpdates() []string {
	f.mu.RLock()
	defer f.mu.RUnlock()

	names := make([]string, 0, len(f.updates))
	for name := range f.updates {
		names = append(names, name)
	}
	return names
}

// ListDeletes returns all registered delete capability names.
func (f *Factory[T]) ListDeletes() []string {
	f.mu.RLock()
	defer f.mu.RUnlock()

	names := make([]string, 0, len(f.deletes))
	for name := range f.deletes {
		names = append(names, name)
	}
	return names
}

// ListAggregates returns all registered aggregate capability names.
func (f *Factory[T]) ListAggregates() []string {
	f.mu.RLock()
	defer f.mu.RUnlock()

	names := make([]string, 0, len(f.aggregates))
	for name := range f.aggregates {
		names = append(names, name)
	}
	return names
}
