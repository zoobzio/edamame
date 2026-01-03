// Package edamame provides a capability-driven query factory built on soy.
//
// Edamame wraps soy to offer a declarative, introspectable API for database
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
//	import "github.com/zoobzio/astql/pkg/postgres"
//
//	factory, err := edamame.New[User](db, "users", postgres.New())
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
	"github.com/zoobzio/astql"
	"github.com/zoobzio/capitan"
	"github.com/zoobzio/soy"
)

// DefaultMaxConditionDepth is the default maximum nesting depth for condition groups.
const DefaultMaxConditionDepth = 10

// Factory provides a capability-driven query API for a specific model type.
// It wraps soy with named, introspectable query capabilities.
type Factory[T any] struct {
	db         sqlx.ExtContext
	soy        *soy.Soy[T]
	primaryKey string

	queries    map[string]QueryCapability
	selects    map[string]SelectCapability
	updates    map[string]UpdateCapability
	deletes    map[string]DeleteCapability
	aggregates map[string]AggregateCapability

	// Configuration
	maxConditionDepth int

	// SQL cache (keyed by "type:name", e.g., "query:active-users")
	sqlCache map[string]string

	mu sync.RWMutex
}

// New creates a new Factory for type T with the given database connection, table name, and renderer.
// CRUD capabilities are registered automatically based on struct metadata.
//
// The db parameter accepts sqlx.ExtContext, which is satisfied by both *sqlx.DB and *sqlx.Tx,
// enabling transaction support by passing a transaction instead of a database connection.
func New[T any](db sqlx.ExtContext, tableName string, renderer astql.Renderer) (*Factory[T], error) {
	c, err := soy.New[T](db, tableName, renderer)
	if err != nil {
		return nil, fmt.Errorf("edamame: failed to create soy instance: %w", err)
	}

	f := &Factory[T]{
		db:                db,
		soy:               c,
		queries:           make(map[string]QueryCapability),
		selects:           make(map[string]SelectCapability),
		updates:           make(map[string]UpdateCapability),
		deletes:           make(map[string]DeleteCapability),
		aggregates:        make(map[string]AggregateCapability),
		maxConditionDepth: DefaultMaxConditionDepth,
		sqlCache:          make(map[string]string),
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

// Soy returns the underlying soy instance for advanced usage.
func (f *Factory[T]) Soy() *soy.Soy[T] {
	return f.soy
}

// RenderQuery renders a query capability to SQL for inspection or debugging.
// Results are cached until the capability is modified or removed.
func (f *Factory[T]) RenderQuery(name string) (string, error) {
	cacheKey := "query:" + name

	// Check cache first
	f.mu.RLock()
	if sql, ok := f.sqlCache[cacheKey]; ok {
		f.mu.RUnlock()
		return sql, nil
	}
	f.mu.RUnlock()

	// Build and render
	q, err := f.Query(name)
	if err != nil {
		return "", err
	}
	result, err := q.Render()
	if err != nil {
		return "", err
	}

	// Cache the result
	f.mu.Lock()
	f.sqlCache[cacheKey] = result.SQL
	f.mu.Unlock()

	return result.SQL, nil
}

// RenderSelect renders a select capability to SQL for inspection or debugging.
// Results are cached until the capability is modified or removed.
func (f *Factory[T]) RenderSelect(name string) (string, error) {
	cacheKey := "select:" + name

	// Check cache first
	f.mu.RLock()
	if sql, ok := f.sqlCache[cacheKey]; ok {
		f.mu.RUnlock()
		return sql, nil
	}
	f.mu.RUnlock()

	// Build and render
	s, err := f.Select(name)
	if err != nil {
		return "", err
	}
	result, err := s.Render()
	if err != nil {
		return "", err
	}

	// Cache the result
	f.mu.Lock()
	f.sqlCache[cacheKey] = result.SQL
	f.mu.Unlock()

	return result.SQL, nil
}

// RenderUpdate renders an update capability to SQL for inspection or debugging.
// Results are cached until the capability is modified or removed.
func (f *Factory[T]) RenderUpdate(name string) (string, error) {
	cacheKey := "update:" + name

	// Check cache first
	f.mu.RLock()
	if sql, ok := f.sqlCache[cacheKey]; ok {
		f.mu.RUnlock()
		return sql, nil
	}
	f.mu.RUnlock()

	// Build and render
	u, err := f.Update(name)
	if err != nil {
		return "", err
	}
	result, err := u.Render()
	if err != nil {
		return "", err
	}

	// Cache the result
	f.mu.Lock()
	f.sqlCache[cacheKey] = result.SQL
	f.mu.Unlock()

	return result.SQL, nil
}

// RenderDelete renders a delete capability to SQL for inspection or debugging.
// Results are cached until the capability is modified or removed.
func (f *Factory[T]) RenderDelete(name string) (string, error) {
	cacheKey := "delete:" + name

	// Check cache first
	f.mu.RLock()
	if sql, ok := f.sqlCache[cacheKey]; ok {
		f.mu.RUnlock()
		return sql, nil
	}
	f.mu.RUnlock()

	// Build and render
	d, err := f.Delete(name)
	if err != nil {
		return "", err
	}
	result, err := d.Render()
	if err != nil {
		return "", err
	}

	// Cache the result
	f.mu.Lock()
	f.sqlCache[cacheKey] = result.SQL
	f.mu.Unlock()

	return result.SQL, nil
}

// RenderAggregate renders an aggregate capability to SQL for inspection or debugging.
// Results are cached until the capability is modified or removed.
func (f *Factory[T]) RenderAggregate(name string) (string, error) {
	cacheKey := "aggregate:" + name

	// Check cache first
	f.mu.RLock()
	if sql, ok := f.sqlCache[cacheKey]; ok {
		f.mu.RUnlock()
		return sql, nil
	}
	f.mu.RUnlock()

	// Build and render
	a, err := f.Aggregate(name)
	if err != nil {
		return "", err
	}
	result, err := a.Render()
	if err != nil {
		return "", err
	}

	// Cache the result
	f.mu.Lock()
	f.sqlCache[cacheKey] = result.SQL
	f.mu.Unlock()

	return result.SQL, nil
}

// RenderCompound renders a compound query to SQL for inspection or debugging.
func (f *Factory[T]) RenderCompound(spec CompoundQuerySpec) (string, error) {
	c, err := f.Compound(spec)
	if err != nil {
		return "", err
	}
	result, err := c.Render()
	if err != nil {
		return "", err
	}
	return result.SQL, nil
}

// TableName returns the table name for this factory.
func (f *Factory[T]) TableName() string {
	return f.soy.TableName()
}

// SetMaxConditionDepth sets the maximum nesting depth for condition groups.
// A depth of 0 or negative disables depth checking.
func (f *Factory[T]) SetMaxConditionDepth(depth int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.maxConditionDepth = depth
}

// MaxConditionDepth returns the current maximum condition depth setting.
func (f *Factory[T]) MaxConditionDepth() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.maxConditionDepth
}

// findPrimaryKey extracts the primary key field from struct metadata.
// Returns an error if no primary key constraint is found.
func (f *Factory[T]) findPrimaryKey() (string, error) {
	meta := f.soy.Metadata()
	for _, field := range meta.Fields {
		constraints := strings.ToLower(field.Tags["constraints"])
		if strings.Contains(constraints, "primarykey") || strings.Contains(constraints, "primary_key") {
			return field.Tags["db"], nil
		}
	}
	return "", fmt.Errorf("no primary key constraint found for table %q: add `constraints:\"primarykey\"` to your struct", f.soy.TableName())
}

// registerDefaults sets up the standard CRUD capabilities.
func (f *Factory[T]) registerDefaults() {
	pk := f.primaryKey
	pkType := f.fieldType(pk)

	// SELECT by primary key (single record)
	f.selects["select"] = SelectCapability{
		Name:        "select",
		Description: fmt.Sprintf("Select a single %s by primary key", f.soy.TableName()),
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
		Description: fmt.Sprintf("Query all %s records", f.soy.TableName()),
		Spec:        QuerySpec{},
		Params:      []ParamSpec{},
		Tags:        []string{"crud", "read"},
	}

	// DELETE by primary key
	f.deletes["delete"] = DeleteCapability{
		Name:        "delete",
		Description: fmt.Sprintf("Delete a %s by primary key", f.soy.TableName()),
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
		Description: fmt.Sprintf("Count all %s records", f.soy.TableName()),
		Spec:        AggregateSpec{},
		Func:        AggCount,
		Params:      []ParamSpec{},
		Tags:        []string{"crud", "read", "aggregate"},
	}
}

// fieldType returns the type string for a field from metadata.
func (f *Factory[T]) fieldType(fieldName string) string {
	meta := f.soy.Metadata()
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
// Returns ErrMaxDepthExceeded if the spec's condition nesting exceeds the configured maximum.
func (f *Factory[T]) AddQuery(c QueryCapability) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Derive params from spec if not provided
	if len(c.Params) == 0 {
		params, err := f.deriveQueryParams(c.Spec)
		if err != nil {
			return fmt.Errorf("query %q: %w", c.Name, err)
		}
		c.Params = params
	}

	// Invalidate cache for this capability
	delete(f.sqlCache, "query:"+c.Name)

	f.queries[c.Name] = c

	capitan.Emit(context.Background(), CapabilityAdded,
		KeyTable.Field(f.soy.TableName()),
		KeyCapability.Field(c.Name),
		KeyType.Field("query"))

	return nil
}

// AddSelect registers a custom select capability.
// Returns ErrMaxDepthExceeded if the spec's condition nesting exceeds the configured maximum.
func (f *Factory[T]) AddSelect(c SelectCapability) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if len(c.Params) == 0 {
		params, err := f.deriveSelectParams(c.Spec)
		if err != nil {
			return fmt.Errorf("select %q: %w", c.Name, err)
		}
		c.Params = params
	}

	// Invalidate cache for this capability
	delete(f.sqlCache, "select:"+c.Name)

	f.selects[c.Name] = c

	capitan.Emit(context.Background(), CapabilityAdded,
		KeyTable.Field(f.soy.TableName()),
		KeyCapability.Field(c.Name),
		KeyType.Field("select"))

	return nil
}

// AddUpdate registers a custom update capability.
// Returns ErrMaxDepthExceeded if the spec's condition nesting exceeds the configured maximum.
func (f *Factory[T]) AddUpdate(c UpdateCapability) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if len(c.Params) == 0 {
		params, err := f.deriveUpdateParams(c.Spec)
		if err != nil {
			return fmt.Errorf("update %q: %w", c.Name, err)
		}
		c.Params = params
	}

	// Invalidate cache for this capability
	delete(f.sqlCache, "update:"+c.Name)

	f.updates[c.Name] = c

	capitan.Emit(context.Background(), CapabilityAdded,
		KeyTable.Field(f.soy.TableName()),
		KeyCapability.Field(c.Name),
		KeyType.Field("update"))

	return nil
}

// AddDelete registers a custom delete capability.
// Returns ErrMaxDepthExceeded if the spec's condition nesting exceeds the configured maximum.
func (f *Factory[T]) AddDelete(c DeleteCapability) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if len(c.Params) == 0 {
		params, err := f.deriveParams(c.Spec.Where)
		if err != nil {
			return fmt.Errorf("delete %q: %w", c.Name, err)
		}
		c.Params = params
	}

	// Invalidate cache for this capability
	delete(f.sqlCache, "delete:"+c.Name)

	f.deletes[c.Name] = c

	capitan.Emit(context.Background(), CapabilityAdded,
		KeyTable.Field(f.soy.TableName()),
		KeyCapability.Field(c.Name),
		KeyType.Field("delete"))

	return nil
}

// AddAggregate registers a custom aggregate capability.
// Returns ErrMaxDepthExceeded if the spec's condition nesting exceeds the configured maximum.
func (f *Factory[T]) AddAggregate(c AggregateCapability) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if len(c.Params) == 0 {
		params, err := f.deriveParams(c.Spec.Where)
		if err != nil {
			return fmt.Errorf("aggregate %q: %w", c.Name, err)
		}
		c.Params = params
	}

	// Invalidate cache for this capability
	delete(f.sqlCache, "aggregate:"+c.Name)

	f.aggregates[c.Name] = c

	capitan.Emit(context.Background(), CapabilityAdded,
		KeyTable.Field(f.soy.TableName()),
		KeyCapability.Field(c.Name),
		KeyType.Field("aggregate"))

	return nil
}

// deriveParams extracts parameter specifications from WHERE conditions.
// Handles nested condition groups (AND/OR) recursively.
// Returns ErrMaxDepthExceeded if nesting depth exceeds the configured maximum.
func (f *Factory[T]) deriveParams(conditions []ConditionSpec) ([]ParamSpec, error) {
	if len(conditions) == 0 {
		return []ParamSpec{}, nil
	}

	params := make([]ParamSpec, 0, len(conditions))
	seen := make(map[string]bool)

	if err := f.collectParams(conditions, seen, &params, 1); err != nil {
		return nil, err
	}

	return params, nil
}

// deriveQueryParams extracts params from all parts of a QuerySpec.
// Returns ErrMaxDepthExceeded if nesting depth exceeds the configured maximum.
func (f *Factory[T]) deriveQueryParams(spec QuerySpec) ([]ParamSpec, error) {
	seen := make(map[string]bool)
	params := make([]ParamSpec, 0)

	// WHERE conditions
	if err := f.collectParams(spec.Where, seen, &params, 1); err != nil {
		return nil, err
	}

	// HAVING conditions
	if err := f.collectParams(spec.Having, seen, &params, 1); err != nil {
		return nil, err
	}

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

	return params, nil
}

// deriveSelectParams extracts params from all parts of a SelectSpec.
// Returns ErrMaxDepthExceeded if nesting depth exceeds the configured maximum.
func (f *Factory[T]) deriveSelectParams(spec SelectSpec) ([]ParamSpec, error) {
	seen := make(map[string]bool)
	params := make([]ParamSpec, 0)

	// WHERE conditions
	if err := f.collectParams(spec.Where, seen, &params, 1); err != nil {
		return nil, err
	}

	// HAVING conditions
	if err := f.collectParams(spec.Having, seen, &params, 1); err != nil {
		return nil, err
	}

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

	return params, nil
}

// ErrMaxDepthExceeded is returned when condition nesting exceeds the configured maximum.
var ErrMaxDepthExceeded = fmt.Errorf("maximum condition depth exceeded")

// collectParams recursively collects params from conditions, including nested groups.
// Returns ErrMaxDepthExceeded if nesting depth exceeds maxConditionDepth.
func (f *Factory[T]) collectParams(conditions []ConditionSpec, seen map[string]bool, params *[]ParamSpec, depth int) error {
	// Check depth limit (0 or negative disables checking)
	if f.maxConditionDepth > 0 && depth > f.maxConditionDepth {
		return fmt.Errorf("%w: depth %d exceeds maximum %d", ErrMaxDepthExceeded, depth, f.maxConditionDepth)
	}

	for i := range conditions {
		// Handle condition groups (AND/OR)
		if conditions[i].IsGroup() {
			if err := f.collectParams(conditions[i].Group, seen, params, depth+1); err != nil {
				return err
			}
			continue
		}

		// Simple condition
		if conditions[i].Param == "" || seen[conditions[i].Param] {
			continue
		}
		seen[conditions[i].Param] = true

		*params = append(*params, ParamSpec{
			Name:     conditions[i].Param,
			Type:     f.fieldType(conditions[i].Field),
			Required: true,
		})
	}
	return nil
}

// deriveUpdateParams extracts params from both SET and WHERE clauses.
// Handles nested condition groups (AND/OR) recursively.
// Returns ErrMaxDepthExceeded if nesting depth exceeds the configured maximum.
func (f *Factory[T]) deriveUpdateParams(spec UpdateSpec) ([]ParamSpec, error) {
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
	if err := f.collectParams(spec.Where, seen, &params, 1); err != nil {
		return nil, err
	}

	return params, nil
}

// RemoveQuery removes a query capability by name.
func (f *Factory[T]) RemoveQuery(name string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()

	if _, exists := f.queries[name]; exists {
		delete(f.queries, name)
		delete(f.sqlCache, "query:"+name)
		capitan.Emit(context.Background(), CapabilityRemoved,
			KeyTable.Field(f.soy.TableName()),
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
		delete(f.sqlCache, "select:"+name)
		capitan.Emit(context.Background(), CapabilityRemoved,
			KeyTable.Field(f.soy.TableName()),
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
		delete(f.sqlCache, "update:"+name)
		capitan.Emit(context.Background(), CapabilityRemoved,
			KeyTable.Field(f.soy.TableName()),
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
		delete(f.sqlCache, "delete:"+name)
		capitan.Emit(context.Background(), CapabilityRemoved,
			KeyTable.Field(f.soy.TableName()),
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
		delete(f.sqlCache, "aggregate:"+name)
		capitan.Emit(context.Background(), CapabilityRemoved,
			KeyTable.Field(f.soy.TableName()),
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
	c, exists := f.queries[name]
	return c, exists
}

// GetSelect returns a select capability by name.
func (f *Factory[T]) GetSelect(name string) (SelectCapability, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	c, exists := f.selects[name]
	return c, exists
}

// GetUpdate returns an update capability by name.
func (f *Factory[T]) GetUpdate(name string) (UpdateCapability, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	c, exists := f.updates[name]
	return c, exists
}

// GetDelete returns a delete capability by name.
func (f *Factory[T]) GetDelete(name string) (DeleteCapability, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	c, exists := f.deletes[name]
	return c, exists
}

// GetAggregate returns an aggregate capability by name.
func (f *Factory[T]) GetAggregate(name string) (AggregateCapability, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	c, exists := f.aggregates[name]
	return c, exists
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
