package edamame

import (
	"encoding/json"
	"sort"
)

// -----------------------------------------------------------------------------
// Query Building Specs
// -----------------------------------------------------------------------------

// ConditionSpec represents a WHERE condition in a serializable format.
// This can be used to build queries from JSON or other external sources (e.g., LLM-generated).
//
// ConditionSpec can represent either a simple condition or a condition group:
//
// Simple condition:
//
//	{"field": "age", "operator": ">=", "param": "min_age"}
//
// IS NULL condition:
//
//	{"field": "email", "is_null": true}
//
// Condition group (AND/OR):
//
//	{
//	  "logic": "OR",
//	  "group": [
//	    {"field": "status", "operator": "=", "param": "active"},
//	    {"field": "status", "operator": "=", "param": "pending"}
//	  ]
//	}
type ConditionSpec struct {
	// Simple condition fields
	Field    string `json:"field,omitempty"`
	Operator string `json:"operator,omitempty"`
	Param    string `json:"param,omitempty"`
	IsNull   bool   `json:"is_null,omitempty"`

	// Condition group fields (for AND/OR grouping)
	Logic string          `json:"logic,omitempty"` // "AND" or "OR"
	Group []ConditionSpec `json:"group,omitempty"` // Nested conditions
}

// IsGroup returns true if this ConditionSpec represents a condition group.
func (c ConditionSpec) IsGroup() bool {
	return c.Logic != "" && len(c.Group) > 0
}

// OrderBySpec represents an ORDER BY clause in a serializable format.
//
// Simple ordering:
//
//	{"field": "name", "direction": "asc"}
//
// Ordering with nulls:
//
//	{"field": "name", "direction": "asc", "nulls": "last"}
//
// Expression-based ordering (for vector distance with pgvector):
//
//	{"field": "embedding", "operator": "<->", "param": "query_vec", "direction": "asc"}
type OrderBySpec struct {
	Field     string `json:"field"`
	Direction string `json:"direction"`          // "asc" or "desc"
	Nulls     string `json:"nulls,omitempty"`    // "first" or "last" for NULLS FIRST/LAST
	Operator  string `json:"operator,omitempty"` // For vector ops: "<->", "<#>", "<=>", "<+>"
	Param     string `json:"param,omitempty"`    // Parameter for expression-based ordering
}

// HasNulls returns true if this OrderBySpec specifies NULLS ordering.
func (o OrderBySpec) HasNulls() bool {
	return o.Nulls != ""
}

// IsExpression returns true if this OrderBySpec uses an expression (field <op> param).
func (o OrderBySpec) IsExpression() bool {
	return o.Operator != "" && o.Param != ""
}

// HavingAggSpec represents an aggregate HAVING condition in a serializable format.
// Used for conditions like HAVING COUNT(*) > 10 or HAVING SUM("amount") >= :threshold.
//
// Example JSON for COUNT(*):
//
//	{"func": "count", "operator": ">", "param": "min_count"}
//
// Example JSON for SUM(field):
//
//	{"func": "sum", "field": "amount", "operator": ">=", "param": "min_total"}
type HavingAggSpec struct {
	Func     string `json:"func"`            // "count", "sum", "avg", "min", "max", "count_distinct"
	Field    string `json:"field,omitempty"` // Field to aggregate (empty for COUNT(*))
	Operator string `json:"operator"`        // Comparison operator
	Param    string `json:"param"`           // Parameter name for comparison value
}

// QuerySpec represents a SELECT query that returns multiple records in a serializable format.
// This can be unmarshaled from JSON to build complex queries programmatically.
//
// Example JSON:
//
//	{
//	  "fields": ["id", "email", "name"],
//	  "where": [
//	    {"field": "age", "operator": ">=", "param": "min_age"},
//	    {"field": "status", "operator": "=", "param": "status"}
//	  ],
//	  "order_by": [{"field": "name", "direction": "asc"}],
//	  "group_by": ["status"],
//	  "having": [{"field": "age", "operator": ">", "param": "min_age"}],
//	  "having_agg": [{"func": "count", "operator": ">", "param": "min_count"}],
//	  "limit": 10,
//	  "offset": 20,
//	  "distinct_on": ["user_id"],
//	  "for_locking": "update"
//	}
type QuerySpec struct {
	Fields     []string        `json:"fields,omitempty"`
	Where      []ConditionSpec `json:"where,omitempty"`
	OrderBy    []OrderBySpec   `json:"order_by,omitempty"`
	GroupBy    []string        `json:"group_by,omitempty"`
	Having     []ConditionSpec `json:"having,omitempty"`
	HavingAgg  []HavingAggSpec `json:"having_agg,omitempty"`
	Limit      *int            `json:"limit,omitempty"`
	Offset     *int            `json:"offset,omitempty"`
	Distinct   bool            `json:"distinct,omitempty"`
	DistinctOn []string        `json:"distinct_on,omitempty"` // PostgreSQL DISTINCT ON fields
	ForLocking string          `json:"for_locking,omitempty"` // "update", "no_key_update", "share", "key_share"
}

// SelectSpec represents a SELECT query that returns a single record in a serializable format.
// This can be unmarshaled from JSON to build complex queries programmatically.
//
// Example JSON:
//
//	{
//	  "fields": ["id", "email"],
//	  "where": [
//	    {"field": "email", "operator": "=", "param": "user_email"}
//	  ],
//	  "order_by": [{"field": "created_at", "direction": "desc", "nulls": "last"}],
//	  "group_by": ["status"],
//	  "having": [{"field": "age", "operator": ">", "param": "min_age"}],
//	  "having_agg": [{"func": "count", "operator": ">", "param": "min_count"}],
//	  "limit": 1,
//	  "distinct_on": ["user_id"],
//	  "for_locking": "update"
//	}
type SelectSpec struct {
	Fields     []string        `json:"fields,omitempty"`
	Where      []ConditionSpec `json:"where,omitempty"`
	OrderBy    []OrderBySpec   `json:"order_by,omitempty"`
	GroupBy    []string        `json:"group_by,omitempty"`
	Having     []ConditionSpec `json:"having,omitempty"`
	HavingAgg  []HavingAggSpec `json:"having_agg,omitempty"`
	Limit      *int            `json:"limit,omitempty"`
	Offset     *int            `json:"offset,omitempty"`
	Distinct   bool            `json:"distinct,omitempty"`
	DistinctOn []string        `json:"distinct_on,omitempty"` // PostgreSQL DISTINCT ON fields
	ForLocking string          `json:"for_locking,omitempty"` // "update", "no_key_update", "share", "key_share"
}

// UpdateSpec represents an UPDATE query in a serializable format.
// This can be unmarshaled from JSON to build complex queries programmatically.
//
// Example JSON:
//
//	{
//	  "set": {
//	    "name": "new_name",
//	    "age": "new_age"
//	  },
//	  "where": [
//	    {"field": "id", "operator": "=", "param": "user_id"}
//	  ]
//	}
type UpdateSpec struct {
	Set   map[string]string `json:"set"`
	Where []ConditionSpec   `json:"where"`
}

// CreateSpec represents an INSERT query with optional ON CONFLICT handling.
// This can be unmarshaled from JSON to build complex queries programmatically.
//
// Example JSON for simple insert (no conflict handling):
//
//	{}
//
// Example JSON for ON CONFLICT DO NOTHING:
//
//	{
//	  "on_conflict": ["email"],
//	  "conflict_action": "nothing"
//	}
//
// Example JSON for ON CONFLICT DO UPDATE:
//
//	{
//	  "on_conflict": ["email"],
//	  "conflict_action": "update",
//	  "conflict_set": {
//	    "name": "updated_name",
//	    "updated_at": "now"
//	  }
//	}
type CreateSpec struct {
	OnConflict     []string          `json:"on_conflict,omitempty"`     // Conflict columns
	ConflictAction string            `json:"conflict_action,omitempty"` // "nothing" or "update"
	ConflictSet    map[string]string `json:"conflict_set,omitempty"`    // Fields to update on conflict
}

// DeleteSpec represents a DELETE query in a serializable format.
// This can be unmarshaled from JSON to build complex queries programmatically.
//
// Example JSON:
//
//	{
//	  "where": [
//	    {"field": "id", "operator": "=", "param": "user_id"}
//	  ]
//	}
type DeleteSpec struct {
	Where []ConditionSpec `json:"where"`
}

// AggregateSpec represents an aggregate query (COUNT/SUM/AVG/MIN/MAX) in a serializable format.
// This can be unmarshaled from JSON to build complex queries programmatically.
//
// Example JSON for COUNT:
//
//	{
//	  "where": [
//	    {"field": "status", "operator": "=", "param": "active"}
//	  ]
//	}
//
// Example JSON for SUM/AVG/MIN/MAX:
//
//	{
//	  "field": "amount",
//	  "where": [
//	    {"field": "status", "operator": "=", "param": "paid"}
//	  ]
//	}
type AggregateSpec struct {
	Field string          `json:"field,omitempty"` // Required for SUM/AVG/MIN/MAX, not used for COUNT
	Where []ConditionSpec `json:"where,omitempty"`
}

// SetOperandSpec represents one operand in a compound query (UNION, INTERSECT, EXCEPT).
//
// Example JSON:
//
//	{"operation": "union", "query": {"fields": ["id", "name"], "where": [...]}}
type SetOperandSpec struct {
	Operation string    `json:"operation"` // "union", "union_all", "intersect", "intersect_all", "except", "except_all"
	Query     QuerySpec `json:"query"`
}

// CompoundQuerySpec represents a compound query with set operations in a serializable format.
// This can be unmarshaled from JSON to build complex queries programmatically.
//
// Example JSON:
//
//	{
//	  "base": {"fields": ["id", "name"], "where": [{"field": "status", "operator": "=", "param": "active"}]},
//	  "operands": [
//	    {"operation": "union", "query": {"fields": ["id", "name"], "where": [{"field": "status", "operator": "=", "param": "pending"}]}},
//	    {"operation": "except", "query": {"fields": ["id", "name"], "where": [{"field": "role", "operator": "=", "param": "admin"}]}}
//	  ],
//	  "order_by": [{"field": "name", "direction": "asc"}],
//	  "limit": 10
//	}
type CompoundQuerySpec struct {
	Base     QuerySpec        `json:"base"`               // First query
	Operands []SetOperandSpec `json:"operands"`           // Set operations and additional queries
	OrderBy  []OrderBySpec    `json:"order_by,omitempty"` // Final ORDER BY for the compound result
	Limit    *int             `json:"limit,omitempty"`
	Offset   *int             `json:"offset,omitempty"`
}

// -----------------------------------------------------------------------------
// Factory Export Specs
// -----------------------------------------------------------------------------

// FactorySpec provides a complete description of a factory's capabilities
// for introspection, documentation, and LLM tool definitions.
type FactorySpec struct {
	Table      string               `json:"table"`
	Schema     SchemaSpec           `json:"schema"`
	Queries    []QueryCapabilitySpec    `json:"queries"`
	Selects    []SelectCapabilitySpec   `json:"selects"`
	Updates    []UpdateCapabilitySpec   `json:"updates"`
	Deletes    []DeleteCapabilitySpec   `json:"deletes"`
	Aggregates []AggregateCapabilitySpec `json:"aggregates"`
}

// SchemaSpec describes the table schema derived from struct metadata.
type SchemaSpec struct {
	PrimaryKey string      `json:"primary_key"`
	Fields     []FieldSpec `json:"fields"`
}

// FieldSpec describes a single field in the schema.
type FieldSpec struct {
	Name        string   `json:"name"`
	Type        string   `json:"type"`
	Nullable    bool     `json:"nullable"`
	Constraints []string `json:"constraints,omitempty"`
}

// QueryCapabilitySpec is the JSON-serializable representation of a QueryCapability.
type QueryCapabilitySpec struct {
	Name        string      `json:"name"`
	Description string      `json:"description,omitempty"`
	Params      []ParamSpec `json:"params"`
	Tags        []string    `json:"tags,omitempty"`
	// Query modifiers for LLM visibility
	Limit      *int     `json:"limit,omitempty"`
	Offset     *int     `json:"offset,omitempty"`
	Distinct   bool     `json:"distinct,omitempty"`
	DistinctOn []string `json:"distinct_on,omitempty"`
	GroupBy    []string `json:"group_by,omitempty"`
	ForLocking string   `json:"for_locking,omitempty"`
}

// SelectCapabilitySpec is the JSON-serializable representation of a SelectCapability.
type SelectCapabilitySpec struct {
	Name        string      `json:"name"`
	Description string      `json:"description,omitempty"`
	Params      []ParamSpec `json:"params"`
	Tags        []string    `json:"tags,omitempty"`
	// Query modifiers for LLM visibility
	Limit      *int     `json:"limit,omitempty"`
	Offset     *int     `json:"offset,omitempty"`
	Distinct   bool     `json:"distinct,omitempty"`
	DistinctOn []string `json:"distinct_on,omitempty"`
	GroupBy    []string `json:"group_by,omitempty"`
	ForLocking string   `json:"for_locking,omitempty"`
}

// UpdateCapabilitySpec is the JSON-serializable representation of an UpdateCapability.
type UpdateCapabilitySpec struct {
	Name        string      `json:"name"`
	Description string      `json:"description,omitempty"`
	Params      []ParamSpec `json:"params"`
	Tags        []string    `json:"tags,omitempty"`
}

// DeleteCapabilitySpec is the JSON-serializable representation of a DeleteCapability.
type DeleteCapabilitySpec struct {
	Name        string      `json:"name"`
	Description string      `json:"description,omitempty"`
	Params      []ParamSpec `json:"params"`
	Tags        []string    `json:"tags,omitempty"`
}

// AggregateCapabilitySpec is the JSON-serializable representation of an AggregateCapability.
type AggregateCapabilitySpec struct {
	Name        string        `json:"name"`
	Description string        `json:"description,omitempty"`
	Func        AggregateFunc `json:"func"`
	Field       string        `json:"field,omitempty"`
	Params      []ParamSpec   `json:"params"`
	Tags        []string      `json:"tags,omitempty"`
}

// Spec returns a complete specification of the factory's capabilities.
func (f *Factory[T]) Spec() FactorySpec {
	f.mu.RLock()
	defer f.mu.RUnlock()

	spec := FactorySpec{
		Table:      f.cereal.TableName(),
		Schema:     f.buildSchemaSpec(),
		Queries:    make([]QueryCapabilitySpec, 0, len(f.queries)),
		Selects:    make([]SelectCapabilitySpec, 0, len(f.selects)),
		Updates:    make([]UpdateCapabilitySpec, 0, len(f.updates)),
		Deletes:    make([]DeleteCapabilitySpec, 0, len(f.deletes)),
		Aggregates: make([]AggregateCapabilitySpec, 0, len(f.aggregates)),
	}

	for _, cap := range f.queries {
		spec.Queries = append(spec.Queries, QueryCapabilitySpec{
			Name:        cap.Name,
			Description: cap.Description,
			Params:      cap.Params,
			Tags:        cap.Tags,
			Limit:       cap.Spec.Limit,
			Offset:      cap.Spec.Offset,
			Distinct:    cap.Spec.Distinct,
			DistinctOn:  cap.Spec.DistinctOn,
			GroupBy:     cap.Spec.GroupBy,
			ForLocking:  cap.Spec.ForLocking,
		})
	}
	sort.Slice(spec.Queries, func(i, j int) bool {
		return spec.Queries[i].Name < spec.Queries[j].Name
	})

	for _, cap := range f.selects {
		spec.Selects = append(spec.Selects, SelectCapabilitySpec{
			Name:        cap.Name,
			Description: cap.Description,
			Params:      cap.Params,
			Tags:        cap.Tags,
			Limit:       cap.Spec.Limit,
			Offset:      cap.Spec.Offset,
			Distinct:    cap.Spec.Distinct,
			DistinctOn:  cap.Spec.DistinctOn,
			GroupBy:     cap.Spec.GroupBy,
			ForLocking:  cap.Spec.ForLocking,
		})
	}
	sort.Slice(spec.Selects, func(i, j int) bool {
		return spec.Selects[i].Name < spec.Selects[j].Name
	})

	for _, cap := range f.updates {
		spec.Updates = append(spec.Updates, UpdateCapabilitySpec{
			Name:        cap.Name,
			Description: cap.Description,
			Params:      cap.Params,
			Tags:        cap.Tags,
		})
	}
	sort.Slice(spec.Updates, func(i, j int) bool {
		return spec.Updates[i].Name < spec.Updates[j].Name
	})

	for _, cap := range f.deletes {
		spec.Deletes = append(spec.Deletes, DeleteCapabilitySpec{
			Name:        cap.Name,
			Description: cap.Description,
			Params:      cap.Params,
			Tags:        cap.Tags,
		})
	}
	sort.Slice(spec.Deletes, func(i, j int) bool {
		return spec.Deletes[i].Name < spec.Deletes[j].Name
	})

	for _, cap := range f.aggregates {
		spec.Aggregates = append(spec.Aggregates, AggregateCapabilitySpec{
			Name:        cap.Name,
			Description: cap.Description,
			Func:        cap.Func,
			Field:       cap.Spec.Field,
			Params:      cap.Params,
			Tags:        cap.Tags,
		})
	}
	sort.Slice(spec.Aggregates, func(i, j int) bool {
		return spec.Aggregates[i].Name < spec.Aggregates[j].Name
	})

	return spec
}

// SpecJSON returns the factory specification as a JSON string.
// Suitable for LLM prompt injection or external tooling.
func (f *Factory[T]) SpecJSON() (string, error) {
	spec := f.Spec()
	data, err := json.MarshalIndent(spec, "", "  ")
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// buildSchemaSpec constructs the schema specification from cereal metadata.
func (f *Factory[T]) buildSchemaSpec() SchemaSpec {
	meta := f.cereal.Metadata()

	schema := SchemaSpec{
		PrimaryKey: f.primaryKey,
		Fields:     make([]FieldSpec, 0, len(meta.Fields)),
	}

	for _, field := range meta.Fields {
		dbCol := field.Tags["db"]
		if dbCol == "" || dbCol == "-" {
			continue
		}

		fieldSpec := FieldSpec{
			Name:     dbCol,
			Type:     field.Tags["type"],
			Nullable: len(field.Type) > 0 && field.Type[0] == '*',
		}

		if constraints := field.Tags["constraints"]; constraints != "" {
			fieldSpec.Constraints = parseConstraints(constraints)
		}

		schema.Fields = append(schema.Fields, fieldSpec)
	}

	return schema
}

// parseConstraints splits a comma-separated constraints string.
func parseConstraints(s string) []string {
	if s == "" {
		return nil
	}

	var constraints []string
	start := 0
	for i := 0; i <= len(s); i++ {
		if i == len(s) || s[i] == ',' {
			if start < i {
				constraints = append(constraints, s[start:i])
			}
			start = i + 1
		}
	}
	return constraints
}
