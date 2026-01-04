package edamame

import "github.com/google/uuid"

// ParamSpec describes a parameter required for statement execution.
type ParamSpec struct {
	Name        string `json:"name"`
	Type        string `json:"type"`
	Required    bool   `json:"required"`
	Default     any    `json:"default,omitempty"`
	Description string `json:"description,omitempty"`
}

// QueryStatement defines a SELECT query that returns multiple records.
// Statements are defined as package-level variables and passed directly to execution methods.
type QueryStatement struct {
	id          uuid.UUID
	name        string
	description string
	spec        QuerySpec
	params      []ParamSpec
	tags        []string
}

// NewQueryStatement creates a new QueryStatement with an auto-generated UUID.
// Parameters are derived from the spec automatically.
func NewQueryStatement(name, description string, spec QuerySpec, tags ...string) QueryStatement {
	return QueryStatement{
		id:          uuid.New(),
		name:        name,
		description: description,
		spec:        spec,
		params:      deriveQueryParams(spec),
		tags:        tags,
	}
}

// ID returns the statement's unique identifier.
func (s QueryStatement) ID() uuid.UUID { return s.id }

// Name returns the statement's name.
func (s QueryStatement) Name() string { return s.name }

// Description returns the statement's description.
func (s QueryStatement) Description() string { return s.description }

// Params returns the statement's parameter specifications.
func (s QueryStatement) Params() []ParamSpec { return s.params }

// Tags returns the statement's tags.
func (s QueryStatement) Tags() []string { return s.tags }

// SelectStatement defines a SELECT query that returns a single record.
// Statements are defined as package-level variables and passed directly to execution methods.
type SelectStatement struct {
	id          uuid.UUID
	name        string
	description string
	spec        SelectSpec
	params      []ParamSpec
	tags        []string
}

// NewSelectStatement creates a new SelectStatement with an auto-generated UUID.
// Parameters are derived from the spec automatically.
func NewSelectStatement(name, description string, spec SelectSpec, tags ...string) SelectStatement {
	return SelectStatement{
		id:          uuid.New(),
		name:        name,
		description: description,
		spec:        spec,
		params:      deriveSelectParams(spec),
		tags:        tags,
	}
}

// ID returns the statement's unique identifier.
func (s SelectStatement) ID() uuid.UUID { return s.id }

// Name returns the statement's name.
func (s SelectStatement) Name() string { return s.name }

// Description returns the statement's description.
func (s SelectStatement) Description() string { return s.description }

// Params returns the statement's parameter specifications.
func (s SelectStatement) Params() []ParamSpec { return s.params }

// Tags returns the statement's tags.
func (s SelectStatement) Tags() []string { return s.tags }

// UpdateStatement defines an UPDATE mutation.
// Statements are defined as package-level variables and passed directly to execution methods.
type UpdateStatement struct {
	id          uuid.UUID
	name        string
	description string
	spec        UpdateSpec
	params      []ParamSpec
	tags        []string
}

// NewUpdateStatement creates a new UpdateStatement with an auto-generated UUID.
// Parameters are derived from the spec automatically.
func NewUpdateStatement(name, description string, spec UpdateSpec, tags ...string) UpdateStatement {
	return UpdateStatement{
		id:          uuid.New(),
		name:        name,
		description: description,
		spec:        spec,
		params:      deriveUpdateParams(spec),
		tags:        tags,
	}
}

// ID returns the statement's unique identifier.
func (s UpdateStatement) ID() uuid.UUID { return s.id }

// Name returns the statement's name.
func (s UpdateStatement) Name() string { return s.name }

// Description returns the statement's description.
func (s UpdateStatement) Description() string { return s.description }

// Params returns the statement's parameter specifications.
func (s UpdateStatement) Params() []ParamSpec { return s.params }

// Tags returns the statement's tags.
func (s UpdateStatement) Tags() []string { return s.tags }

// DeleteStatement defines a DELETE mutation.
// Statements are defined as package-level variables and passed directly to execution methods.
type DeleteStatement struct {
	id          uuid.UUID
	name        string
	description string
	spec        DeleteSpec
	params      []ParamSpec
	tags        []string
}

// NewDeleteStatement creates a new DeleteStatement with an auto-generated UUID.
// Parameters are derived from the spec automatically.
func NewDeleteStatement(name, description string, spec DeleteSpec, tags ...string) DeleteStatement {
	return DeleteStatement{
		id:          uuid.New(),
		name:        name,
		description: description,
		spec:        spec,
		params:      deriveDeleteParams(spec),
		tags:        tags,
	}
}

// ID returns the statement's unique identifier.
func (s DeleteStatement) ID() uuid.UUID { return s.id }

// Name returns the statement's name.
func (s DeleteStatement) Name() string { return s.name }

// Description returns the statement's description.
func (s DeleteStatement) Description() string { return s.description }

// Params returns the statement's parameter specifications.
func (s DeleteStatement) Params() []ParamSpec { return s.params }

// Tags returns the statement's tags.
func (s DeleteStatement) Tags() []string { return s.tags }

// AggregateStatement defines an aggregate query (COUNT, SUM, AVG, MIN, MAX).
// Statements are defined as package-level variables and passed directly to execution methods.
type AggregateStatement struct {
	id          uuid.UUID
	name        string
	description string
	spec        AggregateSpec
	fn          AggregateFunc
	params      []ParamSpec
	tags        []string
}

// AggregateFunc represents the type of aggregate function.
type AggregateFunc string

const (
	AggCount AggregateFunc = "COUNT"
	AggSum   AggregateFunc = "SUM"
	AggAvg   AggregateFunc = "AVG"
	AggMin   AggregateFunc = "MIN"
	AggMax   AggregateFunc = "MAX"
)

// NewAggregateStatement creates a new AggregateStatement with an auto-generated UUID.
// Parameters are derived from the spec automatically.
func NewAggregateStatement(name, description string, fn AggregateFunc, spec AggregateSpec, tags ...string) AggregateStatement {
	return AggregateStatement{
		id:          uuid.New(),
		name:        name,
		description: description,
		spec:        spec,
		fn:          fn,
		params:      deriveAggregateParams(spec),
		tags:        tags,
	}
}

// ID returns the statement's unique identifier.
func (s AggregateStatement) ID() uuid.UUID { return s.id }

// Name returns the statement's name.
func (s AggregateStatement) Name() string { return s.name }

// Description returns the statement's description.
func (s AggregateStatement) Description() string { return s.description }

// Func returns the aggregate function type.
func (s AggregateStatement) Func() AggregateFunc { return s.fn }

// Params returns the statement's parameter specifications.
func (s AggregateStatement) Params() []ParamSpec { return s.params }

// Tags returns the statement's tags.
func (s AggregateStatement) Tags() []string { return s.tags }

// deriveQueryParams extracts params from all parts of a QuerySpec.
func deriveQueryParams(spec QuerySpec) []ParamSpec {
	seen := make(map[string]bool)
	params := make([]ParamSpec, 0)

	// WHERE conditions
	collectParams(spec.Where, seen, &params)

	// HAVING conditions
	collectParams(spec.Having, seen, &params)

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
				Type:     "any",
				Required: true,
			})
		}
	}

	// Parameterized limit/offset
	if spec.LimitParam != "" && !seen[spec.LimitParam] {
		seen[spec.LimitParam] = true
		params = append(params, ParamSpec{
			Name:     spec.LimitParam,
			Type:     "integer",
			Required: false,
		})
	}
	if spec.OffsetParam != "" && !seen[spec.OffsetParam] {
		seen[spec.OffsetParam] = true
		params = append(params, ParamSpec{
			Name:     spec.OffsetParam,
			Type:     "integer",
			Required: false,
		})
	}

	return params
}

// deriveSelectParams extracts params from all parts of a SelectSpec.
func deriveSelectParams(spec SelectSpec) []ParamSpec {
	seen := make(map[string]bool)
	params := make([]ParamSpec, 0)

	// WHERE conditions
	collectParams(spec.Where, seen, &params)

	// HAVING conditions
	collectParams(spec.Having, seen, &params)

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
				Type:     "any",
				Required: true,
			})
		}
	}

	// Parameterized limit/offset
	if spec.LimitParam != "" && !seen[spec.LimitParam] {
		seen[spec.LimitParam] = true
		params = append(params, ParamSpec{
			Name:     spec.LimitParam,
			Type:     "integer",
			Required: false,
		})
	}
	if spec.OffsetParam != "" && !seen[spec.OffsetParam] {
		seen[spec.OffsetParam] = true
		params = append(params, ParamSpec{
			Name:     spec.OffsetParam,
			Type:     "integer",
			Required: false,
		})
	}

	return params
}

// deriveUpdateParams extracts params from both SET and WHERE clauses.
func deriveUpdateParams(spec UpdateSpec) []ParamSpec {
	seen := make(map[string]bool)
	params := make([]ParamSpec, 0)

	// SET params
	for _, param := range spec.Set {
		if seen[param] {
			continue
		}
		seen[param] = true
		params = append(params, ParamSpec{
			Name:     param,
			Type:     "any",
			Required: true,
		})
	}

	// WHERE params
	collectParams(spec.Where, seen, &params)

	return params
}

// deriveDeleteParams extracts params from WHERE conditions.
func deriveDeleteParams(spec DeleteSpec) []ParamSpec {
	seen := make(map[string]bool)
	params := make([]ParamSpec, 0)
	collectParams(spec.Where, seen, &params)
	return params
}

// deriveAggregateParams extracts params from WHERE conditions.
func deriveAggregateParams(spec AggregateSpec) []ParamSpec {
	seen := make(map[string]bool)
	params := make([]ParamSpec, 0)
	collectParams(spec.Where, seen, &params)
	return params
}

// collectParams recursively collects params from conditions, including nested groups.
func collectParams(conditions []ConditionSpec, seen map[string]bool, params *[]ParamSpec) {
	for i := range conditions {
		// Handle condition groups (AND/OR)
		if conditions[i].IsGroup() {
			collectParams(conditions[i].Group, seen, params)
			continue
		}

		// BETWEEN conditions
		if conditions[i].IsBetween() || conditions[i].IsNotBetween() {
			if conditions[i].LowParam != "" && !seen[conditions[i].LowParam] {
				seen[conditions[i].LowParam] = true
				*params = append(*params, ParamSpec{
					Name:     conditions[i].LowParam,
					Type:     "any",
					Required: true,
				})
			}
			if conditions[i].HighParam != "" && !seen[conditions[i].HighParam] {
				seen[conditions[i].HighParam] = true
				*params = append(*params, ParamSpec{
					Name:     conditions[i].HighParam,
					Type:     "any",
					Required: true,
				})
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
			Type:     "any",
			Required: true,
		})
	}
}
