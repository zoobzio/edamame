package edamame

// ParamSpec describes a parameter required for capability execution.
type ParamSpec struct {
	Name        string `json:"name"`
	Type        string `json:"type"`
	Required    bool   `json:"required"`
	Default     any    `json:"default,omitempty"`
	Description string `json:"description,omitempty"`
}

// QueryCapability defines a named SELECT query that returns multiple records.
type QueryCapability struct {
	Name        string
	Description string
	Spec        QuerySpec
	Params      []ParamSpec
	Tags        []string
}

// SelectCapability defines a named SELECT query that returns a single record.
type SelectCapability struct {
	Name        string
	Description string
	Spec        SelectSpec
	Params      []ParamSpec
	Tags        []string
}

// UpdateCapability defines a named UPDATE mutation.
type UpdateCapability struct {
	Name        string
	Description string
	Spec        UpdateSpec
	Params      []ParamSpec
	Tags        []string
}

// DeleteCapability defines a named DELETE mutation.
type DeleteCapability struct {
	Name        string
	Description string
	Spec        DeleteSpec
	Params      []ParamSpec
	Tags        []string
}

// AggregateCapability defines a named aggregate query (COUNT, SUM, AVG, MIN, MAX).
type AggregateCapability struct {
	Name        string
	Description string
	Spec        AggregateSpec
	Func        AggregateFunc
	Params      []ParamSpec
	Tags        []string
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
