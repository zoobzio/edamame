package edamame

import (
	"encoding/json"
	"testing"

	"github.com/zoobzio/astql/pkg/postgres"
)

func TestSpec(t *testing.T) {
	factory, err := New[User](nil, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	spec := factory.Spec()

	if spec.Table != "users" {
		t.Errorf("Table = %q, want %q", spec.Table, "users")
	}

	// Check schema
	if spec.Schema.PrimaryKey != "id" {
		t.Errorf("Schema.PrimaryKey = %q, want %q", spec.Schema.PrimaryKey, "id")
	}

	if len(spec.Schema.Fields) != 4 {
		t.Errorf("Schema.Fields has %d entries, want 4", len(spec.Schema.Fields))
	}

	// Check default capabilities are present
	if len(spec.Selects) != 1 {
		t.Errorf("Selects has %d entries, want 1", len(spec.Selects))
	}

	if len(spec.Queries) != 1 {
		t.Errorf("Queries has %d entries, want 1", len(spec.Queries))
	}

	if len(spec.Updates) != 0 {
		t.Errorf("Updates has %d entries, want 0", len(spec.Updates))
	}

	if len(spec.Deletes) != 1 {
		t.Errorf("Deletes has %d entries, want 1", len(spec.Deletes))
	}

	if len(spec.Aggregates) != 1 {
		t.Errorf("Aggregates has %d entries, want 1", len(spec.Aggregates))
	}
}

func TestSpecSchema(t *testing.T) {
	factory, err := New[User](nil, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	spec := factory.Spec()

	// Find specific fields
	fieldMap := make(map[string]FieldSpec)
	for _, f := range spec.Schema.Fields {
		fieldMap[f.Name] = f
	}

	// Check id field
	id, ok := fieldMap["id"]
	if !ok {
		t.Fatal("id field not found in schema")
	}
	if id.Type != "integer" {
		t.Errorf("id.Type = %q, want %q", id.Type, "integer")
	}
	if id.Nullable {
		t.Error("id.Nullable = true, want false")
	}
	if len(id.Constraints) != 1 || id.Constraints[0] != "primarykey" {
		t.Errorf("id.Constraints = %v, want [primarykey]", id.Constraints)
	}

	// Check age field (nullable)
	age, ok := fieldMap["age"]
	if !ok {
		t.Fatal("age field not found in schema")
	}
	if !age.Nullable {
		t.Error("age.Nullable = false, want true")
	}
}

func TestSpecJSON(t *testing.T) {
	factory, err := New[User](nil, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	jsonStr, err := factory.SpecJSON()
	if err != nil {
		t.Fatalf("SpecJSON() failed: %v", err)
	}

	// Verify it's valid JSON
	var parsed FactorySpec
	if err := json.Unmarshal([]byte(jsonStr), &parsed); err != nil {
		t.Fatalf("SpecJSON() produced invalid JSON: %v", err)
	}

	if parsed.Table != "users" {
		t.Errorf("parsed.Table = %q, want %q", parsed.Table, "users")
	}
}

func TestSpecWithCustomCapabilities(t *testing.T) {
	factory, err := New[User](nil, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	factory.AddQuery(QueryCapability{
		Name:        "active-users",
		Description: "Find active users",
		Tags:        []string{"custom", "active"},
		Spec: QuerySpec{
			Where: []ConditionSpec{
				{Field: "status", Operator: "=", Param: "status"},
			},
		},
	})

	factory.AddAggregate(AggregateCapability{
		Name: "sum-ages",
		Func: AggSum,
		Spec: AggregateSpec{
			Field: "age",
		},
	})

	spec := factory.Spec()

	// Should have 2 queries now
	if len(spec.Queries) != 2 {
		t.Errorf("Queries has %d entries, want 2", len(spec.Queries))
	}

	// Should have 2 aggregates now
	if len(spec.Aggregates) != 2 {
		t.Errorf("Aggregates has %d entries, want 2", len(spec.Aggregates))
	}

	// Find the custom query
	var activeQuery *QueryCapabilitySpec
	for i := range spec.Queries {
		if spec.Queries[i].Name == "active-users" {
			activeQuery = &spec.Queries[i]
			break
		}
	}

	if activeQuery == nil {
		t.Fatal("active-users query not found in spec")
	}

	if activeQuery.Description != "Find active users" {
		t.Errorf("Description = %q, want %q", activeQuery.Description, "Find active users")
	}

	if len(activeQuery.Tags) != 2 {
		t.Errorf("Tags has %d entries, want 2", len(activeQuery.Tags))
	}

	if len(activeQuery.Params) != 1 || activeQuery.Params[0].Name != "status" {
		t.Error("Params not correctly included in spec")
	}

	// Find the sum aggregate
	var sumAgg *AggregateCapabilitySpec
	for i := range spec.Aggregates {
		if spec.Aggregates[i].Name == "sum-ages" {
			sumAgg = &spec.Aggregates[i]
			break
		}
	}

	if sumAgg == nil {
		t.Fatal("sum-ages aggregate not found in spec")
	}

	if sumAgg.Func != AggSum {
		t.Errorf("Func = %q, want %q", sumAgg.Func, AggSum)
	}

	if sumAgg.Field != "age" {
		t.Errorf("Field = %q, want %q", sumAgg.Field, "age")
	}
}

func TestSpecSorting(t *testing.T) {
	factory, err := New[User](nil, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	// Add queries in reverse alphabetical order
	factory.AddQuery(QueryCapability{Name: "zebra"})
	factory.AddQuery(QueryCapability{Name: "alpha"})
	factory.AddQuery(QueryCapability{Name: "middle"})

	spec := factory.Spec()

	// Should be sorted alphabetically
	if len(spec.Queries) != 4 {
		t.Fatalf("Queries has %d entries, want 4", len(spec.Queries))
	}

	expected := []string{"alpha", "middle", "query", "zebra"}
	for i, name := range expected {
		if spec.Queries[i].Name != name {
			t.Errorf("Queries[%d].Name = %q, want %q", i, spec.Queries[i].Name, name)
		}
	}
}

func TestParseConstraints(t *testing.T) {
	tests := []struct {
		input    string
		expected []string
	}{
		{"", nil},
		{"primarykey", []string{"primarykey"}},
		{"notnull,unique", []string{"notnull", "unique"}},
		{"a,b,c", []string{"a", "b", "c"}},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := parseConstraints(tt.input)
			if len(result) != len(tt.expected) {
				t.Errorf("parseConstraints(%q) = %v, want %v", tt.input, result, tt.expected)
				return
			}
			for i := range result {
				if result[i] != tt.expected[i] {
					t.Errorf("parseConstraints(%q)[%d] = %q, want %q", tt.input, i, result[i], tt.expected[i])
				}
			}
		})
	}
}

func TestSpecQueryModifiers(t *testing.T) {
	factory, err := New[User](nil, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	limit := 10
	offset := 20
	factory.AddQuery(QueryCapability{
		Name: "paginated",
		Spec: QuerySpec{
			Limit:      &limit,
			Offset:     &offset,
			Distinct:   true,
			DistinctOn: []string{"email"},
			GroupBy:    []string{"status"},
			ForLocking: "update",
		},
	})

	spec := factory.Spec()

	var paginated *QueryCapabilitySpec
	for i := range spec.Queries {
		if spec.Queries[i].Name == "paginated" {
			paginated = &spec.Queries[i]
			break
		}
	}

	if paginated == nil {
		t.Fatal("paginated query not found in spec")
	}

	if paginated.Limit == nil || *paginated.Limit != 10 {
		t.Errorf("Limit = %v, want 10", paginated.Limit)
	}

	if paginated.Offset == nil || *paginated.Offset != 20 {
		t.Errorf("Offset = %v, want 20", paginated.Offset)
	}

	if !paginated.Distinct {
		t.Error("Distinct = false, want true")
	}

	if len(paginated.DistinctOn) != 1 || paginated.DistinctOn[0] != "email" {
		t.Errorf("DistinctOn = %v, want [email]", paginated.DistinctOn)
	}

	if len(paginated.GroupBy) != 1 || paginated.GroupBy[0] != "status" {
		t.Errorf("GroupBy = %v, want [status]", paginated.GroupBy)
	}

	if paginated.ForLocking != "update" {
		t.Errorf("ForLocking = %q, want %q", paginated.ForLocking, "update")
	}
}

func TestSpecSelectModifiers(t *testing.T) {
	factory, err := New[User](nil, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	limit := 1
	factory.AddSelect(SelectCapability{
		Name: "first-distinct",
		Spec: SelectSpec{
			Where: []ConditionSpec{
				{Field: "status", Operator: "=", Param: "status"},
			},
			Limit:      &limit,
			Distinct:   true,
			ForLocking: "share",
		},
	})

	spec := factory.Spec()

	var firstDistinct *SelectCapabilitySpec
	for i := range spec.Selects {
		if spec.Selects[i].Name == "first-distinct" {
			firstDistinct = &spec.Selects[i]
			break
		}
	}

	if firstDistinct == nil {
		t.Fatal("first-distinct select not found in spec")
	}

	if firstDistinct.Limit == nil || *firstDistinct.Limit != 1 {
		t.Errorf("Limit = %v, want 1", firstDistinct.Limit)
	}

	if !firstDistinct.Distinct {
		t.Error("Distinct = false, want true")
	}

	if firstDistinct.ForLocking != "share" {
		t.Errorf("ForLocking = %q, want %q", firstDistinct.ForLocking, "share")
	}
}

func TestParamDerivationFromHavingAgg(t *testing.T) {
	factory, err := New[User](nil, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	factory.AddQuery(QueryCapability{
		Name: "grouped-with-having",
		Spec: QuerySpec{
			GroupBy: []string{"status"},
			HavingAgg: []HavingAggSpec{
				{Func: "count", Operator: ">", Param: "min_count"},
				{Func: "sum", Field: "age", Operator: ">=", Param: "min_total_age"},
			},
		},
	})

	c, ok := factory.GetQuery("grouped-with-having")
	if !ok {
		t.Fatal("grouped-with-having query not found")
	}

	// Should have derived 2 params from HavingAgg
	if len(c.Params) != 2 {
		t.Fatalf("Params has %d entries, want 2", len(c.Params))
	}

	paramNames := make(map[string]bool)
	for _, p := range c.Params {
		paramNames[p.Name] = true
	}

	if !paramNames["min_count"] {
		t.Error("min_count param not derived from HavingAgg")
	}

	if !paramNames["min_total_age"] {
		t.Error("min_total_age param not derived from HavingAgg")
	}
}

func TestParamDerivationFromOrderByExpression(t *testing.T) {
	factory, err := New[User](nil, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	factory.AddQuery(QueryCapability{
		Name: "vector-search",
		Spec: QuerySpec{
			OrderBy: []OrderBySpec{
				{Field: "embedding", Operator: "<->", Param: "query_vec", Direction: "asc"},
			},
		},
	})

	c, ok := factory.GetQuery("vector-search")
	if !ok {
		t.Fatal("vector-search query not found")
	}

	// Should have derived 1 param from OrderBy expression
	if len(c.Params) != 1 {
		t.Fatalf("Params has %d entries, want 1", len(c.Params))
	}

	if c.Params[0].Name != "query_vec" {
		t.Errorf("Param name = %q, want %q", c.Params[0].Name, "query_vec")
	}
}

func TestParamDerivationCombined(t *testing.T) {
	factory, err := New[User](nil, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	factory.AddQuery(QueryCapability{
		Name: "complex-query",
		Spec: QuerySpec{
			Where: []ConditionSpec{
				{Field: "status", Operator: "=", Param: "status"},
			},
			Having: []ConditionSpec{
				{Field: "age", Operator: ">", Param: "min_age"},
			},
			HavingAgg: []HavingAggSpec{
				{Func: "count", Operator: ">", Param: "min_count"},
			},
			OrderBy: []OrderBySpec{
				{Field: "embedding", Operator: "<->", Param: "query_vec", Direction: "asc"},
			},
		},
	})

	c, ok := factory.GetQuery("complex-query")
	if !ok {
		t.Fatal("complex-query query not found")
	}

	// Should have 4 params: status, min_age, min_count, query_vec
	if len(c.Params) != 4 {
		t.Fatalf("Params has %d entries, want 4", len(c.Params))
	}

	paramNames := make(map[string]bool)
	for _, p := range c.Params {
		paramNames[p.Name] = true
	}

	expected := []string{"status", "min_age", "min_count", "query_vec"}
	for _, name := range expected {
		if !paramNames[name] {
			t.Errorf("%s param not derived", name)
		}
	}
}

func TestSoyAccessor(t *testing.T) {
	factory, err := New[User](nil, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	c := factory.Soy()
	if c == nil {
		t.Fatal("Soy() returned nil")
	}

	if c.TableName() != "users" {
		t.Errorf("Soy().TableName() = %q, want %q", c.TableName(), "users")
	}
}

// -----------------------------------------------------------------------------
// Query Spec Helper Method Tests
// -----------------------------------------------------------------------------

func TestConditionSpecIsGroup(t *testing.T) {
	tests := []struct {
		name     string
		spec     ConditionSpec
		expected bool
	}{
		{
			name:     "simple condition",
			spec:     ConditionSpec{Field: "age", Operator: ">=", Param: "min_age"},
			expected: false,
		},
		{
			name:     "null condition",
			spec:     ConditionSpec{Field: "email", IsNull: true},
			expected: false,
		},
		{
			name: "OR group",
			spec: ConditionSpec{
				Logic: "OR",
				Group: []ConditionSpec{
					{Field: "status", Operator: "=", Param: "active"},
					{Field: "status", Operator: "=", Param: "pending"},
				},
			},
			expected: true,
		},
		{
			name: "AND group",
			spec: ConditionSpec{
				Logic: "AND",
				Group: []ConditionSpec{
					{Field: "age", Operator: ">=", Param: "min_age"},
					{Field: "age", Operator: "<=", Param: "max_age"},
				},
			},
			expected: true,
		},
		{
			name:     "logic without group",
			spec:     ConditionSpec{Logic: "OR"},
			expected: false,
		},
		{
			name:     "group without logic",
			spec:     ConditionSpec{Group: []ConditionSpec{{Field: "x"}}},
			expected: false,
		},
		{
			name:     "empty group with logic",
			spec:     ConditionSpec{Logic: "OR", Group: []ConditionSpec{}},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.spec.IsGroup(); got != tt.expected {
				t.Errorf("IsGroup() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestOrderBySpecHasNulls(t *testing.T) {
	tests := []struct {
		name     string
		spec     OrderBySpec
		expected bool
	}{
		{
			name:     "simple order by",
			spec:     OrderBySpec{Field: "name", Direction: "asc"},
			expected: false,
		},
		{
			name:     "nulls first",
			spec:     OrderBySpec{Field: "name", Direction: "asc", Nulls: "first"},
			expected: true,
		},
		{
			name:     "nulls last",
			spec:     OrderBySpec{Field: "name", Direction: "desc", Nulls: "last"},
			expected: true,
		},
		{
			name:     "empty nulls",
			spec:     OrderBySpec{Field: "name", Direction: "asc", Nulls: ""},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.spec.HasNulls(); got != tt.expected {
				t.Errorf("HasNulls() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestOrderBySpecIsExpression(t *testing.T) {
	tests := []struct {
		name     string
		spec     OrderBySpec
		expected bool
	}{
		{
			name:     "simple order by",
			spec:     OrderBySpec{Field: "name", Direction: "asc"},
			expected: false,
		},
		{
			name:     "vector distance expression",
			spec:     OrderBySpec{Field: "embedding", Operator: "<->", Param: "query_vec", Direction: "asc"},
			expected: true,
		},
		{
			name:     "operator without param",
			spec:     OrderBySpec{Field: "embedding", Operator: "<->", Direction: "asc"},
			expected: false,
		},
		{
			name:     "param without operator",
			spec:     OrderBySpec{Field: "embedding", Param: "query_vec", Direction: "asc"},
			expected: false,
		},
		{
			name:     "inner product expression",
			spec:     OrderBySpec{Field: "embedding", Operator: "<#>", Param: "query_vec", Direction: "asc"},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.spec.IsExpression(); got != tt.expected {
				t.Errorf("IsExpression() = %v, want %v", got, tt.expected)
			}
		})
	}
}
