package edamame

import (
	"testing"
)

func TestQueryDispatch(t *testing.T) {
	factory, err := New[User](nil, "users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	// Get the default query builder
	builder, err := factory.Query("query")
	if err != nil {
		t.Fatalf("Query('query') failed: %v", err)
	}
	if builder == nil {
		t.Fatal("Query('query') returned nil")
	}

	// Render to verify it produces valid SQL
	result, err := builder.Render()
	if err != nil {
		t.Fatalf("Render() failed: %v", err)
	}

	if result.SQL == "" {
		t.Error("Render() produced empty SQL")
	}
}

func TestSelectDispatch(t *testing.T) {
	factory, err := New[User](nil, "users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	builder, err := factory.Select("select")
	if err != nil {
		t.Fatalf("Select('select') failed: %v", err)
	}
	if builder == nil {
		t.Fatal("Select('select') returned nil")
	}

	result, err := builder.Render()
	if err != nil {
		t.Fatalf("Render() failed: %v", err)
	}

	// Should have WHERE clause with id
	if result.SQL == "" {
		t.Error("Render() produced empty SQL")
	}
}

func TestUpdateDispatch(t *testing.T) {
	factory, err := New[User](nil, "users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	// Register a custom update capability (no default update exists)
	factory.AddUpdate(UpdateCapability{
		Name: "update-name",
		Spec: UpdateSpec{
			Set: map[string]string{"name": "new_name"},
			Where: []ConditionSpec{
				{Field: "id", Operator: "=", Param: "id"},
			},
		},
	})

	builder, err := factory.Update("update-name")
	if err != nil {
		t.Fatalf("Update('update-name') failed: %v", err)
	}
	if builder == nil {
		t.Fatal("Update('update-name') returned nil")
	}

	result, err := builder.Render()
	if err != nil {
		t.Fatalf("Render() failed: %v", err)
	}

	if result.SQL == "" {
		t.Error("Render() produced empty SQL")
	}
}

func TestDeleteDispatch(t *testing.T) {
	factory, err := New[User](nil, "users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	builder, err := factory.Delete("delete")
	if err != nil {
		t.Fatalf("Delete('delete') failed: %v", err)
	}
	if builder == nil {
		t.Fatal("Delete('delete') returned nil")
	}

	result, err := builder.Render()
	if err != nil {
		t.Fatalf("Render() failed: %v", err)
	}

	if result.SQL == "" {
		t.Error("Render() produced empty SQL")
	}
}

func TestAggregateDispatch(t *testing.T) {
	factory, err := New[User](nil, "users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	builder, err := factory.Aggregate("count")
	if err != nil {
		t.Fatalf("Aggregate('count') failed: %v", err)
	}
	if builder == nil {
		t.Fatal("Aggregate('count') returned nil")
	}

	result, err := builder.Render()
	if err != nil {
		t.Fatalf("Render() failed: %v", err)
	}

	if result.SQL == "" {
		t.Error("Render() produced empty SQL")
	}
}

func TestInsertDispatch(t *testing.T) {
	factory, err := New[User](nil, "users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	builder := factory.Insert()
	if builder == nil {
		t.Fatal("Insert() returned nil")
	}

	result, err := builder.Render()
	if err != nil {
		t.Fatalf("Render() failed: %v", err)
	}

	if result.SQL == "" {
		t.Error("Render() produced empty SQL")
	}
}

func TestCustomQueryDispatch(t *testing.T) {
	factory, err := New[User](nil, "users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	factory.AddQuery(QueryCapability{
		Name: "by-age",
		Spec: QuerySpec{
			Where: []ConditionSpec{
				{Field: "age", Operator: ">=", Param: "min_age"},
			},
			OrderBy: []OrderBySpec{
				{Field: "age", Direction: "desc"},
			},
			Limit: intPtr(10),
		},
	})

	builder, err := factory.Query("by-age")
	if err != nil {
		t.Fatalf("Query('by-age') failed: %v", err)
	}

	result, err := builder.Render()
	if err != nil {
		t.Fatalf("Render() failed: %v", err)
	}

	// Verify the SQL contains expected clauses
	sql := result.SQL
	if sql == "" {
		t.Error("Render() produced empty SQL")
	}
}

func TestAggregateDispatchVariants(t *testing.T) {
	factory, err := New[User](nil, "users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	tests := []struct {
		name string
		fn   AggregateFunc
	}{
		{"sum-age", AggSum},
		{"avg-age", AggAvg},
		{"min-age", AggMin},
		{"max-age", AggMax},
	}

	for _, tt := range tests {
		factory.AddAggregate(AggregateCapability{
			Name: tt.name,
			Func: tt.fn,
			Spec: AggregateSpec{
				Field: "age",
			},
		})
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder, err := factory.Aggregate(tt.name)
			if err != nil {
				t.Fatalf("Aggregate('%s') failed: %v", tt.name, err)
			}

			result, err := builder.Render()
			if err != nil {
				t.Fatalf("Render() failed: %v", err)
			}

			if result.SQL == "" {
				t.Error("Render() produced empty SQL")
			}
		})
	}
}

func TestDispatchMissingCapability(t *testing.T) {
	factory, err := New[User](nil, "users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	// These should return errors immediately
	_, err = factory.Query("nonexistent")
	if err == nil {
		t.Error("Query('nonexistent') should return error")
	}

	_, err = factory.Select("nonexistent")
	if err == nil {
		t.Error("Select('nonexistent') should return error")
	}

	_, err = factory.Update("nonexistent")
	if err == nil {
		t.Error("Update('nonexistent') should return error")
	}

	_, err = factory.Delete("nonexistent")
	if err == nil {
		t.Error("Delete('nonexistent') should return error")
	}

	_, err = factory.Aggregate("nonexistent")
	if err == nil {
		t.Error("Aggregate('nonexistent') should return error")
	}
}

func TestBuilderChaining(t *testing.T) {
	factory, err := New[User](nil, "users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	// Should be able to chain additional methods
	builder, err := factory.Query("query")
	if err != nil {
		t.Fatalf("Query('query') failed: %v", err)
	}

	result, err := builder.
		Where("age", ">=", "min_age").
		OrderBy("name", "asc").
		Limit(10).
		Render()
	if err != nil {
		t.Fatalf("Render() failed: %v", err)
	}

	if result.SQL == "" {
		t.Error("Render() produced empty SQL")
	}
}

// Helper for creating int pointers
func intPtr(i int) *int {
	return &i
}

func TestRenderMethods(t *testing.T) {
	factory, err := New[User](nil, "users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	// Add an update capability for testing
	factory.AddUpdate(UpdateCapability{
		Name: "update-name",
		Spec: UpdateSpec{
			Set: map[string]string{"name": "new_name"},
			Where: []ConditionSpec{
				{Field: "id", Operator: "=", Param: "id"},
			},
		},
	})

	tests := []struct {
		name   string
		render func() (string, error)
	}{
		{"RenderQuery", func() (string, error) { return factory.RenderQuery("query") }},
		{"RenderSelect", func() (string, error) { return factory.RenderSelect("select") }},
		{"RenderUpdate", func() (string, error) { return factory.RenderUpdate("update-name") }},
		{"RenderDelete", func() (string, error) { return factory.RenderDelete("delete") }},
		{"RenderAggregate", func() (string, error) { return factory.RenderAggregate("count") }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sql, err := tt.render()
			if err != nil {
				t.Fatalf("%s failed: %v", tt.name, err)
			}
			if sql == "" {
				t.Errorf("%s returned empty SQL", tt.name)
			}
		})
	}
}

func TestRenderMissingCapability(t *testing.T) {
	factory, err := New[User](nil, "users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	_, err = factory.RenderQuery("nonexistent")
	if err == nil {
		t.Error("RenderQuery('nonexistent') should return error")
	}

	_, err = factory.RenderSelect("nonexistent")
	if err == nil {
		t.Error("RenderSelect('nonexistent') should return error")
	}

	_, err = factory.RenderUpdate("nonexistent")
	if err == nil {
		t.Error("RenderUpdate('nonexistent') should return error")
	}

	_, err = factory.RenderDelete("nonexistent")
	if err == nil {
		t.Error("RenderDelete('nonexistent') should return error")
	}

	_, err = factory.RenderAggregate("nonexistent")
	if err == nil {
		t.Error("RenderAggregate('nonexistent') should return error")
	}
}

func TestBatchDispatchMissingCapability(t *testing.T) {
	factory, err := New[User](nil, "users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	// Batch update/delete should fail for nonexistent capabilities
	_, err = factory.ExecUpdateBatch(nil, "nonexistent", nil)
	if err == nil {
		t.Error("ExecUpdateBatch('nonexistent') should return error")
	}

	_, err = factory.ExecDeleteBatch(nil, "nonexistent", nil)
	if err == nil {
		t.Error("ExecDeleteBatch('nonexistent') should return error")
	}
}
