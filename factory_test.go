package edamame

import (
	"strings"
	"testing"

	"github.com/zoobzio/astql/pkg/postgres"
)

// User is a test model.
type User struct {
	ID    int    `db:"id" type:"integer" constraints:"primarykey"`
	Email string `db:"email" type:"text" constraints:"notnull,unique"`
	Name  string `db:"name" type:"text"`
	Age   *int   `db:"age" type:"integer"`
}

func TestNew(t *testing.T) {
	// nil db is allowed for query building without execution
	factory, err := New[User](nil, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	if factory.TableName() != "users" {
		t.Errorf("TableName() = %q, want %q", factory.TableName(), "users")
	}

	if factory.Cereal() == nil {
		t.Error("Cereal() returned nil")
	}
}

func TestNew_EmptyTableName(t *testing.T) {
	_, err := New[User](nil, "", postgres.New())
	if err == nil {
		t.Error("New() with empty table name should fail")
	}
}

func TestDefaultCRUDCapabilities(t *testing.T) {
	factory, err := New[User](nil, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	// Check default capabilities exist
	tests := []struct {
		name   string
		exists func(string) bool
	}{
		{"select", factory.HasSelect},
		{"query", factory.HasQuery},
		{"delete", factory.HasDelete},
		{"count", factory.HasAggregate},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !tt.exists(tt.name) {
				t.Errorf("default capability %q not registered", tt.name)
			}
		})
	}

	// Verify no default update capability exists
	if factory.HasUpdate("update") {
		t.Error("default 'update' capability should not exist")
	}
}

func TestSelectCapability(t *testing.T) {
	factory, err := New[User](nil, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	c, ok := factory.GetSelect("select")
	if !ok {
		t.Fatal("GetSelect('select') returned false")
	}

	if c.Name != "select" {
		t.Errorf("Name = %q, want %q", c.Name, "select")
	}

	if len(c.Spec.Where) != 1 {
		t.Fatalf("Spec.Where has %d conditions, want 1", len(c.Spec.Where))
	}

	if c.Spec.Where[0].Field != "id" {
		t.Errorf("Where[0].Field = %q, want %q", c.Spec.Where[0].Field, "id")
	}

	if len(c.Params) != 1 {
		t.Fatalf("Params has %d entries, want 1", len(c.Params))
	}

	if c.Params[0].Name != "id" {
		t.Errorf("Params[0].Name = %q, want %q", c.Params[0].Name, "id")
	}

	if !c.Params[0].Required {
		t.Error("Params[0].Required = false, want true")
	}
}

func TestQueryCapability(t *testing.T) {
	factory, err := New[User](nil, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	c, ok := factory.GetQuery("query")
	if !ok {
		t.Fatal("GetQuery('query') returned false")
	}

	if c.Name != "query" {
		t.Errorf("Name = %q, want %q", c.Name, "query")
	}

	// Default query has no conditions
	if len(c.Spec.Where) != 0 {
		t.Errorf("Spec.Where has %d conditions, want 0", len(c.Spec.Where))
	}

	if len(c.Params) != 0 {
		t.Errorf("Params has %d entries, want 0", len(c.Params))
	}
}

func TestAddQuery(t *testing.T) {
	factory, err := New[User](nil, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	factory.AddQuery(QueryCapability{
		Name:        "active-users",
		Description: "Find active users",
		Spec: QuerySpec{
			Where: []ConditionSpec{
				{Field: "status", Operator: "=", Param: "status"},
			},
		},
	})

	if !factory.HasQuery("active-users") {
		t.Error("HasQuery('active-users') = false after AddQuery")
	}

	c, ok := factory.GetQuery("active-users")
	if !ok {
		t.Fatal("GetQuery('active-users') returned false")
	}

	if c.Description != "Find active users" {
		t.Errorf("Description = %q, want %q", c.Description, "Find active users")
	}

	// Params should be auto-derived
	if len(c.Params) != 1 {
		t.Fatalf("Params has %d entries, want 1", len(c.Params))
	}

	if c.Params[0].Name != "status" {
		t.Errorf("Params[0].Name = %q, want %q", c.Params[0].Name, "status")
	}
}

func TestAddSelect(t *testing.T) {
	factory, err := New[User](nil, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	factory.AddSelect(SelectCapability{
		Name: "by-email",
		Spec: SelectSpec{
			Where: []ConditionSpec{
				{Field: "email", Operator: "=", Param: "email"},
			},
		},
	})

	if !factory.HasSelect("by-email") {
		t.Error("HasSelect('by-email') = false after AddSelect")
	}

	c, _ := factory.GetSelect("by-email")
	if len(c.Params) != 1 || c.Params[0].Name != "email" {
		t.Error("Params not correctly derived for by-email capability")
	}
}

func TestAddUpdate(t *testing.T) {
	factory, err := New[User](nil, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	factory.AddUpdate(UpdateCapability{
		Name: "update-name",
		Spec: UpdateSpec{
			Set: map[string]string{"name": "new_name"},
			Where: []ConditionSpec{
				{Field: "id", Operator: "=", Param: "id"},
			},
		},
	})

	if !factory.HasUpdate("update-name") {
		t.Error("HasUpdate('update-name') = false after AddUpdate")
	}

	c, _ := factory.GetUpdate("update-name")
	// Should have params for both SET and WHERE
	if len(c.Params) != 2 {
		t.Errorf("Params has %d entries, want 2", len(c.Params))
	}
}

func TestAddDelete(t *testing.T) {
	factory, err := New[User](nil, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	factory.AddDelete(DeleteCapability{
		Name: "delete-by-email",
		Spec: DeleteSpec{
			Where: []ConditionSpec{
				{Field: "email", Operator: "=", Param: "email"},
			},
		},
	})

	if !factory.HasDelete("delete-by-email") {
		t.Error("HasDelete('delete-by-email') = false after AddDelete")
	}
}

func TestAddAggregate(t *testing.T) {
	factory, err := New[User](nil, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	factory.AddAggregate(AggregateCapability{
		Name: "sum-ages",
		Func: AggSum,
		Spec: AggregateSpec{
			Field: "age",
		},
	})

	if !factory.HasAggregate("sum-ages") {
		t.Error("HasAggregate('sum-ages') = false after AddAggregate")
	}

	c, _ := factory.GetAggregate("sum-ages")
	if c.Func != AggSum {
		t.Errorf("Func = %q, want %q", c.Func, AggSum)
	}
}

func TestRemoveCapabilities(t *testing.T) {
	factory, err := New[User](nil, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	// Add and remove query
	factory.AddQuery(QueryCapability{Name: "temp"})
	if !factory.RemoveQuery("temp") {
		t.Error("RemoveQuery('temp') = false")
	}
	if factory.HasQuery("temp") {
		t.Error("HasQuery('temp') = true after removal")
	}

	// Remove non-existent
	if factory.RemoveQuery("nonexistent") {
		t.Error("RemoveQuery('nonexistent') = true for non-existent capability")
	}
}

func TestListCapabilities(t *testing.T) {
	factory, err := New[User](nil, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	queries := factory.ListQueries()
	if len(queries) != 1 || queries[0] != "query" {
		t.Errorf("ListQueries() = %v, want [query]", queries)
	}

	selects := factory.ListSelects()
	if len(selects) != 1 || selects[0] != "select" {
		t.Errorf("ListSelects() = %v, want [select]", selects)
	}

	updates := factory.ListUpdates()
	if len(updates) != 0 {
		t.Errorf("ListUpdates() = %v, want []", updates)
	}

	deletes := factory.ListDeletes()
	if len(deletes) != 1 || deletes[0] != "delete" {
		t.Errorf("ListDeletes() = %v, want [delete]", deletes)
	}

	aggregates := factory.ListAggregates()
	if len(aggregates) != 1 || aggregates[0] != "count" {
		t.Errorf("ListAggregates() = %v, want [count]", aggregates)
	}
}

func TestConditionGroupsInCapability(t *testing.T) {
	factory, err := New[User](nil, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	// Add a query with OR condition group
	factory.AddQuery(QueryCapability{
		Name:        "active-or-pending",
		Description: "Find users that are active or pending",
		Spec: QuerySpec{
			Where: []ConditionSpec{
				{
					Logic: "OR",
					Group: []ConditionSpec{
						{Field: "name", Operator: "=", Param: "name1"},
						{Field: "name", Operator: "=", Param: "name2"},
					},
				},
			},
		},
	})

	c, ok := factory.GetQuery("active-or-pending")
	if !ok {
		t.Fatal("GetQuery('active-or-pending') returned false")
	}

	// Verify the spec has the OR group
	if len(c.Spec.Where) != 1 {
		t.Fatalf("Spec.Where has %d conditions, want 1", len(c.Spec.Where))
	}

	if c.Spec.Where[0].Logic != "OR" {
		t.Errorf("Where[0].Logic = %q, want %q", c.Spec.Where[0].Logic, "OR")
	}

	if len(c.Spec.Where[0].Group) != 2 {
		t.Errorf("Where[0].Group has %d conditions, want 2", len(c.Spec.Where[0].Group))
	}

	// Params should be derived from nested conditions
	if len(c.Params) != 2 {
		t.Errorf("Params has %d entries, want 2 (from nested OR group)", len(c.Params))
	}
}

func TestMixedConditionsInCapability(t *testing.T) {
	factory, err := New[User](nil, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	// Add a query with simple condition + OR group
	factory.AddQuery(QueryCapability{
		Name: "complex-query",
		Spec: QuerySpec{
			Where: []ConditionSpec{
				{Field: "age", Operator: ">=", Param: "min_age"},
				{
					Logic: "OR",
					Group: []ConditionSpec{
						{Field: "name", Operator: "=", Param: "name1"},
						{Field: "name", Operator: "=", Param: "name2"},
					},
				},
			},
		},
	})

	c, ok := factory.GetQuery("complex-query")
	if !ok {
		t.Fatal("GetQuery('complex-query') returned false")
	}

	// Should have params: min_age, name1, name2
	if len(c.Params) != 3 {
		t.Errorf("Params has %d entries, want 3", len(c.Params))
	}

	// Verify param names
	paramNames := make(map[string]bool)
	for _, p := range c.Params {
		paramNames[p.Name] = true
	}

	expectedParams := []string{"min_age", "name1", "name2"}
	for _, expected := range expectedParams {
		if !paramNames[expected] {
			t.Errorf("Expected param %q not found", expected)
		}
	}
}

func TestOrderByExpressionInCapability(t *testing.T) {
	factory, err := New[User](nil, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	// Add a query with expression-based ORDER BY (vector similarity)
	factory.AddQuery(QueryCapability{
		Name:        "similar-users",
		Description: "Find users similar to a given embedding",
		Spec: QuerySpec{
			OrderBy: []OrderBySpec{
				{
					Field:     "embedding",
					Operator:  "<->",
					Param:     "query_vec",
					Direction: "asc",
				},
			},
			Limit: intPtr(10),
		},
	})

	c, ok := factory.GetQuery("similar-users")
	if !ok {
		t.Fatal("GetQuery('similar-users') returned false")
	}

	// Verify the spec has vector ORDER BY
	if len(c.Spec.OrderBy) != 1 {
		t.Fatalf("Spec.OrderBy has %d entries, want 1", len(c.Spec.OrderBy))
	}

	orderBy := c.Spec.OrderBy[0]
	if orderBy.Field != "embedding" {
		t.Errorf("OrderBy.Field = %q, want %q", orderBy.Field, "embedding")
	}
	if orderBy.Operator != "<->" {
		t.Errorf("OrderBy.Operator = %q, want %q", orderBy.Operator, "<->")
	}
	if orderBy.Param != "query_vec" {
		t.Errorf("OrderBy.Param = %q, want %q", orderBy.Param, "query_vec")
	}
	if orderBy.Direction != "asc" {
		t.Errorf("OrderBy.Direction = %q, want %q", orderBy.Direction, "asc")
	}
}

func TestNestedConditionGroupsInUpdateAndDelete(t *testing.T) {
	factory, err := New[User](nil, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	// Add update with OR group
	factory.AddUpdate(UpdateCapability{
		Name: "update-multiple",
		Spec: UpdateSpec{
			Set: map[string]string{"name": "new_name"},
			Where: []ConditionSpec{
				{
					Logic: "OR",
					Group: []ConditionSpec{
						{Field: "id", Operator: "=", Param: "id1"},
						{Field: "id", Operator: "=", Param: "id2"},
					},
				},
			},
		},
	})

	updateCap, ok := factory.GetUpdate("update-multiple")
	if !ok {
		t.Fatal("GetUpdate('update-multiple') returned false")
	}

	// Should have params: new_name, id1, id2
	if len(updateCap.Params) != 3 {
		t.Errorf("Update Params has %d entries, want 3", len(updateCap.Params))
	}

	// Add delete with OR group
	factory.AddDelete(DeleteCapability{
		Name: "delete-multiple",
		Spec: DeleteSpec{
			Where: []ConditionSpec{
				{
					Logic: "OR",
					Group: []ConditionSpec{
						{Field: "id", Operator: "=", Param: "id1"},
						{Field: "id", Operator: "=", Param: "id2"},
					},
				},
			},
		},
	})

	deleteCap, ok := factory.GetDelete("delete-multiple")
	if !ok {
		t.Fatal("GetDelete('delete-multiple') returned false")
	}

	// Should have params: id1, id2
	if len(deleteCap.Params) != 2 {
		t.Errorf("Delete Params has %d entries, want 2", len(deleteCap.Params))
	}
}

// UserNoPK is a test model without a primary key constraint.
type UserNoPK struct {
	ID    int    `db:"id" type:"integer"`
	Email string `db:"email" type:"text"`
	Name  string `db:"name" type:"text"`
}

func TestNew_MissingPrimaryKey(t *testing.T) {
	_, err := New[UserNoPK](nil, "users", postgres.New())
	if err == nil {
		t.Error("New() should fail when struct has no primary key constraint")
	}

	// Verify error message is helpful
	if err != nil && !strings.Contains(err.Error(), "primary key") {
		t.Errorf("error message should mention 'primary key': %v", err)
	}
}

func TestInsertFromSpec_InvalidConflictAction(t *testing.T) {
	factory, err := New[User](nil, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	// Conflict columns specified but action is invalid
	spec := CreateSpec{
		OnConflict:     []string{"email"},
		ConflictAction: "invalid_action",
	}

	_, err = factory.insertFromSpec(spec)
	if err == nil {
		t.Error("insertFromSpec() should fail with invalid conflict action")
	}
}

func TestInsertFromSpec_MissingConflictAction(t *testing.T) {
	factory, err := New[User](nil, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	// Conflict columns specified but no action
	spec := CreateSpec{
		OnConflict: []string{"email"},
		// ConflictAction not specified
	}

	_, err = factory.insertFromSpec(spec)
	if err == nil {
		t.Error("insertFromSpec() should fail when conflict columns specified without action")
	}
}

func TestSelectFromSpec_InvalidLockMode(t *testing.T) {
	factory, err := New[User](nil, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	spec := SelectSpec{
		Where:      []ConditionSpec{{Field: "id", Operator: "=", Param: "id"}},
		ForLocking: "invalid_lock",
	}

	_, err = factory.selectFromSpec(spec)
	if err == nil {
		t.Error("selectFromSpec() should fail with invalid lock mode")
	}
}

// -----------------------------------------------------------------------------
// Depth Limit Tests
// -----------------------------------------------------------------------------

func TestMaxConditionDepth_Default(t *testing.T) {
	factory, err := New[User](nil, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	if factory.MaxConditionDepth() != DefaultMaxConditionDepth {
		t.Errorf("MaxConditionDepth() = %d, want %d", factory.MaxConditionDepth(), DefaultMaxConditionDepth)
	}
}

func TestMaxConditionDepth_Configurable(t *testing.T) {
	factory, err := New[User](nil, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	factory.SetMaxConditionDepth(5)
	if factory.MaxConditionDepth() != 5 {
		t.Errorf("MaxConditionDepth() = %d, want 5", factory.MaxConditionDepth())
	}

	factory.SetMaxConditionDepth(0)
	if factory.MaxConditionDepth() != 0 {
		t.Errorf("MaxConditionDepth() = %d, want 0 (disabled)", factory.MaxConditionDepth())
	}
}

func TestAddQuery_DepthExceeded(t *testing.T) {
	factory, err := New[User](nil, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	// Set a low depth limit
	factory.SetMaxConditionDepth(2)

	// Create a deeply nested condition (depth 3)
	deepCondition := QueryCapability{
		Name: "deep-query",
		Spec: QuerySpec{
			Where: []ConditionSpec{
				{
					Logic: "AND",
					Group: []ConditionSpec{
						{
							Logic: "OR",
							Group: []ConditionSpec{
								{
									Logic: "AND",
									Group: []ConditionSpec{
										{Field: "name", Operator: "=", Param: "name"},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	err = factory.AddQuery(deepCondition)
	if err == nil {
		t.Error("AddQuery() should fail when condition depth exceeds maximum")
	}

	if !strings.Contains(err.Error(), "maximum condition depth exceeded") {
		t.Errorf("error should mention 'maximum condition depth exceeded', got: %v", err)
	}
}

func TestAddQuery_DepthWithinLimit(t *testing.T) {
	factory, err := New[User](nil, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	// Set depth limit to 3
	factory.SetMaxConditionDepth(3)

	// Create a nested condition (depth 2, within limit)
	nestedCondition := QueryCapability{
		Name: "nested-query",
		Spec: QuerySpec{
			Where: []ConditionSpec{
				{
					Logic: "AND",
					Group: []ConditionSpec{
						{
							Logic: "OR",
							Group: []ConditionSpec{
								{Field: "name", Operator: "=", Param: "name"},
							},
						},
					},
				},
			},
		},
	}

	err = factory.AddQuery(nestedCondition)
	if err != nil {
		t.Errorf("AddQuery() should succeed when depth is within limit: %v", err)
	}

	if !factory.HasQuery("nested-query") {
		t.Error("nested-query should be registered")
	}
}

func TestAddQuery_DepthCheckDisabled(t *testing.T) {
	factory, err := New[User](nil, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	// Disable depth checking
	factory.SetMaxConditionDepth(0)

	// Create a very deeply nested condition
	deepCondition := QueryCapability{
		Name: "very-deep-query",
		Spec: QuerySpec{
			Where: []ConditionSpec{
				{
					Logic: "AND",
					Group: []ConditionSpec{
						{
							Logic: "OR",
							Group: []ConditionSpec{
								{
									Logic: "AND",
									Group: []ConditionSpec{
										{
											Logic: "OR",
											Group: []ConditionSpec{
												{Field: "name", Operator: "=", Param: "name"},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	err = factory.AddQuery(deepCondition)
	if err != nil {
		t.Errorf("AddQuery() should succeed when depth checking is disabled: %v", err)
	}
}

func TestAddSelect_DepthExceeded(t *testing.T) {
	factory, err := New[User](nil, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	factory.SetMaxConditionDepth(1)

	deepSelect := SelectCapability{
		Name: "deep-select",
		Spec: SelectSpec{
			Where: []ConditionSpec{
				{
					Logic: "AND",
					Group: []ConditionSpec{
						{
							Logic: "OR",
							Group: []ConditionSpec{
								{Field: "id", Operator: "=", Param: "id"},
							},
						},
					},
				},
			},
		},
	}

	err = factory.AddSelect(deepSelect)
	if err == nil {
		t.Error("AddSelect() should fail when condition depth exceeds maximum")
	}
}

func TestAddUpdate_DepthExceeded(t *testing.T) {
	factory, err := New[User](nil, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	factory.SetMaxConditionDepth(1)

	deepUpdate := UpdateCapability{
		Name: "deep-update",
		Spec: UpdateSpec{
			Set: map[string]string{"name": "new_name"},
			Where: []ConditionSpec{
				{
					Logic: "AND",
					Group: []ConditionSpec{
						{
							Logic: "OR",
							Group: []ConditionSpec{
								{Field: "id", Operator: "=", Param: "id"},
							},
						},
					},
				},
			},
		},
	}

	err = factory.AddUpdate(deepUpdate)
	if err == nil {
		t.Error("AddUpdate() should fail when condition depth exceeds maximum")
	}
}

func TestAddDelete_DepthExceeded(t *testing.T) {
	factory, err := New[User](nil, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	factory.SetMaxConditionDepth(1)

	deepDelete := DeleteCapability{
		Name: "deep-delete",
		Spec: DeleteSpec{
			Where: []ConditionSpec{
				{
					Logic: "AND",
					Group: []ConditionSpec{
						{
							Logic: "OR",
							Group: []ConditionSpec{
								{Field: "id", Operator: "=", Param: "id"},
							},
						},
					},
				},
			},
		},
	}

	err = factory.AddDelete(deepDelete)
	if err == nil {
		t.Error("AddDelete() should fail when condition depth exceeds maximum")
	}
}

func TestAddAggregate_DepthExceeded(t *testing.T) {
	factory, err := New[User](nil, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	factory.SetMaxConditionDepth(1)

	deepAggregate := AggregateCapability{
		Name: "deep-aggregate",
		Func: AggCount,
		Spec: AggregateSpec{
			Where: []ConditionSpec{
				{
					Logic: "AND",
					Group: []ConditionSpec{
						{
							Logic: "OR",
							Group: []ConditionSpec{
								{Field: "id", Operator: "=", Param: "id"},
							},
						},
					},
				},
			},
		},
	}

	err = factory.AddAggregate(deepAggregate)
	if err == nil {
		t.Error("AddAggregate() should fail when condition depth exceeds maximum")
	}
}

// -----------------------------------------------------------------------------
// SQL Cache Tests
// -----------------------------------------------------------------------------

func TestRenderQuery_Caching(t *testing.T) {
	factory, err := New[User](nil, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	// First render should populate cache
	sql1, err := factory.RenderQuery("query")
	if err != nil {
		t.Fatalf("RenderQuery() failed: %v", err)
	}

	// Second render should return cached value
	sql2, err := factory.RenderQuery("query")
	if err != nil {
		t.Fatalf("RenderQuery() failed on second call: %v", err)
	}

	if sql1 != sql2 {
		t.Errorf("cached SQL should match: %q != %q", sql1, sql2)
	}
}

func TestRenderSelect_Caching(t *testing.T) {
	factory, err := New[User](nil, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	sql1, err := factory.RenderSelect("select")
	if err != nil {
		t.Fatalf("RenderSelect() failed: %v", err)
	}

	sql2, err := factory.RenderSelect("select")
	if err != nil {
		t.Fatalf("RenderSelect() failed on second call: %v", err)
	}

	if sql1 != sql2 {
		t.Errorf("cached SQL should match: %q != %q", sql1, sql2)
	}
}

func TestCache_InvalidatedOnAdd(t *testing.T) {
	factory, err := New[User](nil, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	// Render to populate cache
	sql1, err := factory.RenderQuery("query")
	if err != nil {
		t.Fatalf("RenderQuery() failed: %v", err)
	}

	// Re-add the same capability with different spec
	err = factory.AddQuery(QueryCapability{
		Name: "query",
		Spec: QuerySpec{
			Where: []ConditionSpec{
				{Field: "name", Operator: "=", Param: "name"},
			},
		},
	})
	if err != nil {
		t.Fatalf("AddQuery() failed: %v", err)
	}

	// Render again - should get new SQL
	sql2, err := factory.RenderQuery("query")
	if err != nil {
		t.Fatalf("RenderQuery() failed after re-add: %v", err)
	}

	if sql1 == sql2 {
		t.Error("SQL should differ after capability was re-added with different spec")
	}
}

func TestCache_InvalidatedOnRemove(t *testing.T) {
	factory, err := New[User](nil, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	// Add a custom query
	err = factory.AddQuery(QueryCapability{
		Name: "custom",
		Spec: QuerySpec{
			Where: []ConditionSpec{
				{Field: "name", Operator: "=", Param: "name"},
			},
		},
	})
	if err != nil {
		t.Fatalf("AddQuery() failed: %v", err)
	}

	// Render to populate cache
	_, err = factory.RenderQuery("custom")
	if err != nil {
		t.Fatalf("RenderQuery() failed: %v", err)
	}

	// Remove the capability
	factory.RemoveQuery("custom")

	// Render should now fail
	_, err = factory.RenderQuery("custom")
	if err == nil {
		t.Error("RenderQuery() should fail after capability was removed")
	}
}
