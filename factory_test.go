package edamame

import (
	"strings"
	"testing"
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
	factory, err := New[User](nil, "users")
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
	_, err := New[User](nil, "")
	if err == nil {
		t.Error("New() with empty table name should fail")
	}
}

func TestDefaultCRUDCapabilities(t *testing.T) {
	factory, err := New[User](nil, "users")
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
	factory, err := New[User](nil, "users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	cap, ok := factory.GetSelect("select")
	if !ok {
		t.Fatal("GetSelect('select') returned false")
	}

	if cap.Name != "select" {
		t.Errorf("Name = %q, want %q", cap.Name, "select")
	}

	if len(cap.Spec.Where) != 1 {
		t.Fatalf("Spec.Where has %d conditions, want 1", len(cap.Spec.Where))
	}

	if cap.Spec.Where[0].Field != "id" {
		t.Errorf("Where[0].Field = %q, want %q", cap.Spec.Where[0].Field, "id")
	}

	if len(cap.Params) != 1 {
		t.Fatalf("Params has %d entries, want 1", len(cap.Params))
	}

	if cap.Params[0].Name != "id" {
		t.Errorf("Params[0].Name = %q, want %q", cap.Params[0].Name, "id")
	}

	if !cap.Params[0].Required {
		t.Error("Params[0].Required = false, want true")
	}
}

func TestQueryCapability(t *testing.T) {
	factory, err := New[User](nil, "users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	cap, ok := factory.GetQuery("query")
	if !ok {
		t.Fatal("GetQuery('query') returned false")
	}

	if cap.Name != "query" {
		t.Errorf("Name = %q, want %q", cap.Name, "query")
	}

	// Default query has no conditions
	if len(cap.Spec.Where) != 0 {
		t.Errorf("Spec.Where has %d conditions, want 0", len(cap.Spec.Where))
	}

	if len(cap.Params) != 0 {
		t.Errorf("Params has %d entries, want 0", len(cap.Params))
	}
}

func TestAddQuery(t *testing.T) {
	factory, err := New[User](nil, "users")
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

	cap, ok := factory.GetQuery("active-users")
	if !ok {
		t.Fatal("GetQuery('active-users') returned false")
	}

	if cap.Description != "Find active users" {
		t.Errorf("Description = %q, want %q", cap.Description, "Find active users")
	}

	// Params should be auto-derived
	if len(cap.Params) != 1 {
		t.Fatalf("Params has %d entries, want 1", len(cap.Params))
	}

	if cap.Params[0].Name != "status" {
		t.Errorf("Params[0].Name = %q, want %q", cap.Params[0].Name, "status")
	}
}

func TestAddSelect(t *testing.T) {
	factory, err := New[User](nil, "users")
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

	cap, _ := factory.GetSelect("by-email")
	if len(cap.Params) != 1 || cap.Params[0].Name != "email" {
		t.Error("Params not correctly derived for by-email capability")
	}
}

func TestAddUpdate(t *testing.T) {
	factory, err := New[User](nil, "users")
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

	cap, _ := factory.GetUpdate("update-name")
	// Should have params for both SET and WHERE
	if len(cap.Params) != 2 {
		t.Errorf("Params has %d entries, want 2", len(cap.Params))
	}
}

func TestAddDelete(t *testing.T) {
	factory, err := New[User](nil, "users")
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
	factory, err := New[User](nil, "users")
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

	cap, _ := factory.GetAggregate("sum-ages")
	if cap.Func != AggSum {
		t.Errorf("Func = %q, want %q", cap.Func, AggSum)
	}
}

func TestRemoveCapabilities(t *testing.T) {
	factory, err := New[User](nil, "users")
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
	factory, err := New[User](nil, "users")
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
	factory, err := New[User](nil, "users")
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

	cap, ok := factory.GetQuery("active-or-pending")
	if !ok {
		t.Fatal("GetQuery('active-or-pending') returned false")
	}

	// Verify the spec has the OR group
	if len(cap.Spec.Where) != 1 {
		t.Fatalf("Spec.Where has %d conditions, want 1", len(cap.Spec.Where))
	}

	if cap.Spec.Where[0].Logic != "OR" {
		t.Errorf("Where[0].Logic = %q, want %q", cap.Spec.Where[0].Logic, "OR")
	}

	if len(cap.Spec.Where[0].Group) != 2 {
		t.Errorf("Where[0].Group has %d conditions, want 2", len(cap.Spec.Where[0].Group))
	}

	// Params should be derived from nested conditions
	if len(cap.Params) != 2 {
		t.Errorf("Params has %d entries, want 2 (from nested OR group)", len(cap.Params))
	}
}

func TestMixedConditionsInCapability(t *testing.T) {
	factory, err := New[User](nil, "users")
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

	cap, ok := factory.GetQuery("complex-query")
	if !ok {
		t.Fatal("GetQuery('complex-query') returned false")
	}

	// Should have params: min_age, name1, name2
	if len(cap.Params) != 3 {
		t.Errorf("Params has %d entries, want 3", len(cap.Params))
	}

	// Verify param names
	paramNames := make(map[string]bool)
	for _, p := range cap.Params {
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
	factory, err := New[User](nil, "users")
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

	cap, ok := factory.GetQuery("similar-users")
	if !ok {
		t.Fatal("GetQuery('similar-users') returned false")
	}

	// Verify the spec has vector ORDER BY
	if len(cap.Spec.OrderBy) != 1 {
		t.Fatalf("Spec.OrderBy has %d entries, want 1", len(cap.Spec.OrderBy))
	}

	orderBy := cap.Spec.OrderBy[0]
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
	factory, err := New[User](nil, "users")
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
	_, err := New[UserNoPK](nil, "users")
	if err == nil {
		t.Error("New() should fail when struct has no primary key constraint")
	}

	// Verify error message is helpful
	if err != nil && !strings.Contains(err.Error(), "primary key") {
		t.Errorf("error message should mention 'primary key': %v", err)
	}
}

func TestInsertFromSpec_InvalidConflictAction(t *testing.T) {
	factory, err := New[User](nil, "users")
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
	factory, err := New[User](nil, "users")
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
	factory, err := New[User](nil, "users")
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
