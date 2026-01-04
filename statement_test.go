package edamame

import (
	"testing"

	"github.com/google/uuid"
)

func TestQueryStatement_Accessors(t *testing.T) {
	stmt := NewQueryStatement("test-query", "Test query description", QuerySpec{
		Where: []ConditionSpec{{Field: "status", Operator: "=", Param: "status"}},
	}, "tag1", "tag2")

	// Test ID is a valid UUID
	if stmt.ID() == uuid.Nil {
		t.Error("ID() should return a non-nil UUID")
	}

	// Test Name
	if stmt.Name() != "test-query" {
		t.Errorf("Name() = %q, want %q", stmt.Name(), "test-query")
	}

	// Test Description
	if stmt.Description() != "Test query description" {
		t.Errorf("Description() = %q, want %q", stmt.Description(), "Test query description")
	}

	// Test Params
	params := stmt.Params()
	if len(params) != 1 {
		t.Fatalf("Params() len = %d, want 1", len(params))
	}
	if params[0].Name != "status" {
		t.Errorf("Params()[0].Name = %q, want %q", params[0].Name, "status")
	}

	// Test Tags
	tags := stmt.Tags()
	if len(tags) != 2 {
		t.Fatalf("Tags() len = %d, want 2", len(tags))
	}
	if tags[0] != "tag1" || tags[1] != "tag2" {
		t.Errorf("Tags() = %v, want [tag1 tag2]", tags)
	}
}

func TestSelectStatement_Accessors(t *testing.T) {
	stmt := NewSelectStatement("test-select", "Test select description", SelectSpec{
		Where: []ConditionSpec{{Field: "id", Operator: "=", Param: "id"}},
	}, "select-tag")

	if stmt.ID() == uuid.Nil {
		t.Error("ID() should return a non-nil UUID")
	}

	if stmt.Name() != "test-select" {
		t.Errorf("Name() = %q, want %q", stmt.Name(), "test-select")
	}

	if stmt.Description() != "Test select description" {
		t.Errorf("Description() = %q, want %q", stmt.Description(), "Test select description")
	}

	params := stmt.Params()
	if len(params) != 1 || params[0].Name != "id" {
		t.Errorf("Params() = %v, want [{id ...}]", params)
	}

	tags := stmt.Tags()
	if len(tags) != 1 || tags[0] != "select-tag" {
		t.Errorf("Tags() = %v, want [select-tag]", tags)
	}
}

func TestUpdateStatement_Accessors(t *testing.T) {
	stmt := NewUpdateStatement("test-update", "Test update description", UpdateSpec{
		Set:   map[string]string{"name": "new_name"},
		Where: []ConditionSpec{{Field: "id", Operator: "=", Param: "id"}},
	})

	if stmt.ID() == uuid.Nil {
		t.Error("ID() should return a non-nil UUID")
	}

	if stmt.Name() != "test-update" {
		t.Errorf("Name() = %q, want %q", stmt.Name(), "test-update")
	}

	if stmt.Description() != "Test update description" {
		t.Errorf("Description() = %q, want %q", stmt.Description(), "Test update description")
	}

	params := stmt.Params()
	// Should have both "new_name" (from Set) and "id" (from Where)
	if len(params) != 2 {
		t.Errorf("Params() len = %d, want 2", len(params))
	}

	// Tags should be empty when not provided
	if len(stmt.Tags()) != 0 {
		t.Errorf("Tags() len = %d, want 0", len(stmt.Tags()))
	}
}

func TestDeleteStatement_Accessors(t *testing.T) {
	stmt := NewDeleteStatement("test-delete", "Test delete description", DeleteSpec{
		Where: []ConditionSpec{{Field: "id", Operator: "=", Param: "id"}},
	}, "delete-tag")

	if stmt.ID() == uuid.Nil {
		t.Error("ID() should return a non-nil UUID")
	}

	if stmt.Name() != "test-delete" {
		t.Errorf("Name() = %q, want %q", stmt.Name(), "test-delete")
	}

	if stmt.Description() != "Test delete description" {
		t.Errorf("Description() = %q, want %q", stmt.Description(), "Test delete description")
	}

	params := stmt.Params()
	if len(params) != 1 || params[0].Name != "id" {
		t.Errorf("Params() = %v, want [{id ...}]", params)
	}

	tags := stmt.Tags()
	if len(tags) != 1 || tags[0] != "delete-tag" {
		t.Errorf("Tags() = %v, want [delete-tag]", tags)
	}
}

func TestAggregateStatement_Accessors(t *testing.T) {
	stmt := NewAggregateStatement("test-count", "Test count description", AggCount, AggregateSpec{
		Where: []ConditionSpec{{Field: "active", Operator: "=", Param: "active"}},
	}, "agg-tag")

	if stmt.ID() == uuid.Nil {
		t.Error("ID() should return a non-nil UUID")
	}

	if stmt.Name() != "test-count" {
		t.Errorf("Name() = %q, want %q", stmt.Name(), "test-count")
	}

	if stmt.Description() != "Test count description" {
		t.Errorf("Description() = %q, want %q", stmt.Description(), "Test count description")
	}

	if stmt.Func() != AggCount {
		t.Errorf("Func() = %v, want %v", stmt.Func(), AggCount)
	}

	params := stmt.Params()
	if len(params) != 1 || params[0].Name != "active" {
		t.Errorf("Params() = %v, want [{active ...}]", params)
	}

	tags := stmt.Tags()
	if len(tags) != 1 || tags[0] != "agg-tag" {
		t.Errorf("Tags() = %v, want [agg-tag]", tags)
	}
}

func TestStatement_UniqueIDs(t *testing.T) {
	// Each statement should have a unique ID
	stmt1 := NewQueryStatement("query1", "Query 1", QuerySpec{})
	stmt2 := NewQueryStatement("query1", "Query 1", QuerySpec{}) // Same name/spec

	if stmt1.ID() == stmt2.ID() {
		t.Error("different statement instances should have different IDs")
	}
}

func TestQueryStatement_ParamDerivation_Complex(t *testing.T) {
	limit := 10
	stmt := NewQueryStatement("complex", "Complex query", QuerySpec{
		Where: []ConditionSpec{
			{Field: "age", Operator: ">=", Param: "min_age"},
			{Field: "age", Operator: "<=", Param: "max_age"},
			{
				Logic: "OR",
				Group: []ConditionSpec{
					{Field: "role", Operator: "=", Param: "role1"},
					{Field: "role", Operator: "=", Param: "role2"},
				},
			},
		},
		OrderBy: []OrderBySpec{
			{Field: "name", Operator: "<->", Param: "search_vector"},
		},
		LimitParam:  "page_size",
		OffsetParam: "offset",
		Limit:       &limit,
	})

	params := stmt.Params()

	// Should have: min_age, max_age, role1, role2, search_vector, page_size, offset
	expectedParams := map[string]bool{
		"min_age":       false,
		"max_age":       false,
		"role1":         false,
		"role2":         false,
		"search_vector": false,
		"page_size":     false,
		"offset":        false,
	}

	for _, p := range params {
		if _, ok := expectedParams[p.Name]; ok {
			expectedParams[p.Name] = true
		}
	}

	for name, found := range expectedParams {
		if !found {
			t.Errorf("expected param %q not found", name)
		}
	}
}

func TestQueryStatement_ParamDerivation_HavingAgg(t *testing.T) {
	stmt := NewQueryStatement("grouped", "Grouped query", QuerySpec{
		GroupBy: []string{"role"},
		HavingAgg: []HavingAggSpec{
			{Func: "count", Field: "*", Operator: ">=", Param: "min_count"},
			{Func: "sum", Field: "balance", Operator: ">", Param: "min_balance"},
		},
	})

	params := stmt.Params()

	hasMinCount := false
	hasMinBalance := false
	for _, p := range params {
		if p.Name == "min_count" {
			hasMinCount = true
		}
		if p.Name == "min_balance" {
			hasMinBalance = true
		}
	}

	if !hasMinCount {
		t.Error("expected param 'min_count' from HavingAgg")
	}
	if !hasMinBalance {
		t.Error("expected param 'min_balance' from HavingAgg")
	}
}

func TestQueryStatement_ParamDerivation_Between(t *testing.T) {
	stmt := NewQueryStatement("between", "Between query", QuerySpec{
		Where: []ConditionSpec{
			{Field: "age", Between: true, LowParam: "min_age", HighParam: "max_age"},
			{Field: "date", NotBetween: true, LowParam: "start_date", HighParam: "end_date"},
		},
	})

	params := stmt.Params()

	expectedParams := map[string]bool{
		"min_age":    false,
		"max_age":    false,
		"start_date": false,
		"end_date":   false,
	}

	for _, p := range params {
		if _, ok := expectedParams[p.Name]; ok {
			expectedParams[p.Name] = true
		}
	}

	for name, found := range expectedParams {
		if !found {
			t.Errorf("expected param %q not found in BETWEEN conditions", name)
		}
	}
}

func TestSelectStatement_ParamDerivation(t *testing.T) {
	stmt := NewSelectStatement("select-complex", "Complex select", SelectSpec{
		Where: []ConditionSpec{
			{Field: "id", Operator: "=", Param: "id"},
		},
		LimitParam:  "limit",
		OffsetParam: "offset",
	})

	params := stmt.Params()

	expectedParams := map[string]bool{
		"id":     false,
		"limit":  false,
		"offset": false,
	}

	for _, p := range params {
		if _, ok := expectedParams[p.Name]; ok {
			expectedParams[p.Name] = true
		}
	}

	for name, found := range expectedParams {
		if !found {
			t.Errorf("expected param %q not found", name)
		}
	}
}
