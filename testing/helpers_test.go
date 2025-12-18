package testing

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/zoobzio/capitan"
	"github.com/zoobzio/edamame"
)

func TestQueryCapture(t *testing.T) {
	capture := NewQueryCapture()

	capture.CaptureQuery("users-by-age", "query", "SELECT * FROM users WHERE age >= $1", map[string]any{"min_age": 18})
	capture.CaptureQuery("get-user", "select", "SELECT * FROM users WHERE id = $1", map[string]any{"id": 123})

	if capture.Count() != 2 {
		t.Errorf("expected 2 queries, got %d", capture.Count())
	}

	queries := capture.Queries()
	if len(queries) != 2 {
		t.Fatalf("expected 2 queries, got %d", len(queries))
	}

	if queries[0].Capability != "users-by-age" {
		t.Errorf("expected capability 'users-by-age', got %q", queries[0].Capability)
	}
	if queries[0].Type != "query" {
		t.Errorf("expected type 'query', got %q", queries[0].Type)
	}
}

func TestQueryCaptureReset(t *testing.T) {
	capture := NewQueryCapture()

	capture.CaptureQuery("test", "query", "SELECT 1", nil)

	if capture.Count() != 1 {
		t.Errorf("expected 1 query, got %d", capture.Count())
	}

	capture.Reset()

	if capture.Count() != 0 {
		t.Errorf("expected 0 queries after reset, got %d", capture.Count())
	}
}

func TestQueryCaptureLast(t *testing.T) {
	capture := NewQueryCapture()

	if capture.Last() != nil {
		t.Error("expected nil for empty capture")
	}

	capture.CaptureQuery("first", "query", "SELECT 1", nil)
	capture.CaptureQuery("second", "select", "SELECT 2", nil)

	last := capture.Last()
	if last == nil {
		t.Fatal("expected non-nil last query")
	}
	if last.Capability != "second" {
		t.Errorf("expected 'second', got %q", last.Capability)
	}
}

func TestQueryCaptureByType(t *testing.T) {
	capture := NewQueryCapture()

	capture.CaptureQuery("q1", "query", "SELECT 1", nil)
	capture.CaptureQuery("s1", "select", "SELECT 2", nil)
	capture.CaptureQuery("q2", "query", "SELECT 3", nil)
	capture.CaptureQuery("u1", "update", "UPDATE test SET x = 1", nil)

	queries := capture.ByType("query")
	if len(queries) != 2 {
		t.Errorf("expected 2 query type, got %d", len(queries))
	}

	selects := capture.ByType("select")
	if len(selects) != 1 {
		t.Errorf("expected 1 select type, got %d", len(selects))
	}
}

func TestQueryCaptureByCapability(t *testing.T) {
	capture := NewQueryCapture()

	capture.CaptureQuery("users-query", "query", "SELECT 1", nil)
	capture.CaptureQuery("users-query", "select", "SELECT 2", nil)
	capture.CaptureQuery("posts-query", "query", "SELECT 3", nil)

	usersQueries := capture.ByCapability("users-query")
	if len(usersQueries) != 2 {
		t.Errorf("expected 2 users-query, got %d", len(usersQueries))
	}
}

func TestQueryCaptureConcurrent(t *testing.T) {
	capture := NewQueryCapture()

	const numCaptures = 50
	var wg sync.WaitGroup
	wg.Add(numCaptures)

	for i := 0; i < numCaptures; i++ {
		go func(n int) {
			defer wg.Done()
			capture.CaptureQuery("test", "query", "SELECT 1", nil)
		}(i)
	}

	wg.Wait()

	if capture.Count() != numCaptures {
		t.Errorf("expected %d queries, got %d", numCaptures, capture.Count())
	}
}

func TestCapabilityCapture(t *testing.T) {
	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	capture := NewCapabilityCapture()

	c.Hook(edamame.CapabilityAdded, capture.Handler())
	c.Hook(edamame.CapabilityRemoved, capture.Handler())
	c.Hook(edamame.CapabilityNotFound, capture.Handler())

	ctx := context.Background()

	// Emit added event
	c.Emit(ctx, edamame.CapabilityAdded,
		edamame.KeyTable.Field("users"),
		edamame.KeyCapability.Field("find-by-email"),
		edamame.KeyType.Field("query"))

	// Emit removed event
	c.Emit(ctx, edamame.CapabilityRemoved,
		edamame.KeyTable.Field("users"),
		edamame.KeyCapability.Field("old-query"),
		edamame.KeyType.Field("query"))

	if capture.Count() != 2 {
		t.Errorf("expected 2 capabilities, got %d", capture.Count())
	}

	added := capture.ByAction("added")
	if len(added) != 1 {
		t.Errorf("expected 1 added, got %d", len(added))
	}

	if added[0].Table != "users" {
		t.Errorf("expected table 'users', got %q", added[0].Table)
	}
	if added[0].Name != "find-by-email" {
		t.Errorf("expected capability 'find-by-email', got %q", added[0].Name)
	}
}

func TestCapabilityCaptureByTable(t *testing.T) {
	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	capture := NewCapabilityCapture()
	c.Hook(edamame.CapabilityAdded, capture.Handler())

	ctx := context.Background()

	c.Emit(ctx, edamame.CapabilityAdded,
		edamame.KeyTable.Field("users"),
		edamame.KeyCapability.Field("q1"),
		edamame.KeyType.Field("query"))

	c.Emit(ctx, edamame.CapabilityAdded,
		edamame.KeyTable.Field("posts"),
		edamame.KeyCapability.Field("q2"),
		edamame.KeyType.Field("query"))

	c.Emit(ctx, edamame.CapabilityAdded,
		edamame.KeyTable.Field("users"),
		edamame.KeyCapability.Field("q3"),
		edamame.KeyType.Field("select"))

	users := capture.ByTable("users")
	if len(users) != 2 {
		t.Errorf("expected 2 users capabilities, got %d", len(users))
	}
}

func TestCapabilityCaptureWaitForCount(t *testing.T) {
	c := capitan.New()
	defer c.Shutdown()

	capture := NewCapabilityCapture()
	c.Hook(edamame.CapabilityAdded, capture.Handler())

	go func() {
		time.Sleep(10 * time.Millisecond)
		c.Emit(context.Background(), edamame.CapabilityAdded,
			edamame.KeyTable.Field("users"),
			edamame.KeyCapability.Field("test"),
			edamame.KeyType.Field("query"))
	}()

	if !capture.WaitForCount(1, 500*time.Millisecond) {
		t.Error("WaitForCount timed out")
	}

	// Test timeout
	if capture.WaitForCount(100, 10*time.Millisecond) {
		t.Error("WaitForCount should have timed out")
	}
}

func TestCapabilityCaptureReset(t *testing.T) {
	capture := NewCapabilityCapture()

	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	c.Hook(edamame.CapabilityAdded, capture.Handler())

	c.Emit(context.Background(), edamame.CapabilityAdded,
		edamame.KeyTable.Field("users"),
		edamame.KeyCapability.Field("test"),
		edamame.KeyType.Field("query"))

	if capture.Count() != 1 {
		t.Errorf("expected 1, got %d", capture.Count())
	}

	capture.Reset()

	if capture.Count() != 0 {
		t.Errorf("expected 0 after reset, got %d", capture.Count())
	}
}

func TestSpecValidator(t *testing.T) {
	validator := NewSpecValidator()

	spec := edamame.FactorySpec{
		Table: "users",
		Queries: []edamame.QueryCapabilitySpec{
			{Name: "query", Description: "Query all"},
		},
		Selects: []edamame.SelectCapabilitySpec{
			{Name: "select", Description: "Select one"},
		},
		Updates: []edamame.UpdateCapabilitySpec{
			{Name: "update-name", Description: "Update name"},
		},
		Deletes: []edamame.DeleteCapabilitySpec{
			{Name: "delete", Description: "Delete one"},
		},
		Aggregates: []edamame.AggregateCapabilitySpec{
			{Name: "count", Description: "Count all"},
		},
	}

	if !validator.HasQuery(spec, "query") {
		t.Error("expected HasQuery to return true")
	}
	if validator.HasQuery(spec, "nonexistent") {
		t.Error("expected HasQuery to return false for nonexistent")
	}

	if !validator.HasSelect(spec, "select") {
		t.Error("expected HasSelect to return true")
	}

	if !validator.HasUpdate(spec, "update-name") {
		t.Error("expected HasUpdate to return true")
	}

	if !validator.HasDelete(spec, "delete") {
		t.Error("expected HasDelete to return true")
	}

	if !validator.HasAggregate(spec, "count") {
		t.Error("expected HasAggregate to return true")
	}

	if validator.CountCapabilities(spec) != 5 {
		t.Errorf("expected 5 capabilities, got %d", validator.CountCapabilities(spec))
	}
}

func TestSpecValidatorByName(t *testing.T) {
	validator := NewSpecValidator()

	spec := edamame.FactorySpec{
		Queries: []edamame.QueryCapabilitySpec{
			{Name: "find-active", Description: "Find active users"},
		},
		Selects: []edamame.SelectCapabilitySpec{
			{Name: "get-by-id", Description: "Get user by ID"},
		},
	}

	query := validator.QueryByName(spec, "find-active")
	if query == nil {
		t.Fatal("expected non-nil query")
	}
	if query.Description != "Find active users" {
		t.Errorf("expected 'Find active users', got %q", query.Description)
	}

	if validator.QueryByName(spec, "nonexistent") != nil {
		t.Error("expected nil for nonexistent query")
	}

	sel := validator.SelectByName(spec, "get-by-id")
	if sel == nil {
		t.Fatal("expected non-nil select")
	}
}

func TestFactoryEventCapture(t *testing.T) {
	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	capture := NewFactoryEventCapture()
	c.Hook(edamame.FactoryCreated, capture.Handler())

	ctx := context.Background()

	c.Emit(ctx, edamame.FactoryCreated, edamame.KeyTable.Field("users"))
	c.Emit(ctx, edamame.FactoryCreated, edamame.KeyTable.Field("posts"))

	if capture.Count() != 2 {
		t.Errorf("expected 2 events, got %d", capture.Count())
	}

	tables := capture.Tables()
	if len(tables) != 2 {
		t.Fatalf("expected 2 tables, got %d", len(tables))
	}

	if tables[0].Table != "users" {
		t.Errorf("expected 'users', got %q", tables[0].Table)
	}
}

func TestFactoryEventCaptureReset(t *testing.T) {
	capture := NewFactoryEventCapture()

	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	c.Hook(edamame.FactoryCreated, capture.Handler())

	c.Emit(context.Background(), edamame.FactoryCreated, edamame.KeyTable.Field("users"))

	if capture.Count() != 1 {
		t.Errorf("expected 1, got %d", capture.Count())
	}

	capture.Reset()

	if capture.Count() != 0 {
		t.Errorf("expected 0 after reset, got %d", capture.Count())
	}
}

func TestFactoryEventCaptureWaitForCount(t *testing.T) {
	c := capitan.New()
	defer c.Shutdown()

	capture := NewFactoryEventCapture()
	c.Hook(edamame.FactoryCreated, capture.Handler())

	go func() {
		time.Sleep(10 * time.Millisecond)
		c.Emit(context.Background(), edamame.FactoryCreated, edamame.KeyTable.Field("users"))
	}()

	if !capture.WaitForCount(1, 500*time.Millisecond) {
		t.Error("WaitForCount timed out")
	}
}

func TestParamBuilder(t *testing.T) {
	pb := NewParamBuilder()

	params := pb.
		Set("id", 123).
		Set("name", "John").
		Set("active", true).
		Build()

	if params["id"] != 123 {
		t.Errorf("expected id=123, got %v", params["id"])
	}
	if params["name"] != "John" {
		t.Errorf("expected name='John', got %v", params["name"])
	}
	if params["active"] != true {
		t.Errorf("expected active=true, got %v", params["active"])
	}
}

func TestParamBuilderReset(t *testing.T) {
	pb := NewParamBuilder()

	pb.Set("id", 123)
	params1 := pb.Build()

	pb.Reset().Set("name", "Jane")
	params2 := pb.Build()

	if len(params1) != 1 {
		t.Errorf("expected 1 param in first build, got %d", len(params1))
	}

	if len(params2) != 1 {
		t.Errorf("expected 1 param after reset, got %d", len(params2))
	}

	if _, ok := params2["id"]; ok {
		t.Error("id should not be present after reset")
	}
}

func TestParamBuilderIsolation(t *testing.T) {
	pb := NewParamBuilder()

	pb.Set("id", 123)
	params := pb.Build()

	// Modify returned map
	params["id"] = 456

	// Build again - should get original value
	params2 := pb.Build()
	if params2["id"] != 123 {
		t.Errorf("expected original value 123, got %v", params2["id"])
	}
}
