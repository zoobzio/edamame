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

	if queries[0].Statement != "users-by-age" {
		t.Errorf("expected statement 'users-by-age', got %q", queries[0].Statement)
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
	if last.Statement != "second" {
		t.Errorf("expected 'second', got %q", last.Statement)
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

func TestQueryCaptureByStatement(t *testing.T) {
	capture := NewQueryCapture()

	capture.CaptureQuery("users-query", "query", "SELECT 1", nil)
	capture.CaptureQuery("users-query", "select", "SELECT 2", nil)
	capture.CaptureQuery("posts-query", "query", "SELECT 3", nil)

	usersQueries := capture.ByStatement("users-query")
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

func TestExecutorEventCapture(t *testing.T) {
	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	capture := NewExecutorEventCapture()
	c.Hook(edamame.ExecutorCreated, capture.Handler())

	ctx := context.Background()

	c.Emit(ctx, edamame.ExecutorCreated, edamame.KeyTable.Field("users"))
	c.Emit(ctx, edamame.ExecutorCreated, edamame.KeyTable.Field("posts"))

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

func TestExecutorEventCaptureReset(t *testing.T) {
	capture := NewExecutorEventCapture()

	c := capitan.New(capitan.WithSyncMode())
	defer c.Shutdown()

	c.Hook(edamame.ExecutorCreated, capture.Handler())

	c.Emit(context.Background(), edamame.ExecutorCreated, edamame.KeyTable.Field("users"))

	if capture.Count() != 1 {
		t.Errorf("expected 1, got %d", capture.Count())
	}

	capture.Reset()

	if capture.Count() != 0 {
		t.Errorf("expected 0 after reset, got %d", capture.Count())
	}
}

func TestExecutorEventCaptureWaitForCount(t *testing.T) {
	c := capitan.New()
	defer c.Shutdown()

	capture := NewExecutorEventCapture()
	c.Hook(edamame.ExecutorCreated, capture.Handler())

	go func() {
		time.Sleep(10 * time.Millisecond)
		c.Emit(context.Background(), edamame.ExecutorCreated, edamame.KeyTable.Field("users"))
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
