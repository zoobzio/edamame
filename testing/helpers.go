// Package testing provides test utilities and helpers for edamame users.
// These utilities help users test their own edamame-based applications.
package testing

import (
	"context"
	"sync"
	"time"

	"github.com/zoobzio/capitan"
	"github.com/zoobzio/edamame"
)

// RenderedQuery represents a captured rendered query for testing.
type RenderedQuery struct {
	Statement string
	Type      string // "query", "select", "update", "delete", "aggregate"
	SQL       string
	Params    map[string]any
	Timestamp time.Time
}

// QueryCapture captures rendered SQL queries for testing and verification.
// Thread-safe for concurrent capture.
type QueryCapture struct {
	queries []RenderedQuery
	mu      sync.Mutex
}

// NewQueryCapture creates a new QueryCapture instance.
func NewQueryCapture() *QueryCapture {
	return &QueryCapture{
		queries: make([]RenderedQuery, 0),
	}
}

// CaptureQuery adds a rendered query to the capture.
func (qc *QueryCapture) CaptureQuery(statement, queryType, sql string, params map[string]any) {
	qc.mu.Lock()
	defer qc.mu.Unlock()
	qc.queries = append(qc.queries, RenderedQuery{
		Statement: statement,
		Type:      queryType,
		SQL:       sql,
		Params:    params,
		Timestamp: time.Now(),
	})
}

// Queries returns a copy of all captured queries.
func (qc *QueryCapture) Queries() []RenderedQuery {
	qc.mu.Lock()
	defer qc.mu.Unlock()
	result := make([]RenderedQuery, len(qc.queries))
	copy(result, qc.queries)
	return result
}

// Count returns the number of captured queries.
func (qc *QueryCapture) Count() int {
	qc.mu.Lock()
	defer qc.mu.Unlock()
	return len(qc.queries)
}

// Reset clears all captured queries.
func (qc *QueryCapture) Reset() {
	qc.mu.Lock()
	defer qc.mu.Unlock()
	qc.queries = qc.queries[:0]
}

// Last returns the most recently captured query, or nil if none.
func (qc *QueryCapture) Last() *RenderedQuery {
	qc.mu.Lock()
	defer qc.mu.Unlock()
	if len(qc.queries) == 0 {
		return nil
	}
	q := qc.queries[len(qc.queries)-1]
	return &q
}

// ByType returns all captured queries of a specific type.
func (qc *QueryCapture) ByType(queryType string) []RenderedQuery {
	qc.mu.Lock()
	defer qc.mu.Unlock()
	result := make([]RenderedQuery, 0)
	for _, q := range qc.queries {
		if q.Type == queryType {
			result = append(result, q)
		}
	}
	return result
}

// ByStatement returns all captured queries for a specific statement.
func (qc *QueryCapture) ByStatement(statement string) []RenderedQuery {
	qc.mu.Lock()
	defer qc.mu.Unlock()
	result := make([]RenderedQuery, 0)
	for _, q := range qc.queries {
		if q.Statement == statement {
			result = append(result, q)
		}
	}
	return result
}

// ExecutorEventCapture captures executor creation events.
// Thread-safe for concurrent capture.
type ExecutorEventCapture struct {
	tables []ExecutorCreatedEvent
	mu     sync.Mutex
}

// ExecutorCreatedEvent represents a captured executor creation event.
type ExecutorCreatedEvent struct {
	Table     string
	Timestamp time.Time
}

// NewExecutorEventCapture creates a new ExecutorEventCapture instance.
func NewExecutorEventCapture() *ExecutorEventCapture {
	return &ExecutorEventCapture{
		tables: make([]ExecutorCreatedEvent, 0),
	}
}

// Handler returns an EventCallback that captures executor creation events.
func (ec *ExecutorEventCapture) Handler() capitan.EventCallback {
	return func(_ context.Context, e *capitan.Event) {
		if e.Signal() != edamame.ExecutorCreated {
			return
		}

		table, _ := edamame.KeyTable.From(e)

		ec.mu.Lock()
		defer ec.mu.Unlock()
		ec.tables = append(ec.tables, ExecutorCreatedEvent{
			Table:     table,
			Timestamp: time.Now(),
		})
	}
}

// Tables returns a copy of all captured executor tables.
func (ec *ExecutorEventCapture) Tables() []ExecutorCreatedEvent {
	ec.mu.Lock()
	defer ec.mu.Unlock()
	result := make([]ExecutorCreatedEvent, len(ec.tables))
	copy(result, ec.tables)
	return result
}

// Count returns the number of captured executor events.
func (ec *ExecutorEventCapture) Count() int {
	ec.mu.Lock()
	defer ec.mu.Unlock()
	return len(ec.tables)
}

// Reset clears all captured executor events.
func (ec *ExecutorEventCapture) Reset() {
	ec.mu.Lock()
	defer ec.mu.Unlock()
	ec.tables = ec.tables[:0]
}

// WaitForCount blocks until the capture has at least n events or timeout occurs.
func (ec *ExecutorEventCapture) WaitForCount(n int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if ec.Count() >= n {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return false
}

// ParamBuilder helps construct test parameter maps.
type ParamBuilder struct {
	params map[string]any
}

// NewParamBuilder creates a new ParamBuilder instance.
func NewParamBuilder() *ParamBuilder {
	return &ParamBuilder{
		params: make(map[string]any),
	}
}

// Set adds a parameter to the builder.
func (pb *ParamBuilder) Set(key string, value any) *ParamBuilder {
	pb.params[key] = value
	return pb
}

// Build returns the constructed parameter map.
func (pb *ParamBuilder) Build() map[string]any {
	result := make(map[string]any, len(pb.params))
	for k, v := range pb.params {
		result[k] = v
	}
	return result
}

// Reset clears the builder.
func (pb *ParamBuilder) Reset() *ParamBuilder {
	pb.params = make(map[string]any)
	return pb
}
