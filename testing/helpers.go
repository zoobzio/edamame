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
	Capability string
	Type       string // "query", "select", "update", "delete", "aggregate"
	SQL        string
	Params     map[string]any
	Timestamp  time.Time
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
func (qc *QueryCapture) CaptureQuery(capability, queryType, sql string, params map[string]any) {
	qc.mu.Lock()
	defer qc.mu.Unlock()
	qc.queries = append(qc.queries, RenderedQuery{
		Capability: capability,
		Type:       queryType,
		SQL:        sql,
		Params:     params,
		Timestamp:  time.Now(),
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

// ByCapability returns all captured queries for a specific capability.
func (qc *QueryCapture) ByCapability(capability string) []RenderedQuery {
	qc.mu.Lock()
	defer qc.mu.Unlock()
	result := make([]RenderedQuery, 0)
	for _, q := range qc.queries {
		if q.Capability == capability {
			result = append(result, q)
		}
	}
	return result
}

// CapabilityCapture captures capability registration events for testing.
// Thread-safe for concurrent capture.
type CapabilityCapture struct {
	capabilities []CapturedCapability
	mu           sync.Mutex
}

// CapturedCapability represents a captured capability event.
type CapturedCapability struct {
	Table     string
	Name      string
	Type      string
	Action    string // "added", "removed", "not_found"
	Timestamp time.Time
}

// NewCapabilityCapture creates a new CapabilityCapture instance.
func NewCapabilityCapture() *CapabilityCapture {
	return &CapabilityCapture{
		capabilities: make([]CapturedCapability, 0),
	}
}

// Handler returns an EventCallback that captures capability events.
// Use this with capitan.Hook to capture edamame capability events.
func (cc *CapabilityCapture) Handler() capitan.EventCallback {
	return func(_ context.Context, e *capitan.Event) {
		sig := e.Signal()
		var action string
		switch sig {
		case edamame.CapabilityAdded:
			action = "added"
		case edamame.CapabilityRemoved:
			action = "removed"
		case edamame.CapabilityNotFound:
			action = "not_found"
		default:
			return
		}

		table, _ := edamame.KeyTable.From(e)
		name, _ := edamame.KeyCapability.From(e)
		capType, _ := edamame.KeyType.From(e)

		cc.mu.Lock()
		defer cc.mu.Unlock()
		cc.capabilities = append(cc.capabilities, CapturedCapability{
			Table:     table,
			Name:      name,
			Type:      capType,
			Action:    action,
			Timestamp: time.Now(),
		})
	}
}

// Capabilities returns a copy of all captured capabilities.
func (cc *CapabilityCapture) Capabilities() []CapturedCapability {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	result := make([]CapturedCapability, len(cc.capabilities))
	copy(result, cc.capabilities)
	return result
}

// Count returns the number of captured capabilities.
func (cc *CapabilityCapture) Count() int {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	return len(cc.capabilities)
}

// Reset clears all captured capabilities.
func (cc *CapabilityCapture) Reset() {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	cc.capabilities = cc.capabilities[:0]
}

// ByAction returns all captured capabilities with a specific action.
func (cc *CapabilityCapture) ByAction(action string) []CapturedCapability {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	result := make([]CapturedCapability, 0)
	for _, c := range cc.capabilities {
		if c.Action == action {
			result = append(result, c)
		}
	}
	return result
}

// ByTable returns all captured capabilities for a specific table.
func (cc *CapabilityCapture) ByTable(table string) []CapturedCapability {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	result := make([]CapturedCapability, 0)
	for _, c := range cc.capabilities {
		if c.Table == table {
			result = append(result, c)
		}
	}
	return result
}

// WaitForCount blocks until the capture has at least n capabilities or timeout occurs.
// Returns true if count reached, false if timeout.
func (cc *CapabilityCapture) WaitForCount(n int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cc.Count() >= n {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return false
}

// SpecValidator provides utilities for validating factory specs.
type SpecValidator struct{}

// NewSpecValidator creates a new SpecValidator instance.
func NewSpecValidator() *SpecValidator {
	return &SpecValidator{}
}

// HasQuery checks if a spec contains a query capability with the given name.
func (*SpecValidator) HasQuery(spec edamame.FactorySpec, name string) bool {
	for i := range spec.Queries {
		if spec.Queries[i].Name == name {
			return true
		}
	}
	return false
}

// HasSelect checks if a spec contains a select capability with the given name.
func (*SpecValidator) HasSelect(spec edamame.FactorySpec, name string) bool {
	for i := range spec.Selects {
		if spec.Selects[i].Name == name {
			return true
		}
	}
	return false
}

// HasUpdate checks if a spec contains an update capability with the given name.
func (*SpecValidator) HasUpdate(spec edamame.FactorySpec, name string) bool {
	for _, u := range spec.Updates {
		if u.Name == name {
			return true
		}
	}
	return false
}

// HasDelete checks if a spec contains a delete capability with the given name.
func (*SpecValidator) HasDelete(spec edamame.FactorySpec, name string) bool {
	for _, d := range spec.Deletes {
		if d.Name == name {
			return true
		}
	}
	return false
}

// HasAggregate checks if a spec contains an aggregate capability with the given name.
func (*SpecValidator) HasAggregate(spec edamame.FactorySpec, name string) bool {
	for _, a := range spec.Aggregates {
		if a.Name == name {
			return true
		}
	}
	return false
}

// QueryByName returns a query capability by name, or nil if not found.
func (*SpecValidator) QueryByName(spec edamame.FactorySpec, name string) *edamame.QueryCapabilitySpec {
	for i := range spec.Queries {
		if spec.Queries[i].Name == name {
			return &spec.Queries[i]
		}
	}
	return nil
}

// SelectByName returns a select capability by name, or nil if not found.
func (*SpecValidator) SelectByName(spec edamame.FactorySpec, name string) *edamame.SelectCapabilitySpec {
	for i := range spec.Selects {
		if spec.Selects[i].Name == name {
			return &spec.Selects[i]
		}
	}
	return nil
}

// CountCapabilities returns the total number of capabilities in a spec.
func (*SpecValidator) CountCapabilities(spec edamame.FactorySpec) int {
	return len(spec.Queries) + len(spec.Selects) + len(spec.Updates) + len(spec.Deletes) + len(spec.Aggregates)
}

// FactoryEventCapture captures factory creation events.
// Thread-safe for concurrent capture.
type FactoryEventCapture struct {
	tables []FactoryCreatedEvent
	mu     sync.Mutex
}

// FactoryCreatedEvent represents a captured factory creation event.
type FactoryCreatedEvent struct {
	Table     string
	Timestamp time.Time
}

// NewFactoryEventCapture creates a new FactoryEventCapture instance.
func NewFactoryEventCapture() *FactoryEventCapture {
	return &FactoryEventCapture{
		tables: make([]FactoryCreatedEvent, 0),
	}
}

// Handler returns an EventCallback that captures factory creation events.
func (fc *FactoryEventCapture) Handler() capitan.EventCallback {
	return func(_ context.Context, e *capitan.Event) {
		if e.Signal() != edamame.FactoryCreated {
			return
		}

		table, _ := edamame.KeyTable.From(e)

		fc.mu.Lock()
		defer fc.mu.Unlock()
		fc.tables = append(fc.tables, FactoryCreatedEvent{
			Table:     table,
			Timestamp: time.Now(),
		})
	}
}

// Tables returns a copy of all captured factory tables.
func (fc *FactoryEventCapture) Tables() []FactoryCreatedEvent {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	result := make([]FactoryCreatedEvent, len(fc.tables))
	copy(result, fc.tables)
	return result
}

// Count returns the number of captured factory events.
func (fc *FactoryEventCapture) Count() int {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	return len(fc.tables)
}

// Reset clears all captured factory events.
func (fc *FactoryEventCapture) Reset() {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	fc.tables = fc.tables[:0]
}

// WaitForCount blocks until the capture has at least n events or timeout occurs.
func (fc *FactoryEventCapture) WaitForCount(n int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fc.Count() >= n {
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
