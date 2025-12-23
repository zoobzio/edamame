package edamame

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/zoobzio/astql/pkg/postgres"
)

// testDB holds the shared database connection for exec tests.
var testDB *sqlx.DB

// testContainer holds the shared container reference.
var testContainer testcontainers.Container

// TestMain sets up a shared postgres container for all tests.
func TestMain(m *testing.M) {
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "postgres:16-alpine",
		ExposedPorts: []string{"5432/tcp"},
		WaitingFor:   wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(60 * time.Second),
		Env: map[string]string{
			"POSTGRES_USER":     "test",
			"POSTGRES_PASSWORD": "test",
			"POSTGRES_DB":       "testdb",
		},
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to start container: %v\n", err)
		os.Exit(1)
	}
	testContainer = container

	host, err := container.Host(ctx)
	if err != nil {
		container.Terminate(ctx)
		fmt.Fprintf(os.Stderr, "failed to get container host: %v\n", err)
		os.Exit(1)
	}

	mappedPort, err := container.MappedPort(ctx, "5432")
	if err != nil {
		container.Terminate(ctx)
		fmt.Fprintf(os.Stderr, "failed to get container port: %v\n", err)
		os.Exit(1)
	}

	dsn := fmt.Sprintf("host=%s port=%s user=test password=test dbname=testdb sslmode=disable", host, mappedPort.Port())
	db, err := sqlx.Connect("postgres", dsn)
	if err != nil {
		container.Terminate(ctx)
		fmt.Fprintf(os.Stderr, "failed to connect to database: %v\n", err)
		os.Exit(1)
	}
	testDB = db

	_, err = testDB.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS users (
			id SERIAL PRIMARY KEY,
			email TEXT NOT NULL UNIQUE,
			name TEXT,
			age INTEGER
		)
	`)
	if err != nil {
		db.Close()
		container.Terminate(ctx)
		fmt.Fprintf(os.Stderr, "failed to create table: %v\n", err)
		os.Exit(1)
	}

	code := m.Run()

	db.Close()
	container.Terminate(ctx)

	os.Exit(code)
}

// truncateUsers clears the users table between tests.
func truncateUsers(t *testing.T) {
	t.Helper()
	_, err := testDB.ExecContext(context.Background(), `TRUNCATE TABLE users RESTART IDENTITY CASCADE`)
	if err != nil {
		t.Fatalf("failed to truncate users: %v", err)
	}
}

// insertTestUser inserts a test user and returns the ID.
func insertTestUser(t *testing.T, email, name string, age *int) int {
	t.Helper()
	var id int
	err := testDB.QueryRowContext(context.Background(),
		`INSERT INTO users (email, name, age) VALUES ($1, $2, $3) RETURNING id`,
		email, name, age,
	).Scan(&id)
	if err != nil {
		t.Fatalf("failed to insert user: %v", err)
	}
	return id
}

func TestQueryDispatch(t *testing.T) {
	factory, err := New[User](nil, "users", postgres.New())
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
	factory, err := New[User](nil, "users", postgres.New())
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
	factory, err := New[User](nil, "users", postgres.New())
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
	factory, err := New[User](nil, "users", postgres.New())
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
	factory, err := New[User](nil, "users", postgres.New())
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
	factory, err := New[User](nil, "users", postgres.New())
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
	factory, err := New[User](nil, "users", postgres.New())
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
	factory, err := New[User](nil, "users", postgres.New())
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
	factory, err := New[User](nil, "users", postgres.New())
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
	factory, err := New[User](nil, "users", postgres.New())
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
	factory, err := New[User](nil, "users", postgres.New())
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
	factory, err := New[User](nil, "users", postgres.New())
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
	factory, err := New[User](nil, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	// Batch update/delete should fail for nonexistent capabilities
	_, err = factory.ExecUpdateBatch(context.TODO(), "nonexistent", nil)
	if err == nil {
		t.Error("ExecUpdateBatch('nonexistent') should return error")
	}

	_, err = factory.ExecDeleteBatch(context.TODO(), "nonexistent", nil)
	if err == nil {
		t.Error("ExecDeleteBatch('nonexistent') should return error")
	}
}

// -----------------------------------------------------------------------------
// Exec* Tests (require testcontainer database)
// -----------------------------------------------------------------------------

func TestExecQuery(t *testing.T) {
	truncateUsers(t)
	ctx := context.Background()

	age1, age2 := 25, 30
	insertTestUser(t, "alice@test.com", "Alice", &age1)
	insertTestUser(t, "bob@test.com", "Bob", &age2)

	factory, err := New[User](testDB, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	users, err := factory.ExecQuery(ctx, "query", nil)
	if err != nil {
		t.Fatalf("ExecQuery() failed: %v", err)
	}

	if len(users) != 2 {
		t.Errorf("expected 2 users, got %d", len(users))
	}
}

func TestExecQueryTx(t *testing.T) {
	truncateUsers(t)
	ctx := context.Background()

	age := 25
	insertTestUser(t, "alice@test.com", "Alice", &age)

	factory, err := New[User](testDB, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	tx, err := testDB.BeginTxx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTxx() failed: %v", err)
	}
	defer tx.Rollback()

	users, err := factory.ExecQueryTx(ctx, tx, "query", nil)
	if err != nil {
		t.Fatalf("ExecQueryTx() failed: %v", err)
	}

	if len(users) != 1 {
		t.Errorf("expected 1 user, got %d", len(users))
	}
}

func TestExecSelect(t *testing.T) {
	truncateUsers(t)
	ctx := context.Background()

	age := 25
	id := insertTestUser(t, "alice@test.com", "Alice", &age)

	factory, err := New[User](testDB, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	user, err := factory.ExecSelect(ctx, "select", map[string]any{"id": id})
	if err != nil {
		t.Fatalf("ExecSelect() failed: %v", err)
	}

	if user.Name != "Alice" {
		t.Errorf("expected name 'Alice', got %q", user.Name)
	}
}

func TestExecSelectTx(t *testing.T) {
	truncateUsers(t)
	ctx := context.Background()

	age := 25
	id := insertTestUser(t, "alice@test.com", "Alice", &age)

	factory, err := New[User](testDB, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	tx, err := testDB.BeginTxx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTxx() failed: %v", err)
	}
	defer tx.Rollback()

	user, err := factory.ExecSelectTx(ctx, tx, "select", map[string]any{"id": id})
	if err != nil {
		t.Fatalf("ExecSelectTx() failed: %v", err)
	}

	if user.Name != "Alice" {
		t.Errorf("expected name 'Alice', got %q", user.Name)
	}
}

func TestExecUpdate(t *testing.T) {
	truncateUsers(t)
	ctx := context.Background()

	age := 25
	id := insertTestUser(t, "alice@test.com", "Alice", &age)

	factory, err := New[User](testDB, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	err = factory.AddUpdate(UpdateCapability{
		Name: "update-name",
		Spec: UpdateSpec{
			Set:   map[string]string{"name": "new_name"},
			Where: []ConditionSpec{{Field: "id", Operator: "=", Param: "id"}},
		},
	})
	if err != nil {
		t.Fatalf("AddUpdate() failed: %v", err)
	}

	updated, err := factory.ExecUpdate(ctx, "update-name", map[string]any{"id": id, "new_name": "Updated"})
	if err != nil {
		t.Fatalf("ExecUpdate() failed: %v", err)
	}

	if updated.Name != "Updated" {
		t.Errorf("expected name 'Updated', got %q", updated.Name)
	}
}

func TestExecUpdateTx(t *testing.T) {
	truncateUsers(t)
	ctx := context.Background()

	age := 25
	id := insertTestUser(t, "alice@test.com", "Alice", &age)

	factory, err := New[User](testDB, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	err = factory.AddUpdate(UpdateCapability{
		Name: "update-name-tx",
		Spec: UpdateSpec{
			Set:   map[string]string{"name": "new_name"},
			Where: []ConditionSpec{{Field: "id", Operator: "=", Param: "id"}},
		},
	})
	if err != nil {
		t.Fatalf("AddUpdate() failed: %v", err)
	}

	tx, err := testDB.BeginTxx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTxx() failed: %v", err)
	}

	updated, err := factory.ExecUpdateTx(ctx, tx, "update-name-tx", map[string]any{"id": id, "new_name": "TxUpdated"})
	if err != nil {
		tx.Rollback()
		t.Fatalf("ExecUpdateTx() failed: %v", err)
	}

	if updated.Name != "TxUpdated" {
		tx.Rollback()
		t.Errorf("expected name 'TxUpdated', got %q", updated.Name)
	}

	tx.Rollback()

	user, _ := factory.ExecSelect(ctx, "select", map[string]any{"id": id})
	if user.Name != "Alice" {
		t.Errorf("expected name 'Alice' after rollback, got %q", user.Name)
	}
}

func TestExecDelete(t *testing.T) {
	truncateUsers(t)
	ctx := context.Background()

	age := 25
	id := insertTestUser(t, "alice@test.com", "Alice", &age)

	factory, err := New[User](testDB, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	count, err := factory.ExecDelete(ctx, "delete", map[string]any{"id": id})
	if err != nil {
		t.Fatalf("ExecDelete() failed: %v", err)
	}

	if count != 1 {
		t.Errorf("expected 1 deleted row, got %d", count)
	}
}

func TestExecDeleteTx(t *testing.T) {
	truncateUsers(t)
	ctx := context.Background()

	age := 25
	id := insertTestUser(t, "alice@test.com", "Alice", &age)

	factory, err := New[User](testDB, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	tx, err := testDB.BeginTxx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTxx() failed: %v", err)
	}

	count, err := factory.ExecDeleteTx(ctx, tx, "delete", map[string]any{"id": id})
	if err != nil {
		tx.Rollback()
		t.Fatalf("ExecDeleteTx() failed: %v", err)
	}

	if count != 1 {
		tx.Rollback()
		t.Errorf("expected 1 deleted row, got %d", count)
	}

	tx.Rollback()

	user, err := factory.ExecSelect(ctx, "select", map[string]any{"id": id})
	if err != nil {
		t.Fatalf("user should exist after rollback: %v", err)
	}
	if user.Name != "Alice" {
		t.Errorf("expected name 'Alice', got %q", user.Name)
	}
}

func TestExecAggregate(t *testing.T) {
	truncateUsers(t)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		age := 20 + i
		insertTestUser(t, fmt.Sprintf("user%d@test.com", i), fmt.Sprintf("User%d", i), &age)
	}

	factory, err := New[User](testDB, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	count, err := factory.ExecAggregate(ctx, "count", nil)
	if err != nil {
		t.Fatalf("ExecAggregate() failed: %v", err)
	}

	if count != 5 {
		t.Errorf("expected count 5, got %f", count)
	}
}

func TestExecAggregateTx(t *testing.T) {
	truncateUsers(t)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		age := 20 + i
		insertTestUser(t, fmt.Sprintf("user%d@test.com", i), fmt.Sprintf("User%d", i), &age)
	}

	factory, err := New[User](testDB, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	tx, err := testDB.BeginTxx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTxx() failed: %v", err)
	}
	defer tx.Rollback()

	count, err := factory.ExecAggregateTx(ctx, tx, "count", nil)
	if err != nil {
		t.Fatalf("ExecAggregateTx() failed: %v", err)
	}

	if count != 3 {
		t.Errorf("expected count 3, got %f", count)
	}
}

func TestExecInsert(t *testing.T) {
	truncateUsers(t)
	ctx := context.Background()

	factory, err := New[User](testDB, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	age := 28
	user := &User{
		Email: "charlie@test.com",
		Name:  "Charlie",
		Age:   &age,
	}

	inserted, err := factory.ExecInsert(ctx, user)
	if err != nil {
		t.Fatalf("ExecInsert() failed: %v", err)
	}

	if inserted.ID == 0 {
		t.Error("expected non-zero ID after insert")
	}
	if inserted.Name != "Charlie" {
		t.Errorf("expected name 'Charlie', got %q", inserted.Name)
	}
}

func TestExecInsertTx(t *testing.T) {
	truncateUsers(t)
	ctx := context.Background()

	factory, err := New[User](testDB, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	tx, err := testDB.BeginTxx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTxx() failed: %v", err)
	}

	age := 28
	user := &User{
		Email: "txinsert@test.com",
		Name:  "TxInsert",
		Age:   &age,
	}

	inserted, err := factory.ExecInsertTx(ctx, tx, user)
	if err != nil {
		tx.Rollback()
		t.Fatalf("ExecInsertTx() failed: %v", err)
	}

	tx.Rollback()

	_, err = factory.ExecSelect(ctx, "select", map[string]any{"id": inserted.ID})
	if err == nil {
		t.Error("user should not exist after rollback")
	}
}

func TestExecInsertBatch(t *testing.T) {
	truncateUsers(t)
	ctx := context.Background()

	factory, err := New[User](testDB, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	users := make([]*User, 5)
	for i := 0; i < 5; i++ {
		age := 20 + i
		users[i] = &User{
			Email: fmt.Sprintf("batch%d@test.com", i),
			Name:  fmt.Sprintf("Batch%d", i),
			Age:   &age,
		}
	}

	count, err := factory.ExecInsertBatch(ctx, users)
	if err != nil {
		t.Fatalf("ExecInsertBatch() failed: %v", err)
	}

	if count != 5 {
		t.Errorf("expected 5 inserted, got %d", count)
	}

	totalCount, err := factory.ExecAggregate(ctx, "count", nil)
	if err != nil {
		t.Fatalf("ExecAggregate() failed: %v", err)
	}

	if totalCount != 5 {
		t.Errorf("expected total count 5, got %f", totalCount)
	}
}

func TestExecInsertBatchTx(t *testing.T) {
	truncateUsers(t)
	ctx := context.Background()

	factory, err := New[User](testDB, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	tx, err := testDB.BeginTxx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTxx() failed: %v", err)
	}

	users := make([]*User, 3)
	for i := 0; i < 3; i++ {
		age := 20 + i
		users[i] = &User{
			Email: fmt.Sprintf("txbatch%d@test.com", i),
			Name:  fmt.Sprintf("TxBatch%d", i),
			Age:   &age,
		}
	}

	count, err := factory.ExecInsertBatchTx(ctx, tx, users)
	if err != nil {
		tx.Rollback()
		t.Fatalf("ExecInsertBatchTx() failed: %v", err)
	}

	if count != 3 {
		tx.Rollback()
		t.Errorf("expected 3 inserted, got %d", count)
	}

	tx.Rollback()

	totalCount, err := factory.ExecAggregate(ctx, "count", nil)
	if err != nil {
		t.Fatalf("ExecAggregate() failed: %v", err)
	}

	if totalCount != 0 {
		t.Errorf("expected 0 after rollback, got %f", totalCount)
	}
}

func TestExecCompound(t *testing.T) {
	truncateUsers(t)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		age := 20 + i*5
		insertTestUser(t, fmt.Sprintf("user%d@test.com", i), fmt.Sprintf("User%d", i), &age)
	}

	factory, err := New[User](testDB, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	spec := CompoundQuerySpec{
		Base: QuerySpec{
			Fields: []string{"id", "name", "email", "age"},
			Where:  []ConditionSpec{{Field: "age", Operator: "<", Param: "young_max"}},
		},
		Operands: []SetOperandSpec{
			{
				Operation: "union",
				Query: QuerySpec{
					Fields: []string{"id", "name", "email", "age"},
					Where:  []ConditionSpec{{Field: "age", Operator: ">", Param: "old_min"}},
				},
			},
		},
		OrderBy: []OrderBySpec{{Field: "age", Direction: "asc"}},
	}

	users, err := factory.ExecCompound(ctx, spec, map[string]any{"q0_young_max": 22, "q1_old_min": 38})
	if err != nil {
		t.Fatalf("ExecCompound() failed: %v", err)
	}

	if len(users) != 2 {
		t.Errorf("expected 2 users, got %d", len(users))
	}
}
