// Package integration provides integration tests for edamame using testcontainers.
// These tests require Docker to be running and may take longer to execute.
//
// Run with: go test -tags=integration ./testing/integration/...
//
//go:build integration

package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/zoobzio/astql/pkg/postgres"
	"github.com/zoobzio/edamame"
)

// User is a test model for integration tests.
type User struct {
	ID    int    `db:"id" type:"integer" constraints:"primarykey"`
	Email string `db:"email" type:"text" constraints:"notnull,unique"`
	Name  string `db:"name" type:"text"`
	Age   *int   `db:"age" type:"integer"`
}

// PostgresContainer wraps a testcontainer postgres instance.
type PostgresContainer struct {
	container testcontainers.Container
	db        *sqlx.DB
	host      string
	port      string
}

// NewPostgresContainer creates and starts a PostgreSQL container.
func NewPostgresContainer(ctx context.Context) (*PostgresContainer, error) {
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
		return nil, fmt.Errorf("failed to start container: %w", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		container.Terminate(ctx)
		return nil, fmt.Errorf("failed to get container host: %w", err)
	}

	mappedPort, err := container.MappedPort(ctx, "5432")
	if err != nil {
		container.Terminate(ctx)
		return nil, fmt.Errorf("failed to get container port: %w", err)
	}

	dsn := fmt.Sprintf("host=%s port=%s user=test password=test dbname=testdb sslmode=disable", host, mappedPort.Port())
	db, err := sqlx.Connect("postgres", dsn)
	if err != nil {
		container.Terminate(ctx)
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	return &PostgresContainer{
		container: container,
		db:        db,
		host:      host,
		port:      mappedPort.Port(),
	}, nil
}

// DB returns the database connection.
func (pc *PostgresContainer) DB() *sqlx.DB {
	return pc.db
}

// Close terminates the container and closes the connection.
func (pc *PostgresContainer) Close(ctx context.Context) error {
	if pc.db != nil {
		pc.db.Close()
	}
	if pc.container != nil {
		return pc.container.Terminate(ctx)
	}
	return nil
}

// SetupUsersTable creates the users table for tests.
func (pc *PostgresContainer) SetupUsersTable(ctx context.Context) error {
	_, err := pc.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS users (
			id SERIAL PRIMARY KEY,
			email TEXT NOT NULL UNIQUE,
			name TEXT,
			age INTEGER
		)
	`)
	return err
}

// TruncateUsers clears the users table.
func (pc *PostgresContainer) TruncateUsers(ctx context.Context) error {
	_, err := pc.db.ExecContext(ctx, `TRUNCATE TABLE users RESTART IDENTITY CASCADE`)
	return err
}

// InsertTestUser inserts a test user and returns the ID.
func (pc *PostgresContainer) InsertTestUser(ctx context.Context, email, name string, age *int) (int, error) {
	var id int
	err := pc.db.QueryRowContext(ctx,
		`INSERT INTO users (email, name, age) VALUES ($1, $2, $3) RETURNING id`,
		email, name, age,
	).Scan(&id)
	return id, err
}

func TestPostgresIntegration_FactoryCreation(t *testing.T) {
	ctx := context.Background()

	pg, err := NewPostgresContainer(ctx)
	if err != nil {
		t.Fatalf("failed to create postgres container: %v", err)
	}
	defer pg.Close(ctx)

	if err := pg.SetupUsersTable(ctx); err != nil {
		t.Fatalf("failed to setup users table: %v", err)
	}

	factory, err := edamame.New[User](pg.DB(), "users", postgres.New())
	if err != nil {
		t.Fatalf("failed to create factory: %v", err)
	}

	// Verify default capabilities are registered
	if !factory.HasQuery("query") {
		t.Error("missing default query capability")
	}
	if !factory.HasSelect("select") {
		t.Error("missing default select capability")
	}
	if !factory.HasDelete("delete") {
		t.Error("missing default delete capability")
	}
	if !factory.HasAggregate("count") {
		t.Error("missing default count capability")
	}
}

func TestPostgresIntegration_QueryAll(t *testing.T) {
	ctx := context.Background()

	pg, err := NewPostgresContainer(ctx)
	if err != nil {
		t.Fatalf("failed to create postgres container: %v", err)
	}
	defer pg.Close(ctx)

	if err := pg.SetupUsersTable(ctx); err != nil {
		t.Fatalf("failed to setup users table: %v", err)
	}

	// Insert test data
	age1 := 25
	age2 := 30
	if _, err := pg.InsertTestUser(ctx, "alice@test.com", "Alice", &age1); err != nil {
		t.Fatalf("failed to insert user: %v", err)
	}
	if _, err := pg.InsertTestUser(ctx, "bob@test.com", "Bob", &age2); err != nil {
		t.Fatalf("failed to insert user: %v", err)
	}

	factory, err := edamame.New[User](pg.DB(), "users", postgres.New())
	if err != nil {
		t.Fatalf("failed to create factory: %v", err)
	}

	users, err := factory.ExecQuery(ctx, "query", nil)
	if err != nil {
		t.Fatalf("failed to execute query: %v", err)
	}

	if len(users) != 2 {
		t.Fatalf("expected 2 users, got %d", len(users))
	}
}

func TestPostgresIntegration_SelectByPrimaryKey(t *testing.T) {
	ctx := context.Background()

	pg, err := NewPostgresContainer(ctx)
	if err != nil {
		t.Fatalf("failed to create postgres container: %v", err)
	}
	defer pg.Close(ctx)

	if err := pg.SetupUsersTable(ctx); err != nil {
		t.Fatalf("failed to setup users table: %v", err)
	}

	age := 25
	id, err := pg.InsertTestUser(ctx, "alice@test.com", "Alice", &age)
	if err != nil {
		t.Fatalf("failed to insert user: %v", err)
	}

	factory, err := edamame.New[User](pg.DB(), "users", postgres.New())
	if err != nil {
		t.Fatalf("failed to create factory: %v", err)
	}

	user, err := factory.ExecSelect(ctx, "select", map[string]any{"id": id})
	if err != nil {
		t.Fatalf("failed to execute select: %v", err)
	}

	if user.Name != "Alice" {
		t.Errorf("expected name 'Alice', got %q", user.Name)
	}
	if user.Email != "alice@test.com" {
		t.Errorf("expected email 'alice@test.com', got %q", user.Email)
	}
}

func TestPostgresIntegration_Insert(t *testing.T) {
	ctx := context.Background()

	pg, err := NewPostgresContainer(ctx)
	if err != nil {
		t.Fatalf("failed to create postgres container: %v", err)
	}
	defer pg.Close(ctx)

	if err := pg.SetupUsersTable(ctx); err != nil {
		t.Fatalf("failed to setup users table: %v", err)
	}

	factory, err := edamame.New[User](pg.DB(), "users", postgres.New())
	if err != nil {
		t.Fatalf("failed to create factory: %v", err)
	}

	age := 28
	user := &User{
		Email: "charlie@test.com",
		Name:  "Charlie",
		Age:   &age,
	}

	inserted, err := factory.ExecInsert(ctx, user)
	if err != nil {
		t.Fatalf("failed to execute insert: %v", err)
	}

	if inserted.ID == 0 {
		t.Error("expected non-zero ID after insert")
	}
	if inserted.Name != "Charlie" {
		t.Errorf("expected name 'Charlie', got %q", inserted.Name)
	}

	// Verify in database
	retrieved, err := factory.ExecSelect(ctx, "select", map[string]any{"id": inserted.ID})
	if err != nil {
		t.Fatalf("failed to retrieve inserted user: %v", err)
	}
	if retrieved.Email != "charlie@test.com" {
		t.Errorf("expected email 'charlie@test.com', got %q", retrieved.Email)
	}
}

func TestPostgresIntegration_Delete(t *testing.T) {
	ctx := context.Background()

	pg, err := NewPostgresContainer(ctx)
	if err != nil {
		t.Fatalf("failed to create postgres container: %v", err)
	}
	defer pg.Close(ctx)

	if err := pg.SetupUsersTable(ctx); err != nil {
		t.Fatalf("failed to setup users table: %v", err)
	}

	age := 25
	id, err := pg.InsertTestUser(ctx, "toDelete@test.com", "ToDelete", &age)
	if err != nil {
		t.Fatalf("failed to insert user: %v", err)
	}

	factory, err := edamame.New[User](pg.DB(), "users", postgres.New())
	if err != nil {
		t.Fatalf("failed to create factory: %v", err)
	}

	deleted, err := factory.ExecDelete(ctx, "delete", map[string]any{"id": id})
	if err != nil {
		t.Fatalf("failed to execute delete: %v", err)
	}

	if deleted != 1 {
		t.Errorf("expected 1 deleted row, got %d", deleted)
	}

	// Verify user is gone
	_, err = factory.ExecSelect(ctx, "select", map[string]any{"id": id})
	if err == nil {
		t.Error("expected error when selecting deleted user")
	}
}

func TestPostgresIntegration_Count(t *testing.T) {
	ctx := context.Background()

	pg, err := NewPostgresContainer(ctx)
	if err != nil {
		t.Fatalf("failed to create postgres container: %v", err)
	}
	defer pg.Close(ctx)

	if err := pg.SetupUsersTable(ctx); err != nil {
		t.Fatalf("failed to setup users table: %v", err)
	}

	// Insert multiple users
	for i := 0; i < 5; i++ {
		age := 20 + i
		_, err := pg.InsertTestUser(ctx, fmt.Sprintf("user%d@test.com", i), fmt.Sprintf("User%d", i), &age)
		if err != nil {
			t.Fatalf("failed to insert user: %v", err)
		}
	}

	factory, err := edamame.New[User](pg.DB(), "users", postgres.New())
	if err != nil {
		t.Fatalf("failed to create factory: %v", err)
	}

	count, err := factory.ExecAggregate(ctx, "count", nil)
	if err != nil {
		t.Fatalf("failed to execute count: %v", err)
	}

	if count != 5 {
		t.Errorf("expected count 5, got %f", count)
	}
}

func TestPostgresIntegration_CustomQuery(t *testing.T) {
	ctx := context.Background()

	pg, err := NewPostgresContainer(ctx)
	if err != nil {
		t.Fatalf("failed to create postgres container: %v", err)
	}
	defer pg.Close(ctx)

	if err := pg.SetupUsersTable(ctx); err != nil {
		t.Fatalf("failed to setup users table: %v", err)
	}

	// Insert test data
	for i := 0; i < 10; i++ {
		age := 18 + i
		_, err := pg.InsertTestUser(ctx, fmt.Sprintf("user%d@test.com", i), fmt.Sprintf("User%d", i), &age)
		if err != nil {
			t.Fatalf("failed to insert user: %v", err)
		}
	}

	factory, err := edamame.New[User](pg.DB(), "users", postgres.New())
	if err != nil {
		t.Fatalf("failed to create factory: %v", err)
	}

	// Add custom query for users over a certain age
	factory.AddQuery(edamame.QueryCapability{
		Name:        "adults",
		Description: "Find adult users",
		Spec: edamame.QuerySpec{
			Where: []edamame.ConditionSpec{
				{Field: "age", Operator: ">=", Param: "min_age"},
			},
			OrderBy: []edamame.OrderBySpec{
				{Field: "age", Direction: "asc"},
			},
		},
	})

	users, err := factory.ExecQuery(ctx, "adults", map[string]any{"min_age": 25})
	if err != nil {
		t.Fatalf("failed to execute custom query: %v", err)
	}

	if len(users) != 3 {
		t.Errorf("expected 3 users age 25+, got %d", len(users))
	}

	// Verify ordering
	for i := 1; i < len(users); i++ {
		if *users[i].Age < *users[i-1].Age {
			t.Error("users should be ordered by age ascending")
		}
	}
}

func TestPostgresIntegration_Transaction(t *testing.T) {
	ctx := context.Background()

	pg, err := NewPostgresContainer(ctx)
	if err != nil {
		t.Fatalf("failed to create postgres container: %v", err)
	}
	defer pg.Close(ctx)

	if err := pg.SetupUsersTable(ctx); err != nil {
		t.Fatalf("failed to setup users table: %v", err)
	}

	factory, err := edamame.New[User](pg.DB(), "users", postgres.New())
	if err != nil {
		t.Fatalf("failed to create factory: %v", err)
	}

	// Start transaction
	tx, err := pg.DB().BeginTxx(ctx, nil)
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}

	// Insert within transaction
	age := 30
	user := &User{
		Email: "txtest@test.com",
		Name:  "TxTest",
		Age:   &age,
	}

	inserted, err := factory.ExecInsertTx(ctx, tx, user)
	if err != nil {
		tx.Rollback()
		t.Fatalf("failed to insert in transaction: %v", err)
	}

	// Query within same transaction - should see the insert
	users, err := factory.ExecQueryTx(ctx, tx, "query", nil)
	if err != nil {
		tx.Rollback()
		t.Fatalf("failed to query in transaction: %v", err)
	}

	if len(users) != 1 {
		tx.Rollback()
		t.Fatalf("expected 1 user in transaction, got %d", len(users))
	}

	// Rollback
	if err := tx.Rollback(); err != nil {
		t.Fatalf("failed to rollback: %v", err)
	}

	// Verify rollback - user should not exist
	_, err = factory.ExecSelect(ctx, "select", map[string]any{"id": inserted.ID})
	if err == nil {
		t.Error("expected error - user should not exist after rollback")
	}
}

func TestPostgresIntegration_BatchInsert(t *testing.T) {
	ctx := context.Background()

	pg, err := NewPostgresContainer(ctx)
	if err != nil {
		t.Fatalf("failed to create postgres container: %v", err)
	}
	defer pg.Close(ctx)

	if err := pg.SetupUsersTable(ctx); err != nil {
		t.Fatalf("failed to setup users table: %v", err)
	}

	factory, err := edamame.New[User](pg.DB(), "users", postgres.New())
	if err != nil {
		t.Fatalf("failed to create factory: %v", err)
	}

	// Prepare batch
	users := make([]*User, 10)
	for i := 0; i < 10; i++ {
		age := 20 + i
		users[i] = &User{
			Email: fmt.Sprintf("batch%d@test.com", i),
			Name:  fmt.Sprintf("Batch%d", i),
			Age:   &age,
		}
	}

	count, err := factory.ExecInsertBatch(ctx, users)
	if err != nil {
		t.Fatalf("failed to batch insert: %v", err)
	}

	if count != 10 {
		t.Errorf("expected 10 inserted, got %d", count)
	}

	// Verify count
	totalCount, err := factory.ExecAggregate(ctx, "count", nil)
	if err != nil {
		t.Fatalf("failed to count: %v", err)
	}

	if totalCount != 10 {
		t.Errorf("expected total count 10, got %f", totalCount)
	}
}

func TestPostgresIntegration_Spec(t *testing.T) {
	ctx := context.Background()

	pg, err := NewPostgresContainer(ctx)
	if err != nil {
		t.Fatalf("failed to create postgres container: %v", err)
	}
	defer pg.Close(ctx)

	if err := pg.SetupUsersTable(ctx); err != nil {
		t.Fatalf("failed to setup users table: %v", err)
	}

	factory, err := edamame.New[User](pg.DB(), "users", postgres.New())
	if err != nil {
		t.Fatalf("failed to create factory: %v", err)
	}

	// Add custom capabilities
	factory.AddQuery(edamame.QueryCapability{
		Name:        "by-age",
		Description: "Find users by age range",
		Spec: edamame.QuerySpec{
			Where: []edamame.ConditionSpec{
				{Field: "age", Operator: ">=", Param: "min_age"},
				{Field: "age", Operator: "<=", Param: "max_age"},
			},
		},
	})

	spec := factory.Spec()

	if spec.Table != "users" {
		t.Errorf("expected table 'users', got %q", spec.Table)
	}

	// Should have default + custom queries
	if len(spec.Queries) < 2 {
		t.Errorf("expected at least 2 queries, got %d", len(spec.Queries))
	}

	// Find custom query
	var found bool
	for _, q := range spec.Queries {
		if q.Name == "by-age" {
			found = true
			if len(q.Params) != 2 {
				t.Errorf("expected 2 params for by-age, got %d", len(q.Params))
			}
		}
	}

	if !found {
		t.Error("custom query 'by-age' not found in spec")
	}
}
