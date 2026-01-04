package edamame

import (
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

	if factory.Soy() == nil {
		t.Error("Soy() returned nil")
	}
}

func TestNew_EmptyTableName(t *testing.T) {
	_, err := New[User](nil, "", postgres.New())
	if err == nil {
		t.Error("New() with empty table name should fail")
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

func TestSoyAccessor(t *testing.T) {
	factory, err := New[User](nil, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	c := factory.Soy()
	if c == nil {
		t.Fatal("Soy() returned nil")
	}

	if c.TableName() != "users" {
		t.Errorf("Soy().TableName() = %q, want %q", c.TableName(), "users")
	}
}
