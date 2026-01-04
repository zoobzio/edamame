package benchmarks

import (
	"testing"

	"github.com/zoobzio/astql/pkg/postgres"
	"github.com/zoobzio/edamame"
)

// User is a test model for benchmarks.
type User struct {
	ID    int    `db:"id" type:"integer" constraints:"primarykey"`
	Email string `db:"email" type:"text" constraints:"notnull,unique"`
	Name  string `db:"name" type:"text"`
	Age   *int   `db:"age" type:"integer"`
}

// Define benchmark statements
var (
	benchQueryAll = edamame.NewQueryStatement("bench-query-all", "Query all users", edamame.QuerySpec{})

	benchSelectByID = edamame.NewSelectStatement("bench-select-by-id", "Select user by ID", edamame.SelectSpec{
		Where: []edamame.ConditionSpec{{Field: "id", Operator: "=", Param: "id"}},
	})

	benchQueryByAge = edamame.NewQueryStatement("bench-query-by-age", "Query users by age", edamame.QuerySpec{
		Where: []edamame.ConditionSpec{
			{Field: "age", Operator: ">=", Param: "min_age"},
		},
	})

	benchComplexQuery = edamame.NewQueryStatement("bench-complex", "Complex query", edamame.QuerySpec{
		Fields: []string{"id", "name", "email"},
		Where: []edamame.ConditionSpec{
			{Field: "age", Operator: ">=", Param: "min_age"},
			{Field: "age", Operator: "<=", Param: "max_age"},
			{
				Logic: "OR",
				Group: []edamame.ConditionSpec{
					{Field: "name", Operator: "LIKE", Param: "name_pattern"},
					{Field: "email", Operator: "LIKE", Param: "email_pattern"},
				},
			},
		},
		OrderBy: []edamame.OrderBySpec{
			{Field: "name", Direction: "asc"},
			{Field: "age", Direction: "desc"},
		},
		Limit:  intPtr(100),
		Offset: intPtr(0),
	})

	benchGroupedQuery = edamame.NewQueryStatement("bench-grouped", "Grouped conditions", edamame.QuerySpec{
		Where: []edamame.ConditionSpec{
			{Field: "age", Operator: ">=", Param: "min_age"},
			{
				Logic: "OR",
				Group: []edamame.ConditionSpec{
					{Field: "name", Operator: "=", Param: "name1"},
					{Field: "name", Operator: "=", Param: "name2"},
					{Field: "name", Operator: "=", Param: "name3"},
				},
			},
			{
				Logic: "AND",
				Group: []edamame.ConditionSpec{
					{Field: "email", Operator: "LIKE", Param: "email1"},
					{Field: "email", Operator: "LIKE", Param: "email2"},
				},
			},
		},
	})

	benchCountAll = edamame.NewAggregateStatement("bench-count", "Count all", edamame.AggCount, edamame.AggregateSpec{})

	benchSumAge = edamame.NewAggregateStatement("bench-sum-age", "Sum age", edamame.AggSum, edamame.AggregateSpec{Field: "age"})

	benchAvgAge = edamame.NewAggregateStatement("bench-avg-age", "Avg age", edamame.AggAvg, edamame.AggregateSpec{Field: "age"})
)

func intPtr(i int) *int { return &i }

// BenchmarkFactoryCreation measures factory initialization cost.
func BenchmarkFactoryCreation(b *testing.B) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := edamame.New[User](nil, "users", postgres.New())
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkQueryBuilding measures query builder creation from statement.
func BenchmarkQueryBuilding(b *testing.B) {
	factory, err := edamame.New[User](nil, "users", postgres.New())
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		q, err := factory.Query(benchQueryAll)
		if err != nil {
			b.Fatal(err)
		}
		_ = q
	}
}

// BenchmarkQueryWithConditions measures query builder creation with WHERE conditions.
func BenchmarkQueryWithConditions(b *testing.B) {
	factory, err := edamame.New[User](nil, "users", postgres.New())
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		q, err := factory.Query(benchQueryByAge)
		if err != nil {
			b.Fatal(err)
		}
		_ = q
	}
}

// BenchmarkSelectBuilding measures select builder creation from statement.
func BenchmarkSelectBuilding(b *testing.B) {
	factory, err := edamame.New[User](nil, "users", postgres.New())
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		s, err := factory.Select(benchSelectByID)
		if err != nil {
			b.Fatal(err)
		}
		_ = s
	}
}

// BenchmarkQueryRender measures SQL rendering from query statement.
func BenchmarkQueryRender(b *testing.B) {
	factory, err := edamame.New[User](nil, "users", postgres.New())
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		sql, err := factory.RenderQuery(benchQueryAll)
		if err != nil {
			b.Fatal(err)
		}
		_ = sql
	}
}

// BenchmarkSelectRender measures SQL rendering from select statement.
func BenchmarkSelectRender(b *testing.B) {
	factory, err := edamame.New[User](nil, "users", postgres.New())
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		sql, err := factory.RenderSelect(benchSelectByID)
		if err != nil {
			b.Fatal(err)
		}
		_ = sql
	}
}

// BenchmarkStatementCreation measures statement creation overhead.
func BenchmarkStatementCreation(b *testing.B) {
	spec := edamame.QuerySpec{
		Where: []edamame.ConditionSpec{
			{Field: "age", Operator: ">=", Param: "min_age"},
		},
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = edamame.NewQueryStatement("custom", "Custom query", spec)
	}
}

// BenchmarkComplexQuery measures building a complex query with multiple conditions.
func BenchmarkComplexQuery(b *testing.B) {
	factory, err := edamame.New[User](nil, "users", postgres.New())
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		q, err := factory.Query(benchComplexQuery)
		if err != nil {
			b.Fatal(err)
		}
		_, err = q.Render()
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkConditionGroups measures handling of nested condition groups.
func BenchmarkConditionGroups(b *testing.B) {
	factory, err := edamame.New[User](nil, "users", postgres.New())
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		q, err := factory.Query(benchGroupedQuery)
		if err != nil {
			b.Fatal(err)
		}
		_, err = q.Render()
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkConcurrentQueryBuilding measures concurrent query building.
func BenchmarkConcurrentQueryBuilding(b *testing.B) {
	factory, err := edamame.New[User](nil, "users", postgres.New())
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = factory.Query(benchQueryAll)
			_, _ = factory.Select(benchSelectByID)
		}
	})
}

// BenchmarkAggregateStatements measures aggregate statement performance.
func BenchmarkAggregateStatements(b *testing.B) {
	factory, err := edamame.New[User](nil, "users", postgres.New())
	if err != nil {
		b.Fatal(err)
	}

	b.Run("count", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			a := factory.Aggregate(benchCountAll)
			_, _ = a.Render()
		}
	})

	b.Run("sum", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			a := factory.Aggregate(benchSumAge)
			_, _ = a.Render()
		}
	})

	b.Run("avg", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			a := factory.Aggregate(benchAvgAge)
			_, _ = a.Render()
		}
	})
}
