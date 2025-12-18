package benchmarks

import (
	"testing"

	"github.com/zoobzio/edamame"
)

// User is a test model for benchmarks.
type User struct {
	ID    int    `db:"id" type:"integer" constraints:"primarykey"`
	Email string `db:"email" type:"text" constraints:"notnull,unique"`
	Name  string `db:"name" type:"text"`
	Age   *int   `db:"age" type:"integer"`
}

// BenchmarkFactoryCreation measures factory initialization cost.
func BenchmarkFactoryCreation(b *testing.B) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := edamame.New[User](nil, "users")
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkQueryBuilding measures query builder creation from capability.
func BenchmarkQueryBuilding(b *testing.B) {
	factory, err := edamame.New[User](nil, "users")
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		q, err := factory.Query("query")
		if err != nil {
			b.Fatal(err)
		}
		_ = q
	}
}

// BenchmarkSelectBuilding measures select builder creation from capability.
func BenchmarkSelectBuilding(b *testing.B) {
	factory, err := edamame.New[User](nil, "users")
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		s, err := factory.Select("select")
		if err != nil {
			b.Fatal(err)
		}
		_ = s
	}
}

// BenchmarkQueryRender measures SQL rendering from query capability.
func BenchmarkQueryRender(b *testing.B) {
	factory, err := edamame.New[User](nil, "users")
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		sql, err := factory.RenderQuery("query")
		if err != nil {
			b.Fatal(err)
		}
		_ = sql
	}
}

// BenchmarkSelectRender measures SQL rendering from select capability.
func BenchmarkSelectRender(b *testing.B) {
	factory, err := edamame.New[User](nil, "users")
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		sql, err := factory.RenderSelect("select")
		if err != nil {
			b.Fatal(err)
		}
		_ = sql
	}
}

// BenchmarkAddQuery measures custom query capability registration.
func BenchmarkAddQuery(b *testing.B) {
	factory, err := edamame.New[User](nil, "users")
	if err != nil {
		b.Fatal(err)
	}

	cap := edamame.QueryCapability{
		Name:        "custom",
		Description: "Custom query",
		Spec: edamame.QuerySpec{
			Where: []edamame.ConditionSpec{
				{Field: "age", Operator: ">=", Param: "min_age"},
			},
		},
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		factory.AddQuery(cap)
	}
}

// BenchmarkCapabilityLookup measures capability lookup performance.
func BenchmarkCapabilityLookup(b *testing.B) {
	factory, err := edamame.New[User](nil, "users")
	if err != nil {
		b.Fatal(err)
	}

	// Add several custom queries
	for i := 0; i < 10; i++ {
		factory.AddQuery(edamame.QueryCapability{
			Name:        "query-" + string(rune('a'+i)),
			Description: "Custom query",
		})
	}

	b.Run("HasQuery", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			_ = factory.HasQuery("query-e")
		}
	})

	b.Run("GetQuery", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			_, _ = factory.GetQuery("query-e")
		}
	})

	b.Run("Query", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			_, _ = factory.Query("query-e")
		}
	})
}

// BenchmarkListCapabilities measures capability list performance.
func BenchmarkListCapabilities(b *testing.B) {
	factory, err := edamame.New[User](nil, "users")
	if err != nil {
		b.Fatal(err)
	}

	// Add several custom capabilities
	for i := 0; i < 20; i++ {
		factory.AddQuery(edamame.QueryCapability{
			Name: "query-" + string(rune('a'+i)),
		})
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = factory.ListQueries()
	}
}

// BenchmarkSpec measures spec generation performance.
func BenchmarkSpec(b *testing.B) {
	factory, err := edamame.New[User](nil, "users")
	if err != nil {
		b.Fatal(err)
	}

	// Add custom capabilities
	for i := 0; i < 5; i++ {
		factory.AddQuery(edamame.QueryCapability{Name: "q" + string(rune('a'+i))})
		factory.AddSelect(edamame.SelectCapability{Name: "s" + string(rune('a'+i))})
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = factory.Spec()
	}
}

// BenchmarkSpecJSON measures JSON spec generation performance.
func BenchmarkSpecJSON(b *testing.B) {
	factory, err := edamame.New[User](nil, "users")
	if err != nil {
		b.Fatal(err)
	}

	// Add custom capabilities
	for i := 0; i < 5; i++ {
		factory.AddQuery(edamame.QueryCapability{Name: "q" + string(rune('a'+i))})
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := factory.SpecJSON()
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkComplexQuery measures building a complex query with multiple conditions.
func BenchmarkComplexQuery(b *testing.B) {
	factory, err := edamame.New[User](nil, "users")
	if err != nil {
		b.Fatal(err)
	}

	limit := 100
	offset := 0

	factory.AddQuery(edamame.QueryCapability{
		Name: "complex",
		Spec: edamame.QuerySpec{
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
			Limit:  &limit,
			Offset: &offset,
		},
	})

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		q, err := factory.Query("complex")
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
	factory, err := edamame.New[User](nil, "users")
	if err != nil {
		b.Fatal(err)
	}

	factory.AddQuery(edamame.QueryCapability{
		Name: "grouped",
		Spec: edamame.QuerySpec{
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
		},
	})

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		q, err := factory.Query("grouped")
		if err != nil {
			b.Fatal(err)
		}
		_, err = q.Render()
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkConcurrentAccess measures concurrent capability access.
func BenchmarkConcurrentAccess(b *testing.B) {
	factory, err := edamame.New[User](nil, "users")
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = factory.Query("query")
			_ = factory.HasQuery("query")
			_, _ = factory.GetQuery("query")
		}
	})
}

// BenchmarkConcurrentAddRemove measures concurrent add/remove operations.
func BenchmarkConcurrentAddRemove(b *testing.B) {
	factory, err := edamame.New[User](nil, "users")
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			name := "temp-" + string(rune('a'+i%26))
			factory.AddQuery(edamame.QueryCapability{Name: name})
			factory.RemoveQuery(name)
			i++
		}
	})
}

// BenchmarkAggregateCapabilities measures aggregate capability performance.
func BenchmarkAggregateCapabilities(b *testing.B) {
	factory, err := edamame.New[User](nil, "users")
	if err != nil {
		b.Fatal(err)
	}

	// Add various aggregate capabilities
	factory.AddAggregate(edamame.AggregateCapability{
		Name: "sum-age",
		Spec: edamame.AggregateSpec{Field: "age"},
		Func: edamame.AggSum,
	})
	factory.AddAggregate(edamame.AggregateCapability{
		Name: "avg-age",
		Spec: edamame.AggregateSpec{Field: "age"},
		Func: edamame.AggAvg,
	})

	b.Run("count", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			a, err := factory.Aggregate("count")
			if err != nil {
				b.Fatal(err)
			}
			_, _ = a.Render()
		}
	})

	b.Run("sum", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			a, err := factory.Aggregate("sum-age")
			if err != nil {
				b.Fatal(err)
			}
			_, _ = a.Render()
		}
	})

	b.Run("avg", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			a, err := factory.Aggregate("avg-age")
			if err != nil {
				b.Fatal(err)
			}
			_, _ = a.Render()
		}
	})
}
