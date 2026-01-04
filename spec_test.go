package edamame

import "testing"

// -----------------------------------------------------------------------------
// Query Spec Helper Method Tests
// -----------------------------------------------------------------------------

func TestConditionSpecIsGroup(t *testing.T) {
	tests := []struct {
		name     string
		spec     ConditionSpec
		expected bool
	}{
		{
			name:     "simple condition",
			spec:     ConditionSpec{Field: "age", Operator: ">=", Param: "min_age"},
			expected: false,
		},
		{
			name:     "null condition",
			spec:     ConditionSpec{Field: "email", IsNull: true},
			expected: false,
		},
		{
			name: "OR group",
			spec: ConditionSpec{
				Logic: "OR",
				Group: []ConditionSpec{
					{Field: "status", Operator: "=", Param: "active"},
					{Field: "status", Operator: "=", Param: "pending"},
				},
			},
			expected: true,
		},
		{
			name: "AND group",
			spec: ConditionSpec{
				Logic: "AND",
				Group: []ConditionSpec{
					{Field: "age", Operator: ">=", Param: "min_age"},
					{Field: "age", Operator: "<=", Param: "max_age"},
				},
			},
			expected: true,
		},
		{
			name:     "logic without group",
			spec:     ConditionSpec{Logic: "OR"},
			expected: false,
		},
		{
			name:     "group without logic",
			spec:     ConditionSpec{Group: []ConditionSpec{{Field: "x"}}},
			expected: false,
		},
		{
			name:     "empty group with logic",
			spec:     ConditionSpec{Logic: "OR", Group: []ConditionSpec{}},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.spec.IsGroup(); got != tt.expected {
				t.Errorf("IsGroup() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestConditionSpecIsBetween(t *testing.T) {
	tests := []struct {
		name     string
		spec     ConditionSpec
		expected bool
	}{
		{
			name:     "simple condition",
			spec:     ConditionSpec{Field: "age", Operator: ">=", Param: "min_age"},
			expected: false,
		},
		{
			name:     "between condition",
			spec:     ConditionSpec{Field: "age", Between: true, LowParam: "min", HighParam: "max"},
			expected: true,
		},
		{
			name:     "between without params",
			spec:     ConditionSpec{Field: "age", Between: true},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.spec.IsBetween(); got != tt.expected {
				t.Errorf("IsBetween() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestConditionSpecIsNotBetween(t *testing.T) {
	tests := []struct {
		name     string
		spec     ConditionSpec
		expected bool
	}{
		{
			name:     "simple condition",
			spec:     ConditionSpec{Field: "age", Operator: ">=", Param: "min_age"},
			expected: false,
		},
		{
			name:     "not between condition",
			spec:     ConditionSpec{Field: "age", NotBetween: true, LowParam: "min", HighParam: "max"},
			expected: true,
		},
		{
			name:     "not between without params",
			spec:     ConditionSpec{Field: "age", NotBetween: true},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.spec.IsNotBetween(); got != tt.expected {
				t.Errorf("IsNotBetween() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestConditionSpecIsFieldComparison(t *testing.T) {
	tests := []struct {
		name     string
		spec     ConditionSpec
		expected bool
	}{
		{
			name:     "simple condition",
			spec:     ConditionSpec{Field: "age", Operator: ">=", Param: "min_age"},
			expected: false,
		},
		{
			name:     "field comparison",
			spec:     ConditionSpec{Field: "created_at", Operator: "<", RightField: "updated_at"},
			expected: true,
		},
		{
			name:     "right field without operator",
			spec:     ConditionSpec{Field: "created_at", RightField: "updated_at"},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.spec.IsFieldComparison(); got != tt.expected {
				t.Errorf("IsFieldComparison() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestOrderBySpecHasNulls(t *testing.T) {
	tests := []struct {
		name     string
		spec     OrderBySpec
		expected bool
	}{
		{
			name:     "simple order by",
			spec:     OrderBySpec{Field: "name", Direction: "asc"},
			expected: false,
		},
		{
			name:     "nulls first",
			spec:     OrderBySpec{Field: "name", Direction: "asc", Nulls: "first"},
			expected: true,
		},
		{
			name:     "nulls last",
			spec:     OrderBySpec{Field: "name", Direction: "desc", Nulls: "last"},
			expected: true,
		},
		{
			name:     "empty nulls",
			spec:     OrderBySpec{Field: "name", Direction: "asc", Nulls: ""},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.spec.HasNulls(); got != tt.expected {
				t.Errorf("HasNulls() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestOrderBySpecIsExpression(t *testing.T) {
	tests := []struct {
		name     string
		spec     OrderBySpec
		expected bool
	}{
		{
			name:     "simple order by",
			spec:     OrderBySpec{Field: "name", Direction: "asc"},
			expected: false,
		},
		{
			name:     "vector distance expression",
			spec:     OrderBySpec{Field: "embedding", Operator: "<->", Param: "query_vec", Direction: "asc"},
			expected: true,
		},
		{
			name:     "operator without param",
			spec:     OrderBySpec{Field: "embedding", Operator: "<->", Direction: "asc"},
			expected: false,
		},
		{
			name:     "param without operator",
			spec:     OrderBySpec{Field: "embedding", Param: "query_vec", Direction: "asc"},
			expected: false,
		},
		{
			name:     "inner product expression",
			spec:     OrderBySpec{Field: "embedding", Operator: "<#>", Param: "query_vec", Direction: "asc"},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.spec.IsExpression(); got != tt.expected {
				t.Errorf("IsExpression() = %v, want %v", got, tt.expected)
			}
		})
	}
}
