package edamame

import "testing"

func TestAggregateFuncConstants(t *testing.T) {
	tests := []struct {
		name     string
		fn       AggregateFunc
		expected string
	}{
		{"COUNT", AggCount, "COUNT"},
		{"SUM", AggSum, "SUM"},
		{"AVG", AggAvg, "AVG"},
		{"MIN", AggMin, "MIN"},
		{"MAX", AggMax, "MAX"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if string(tt.fn) != tt.expected {
				t.Errorf("AggregateFunc %s = %q, want %q", tt.name, tt.fn, tt.expected)
			}
		})
	}
}

func TestCapabilityTypes(t *testing.T) {
	// Test that capability types can be instantiated with expected fields
	t.Run("QueryCapability", func(t *testing.T) {
		cap := QueryCapability{
			Name:        "test",
			Description: "test query",
			Spec:        QuerySpec{},
			Params:      []ParamSpec{{Name: "id", Required: true}},
			Tags:        []string{"test"},
		}
		if cap.Name != "test" {
			t.Errorf("Name = %q, want %q", cap.Name, "test")
		}
	})

	t.Run("SelectCapability", func(t *testing.T) {
		cap := SelectCapability{
			Name:        "test",
			Description: "test select",
			Spec:        SelectSpec{},
			Params:      []ParamSpec{{Name: "id", Required: true}},
			Tags:        []string{"test"},
		}
		if cap.Name != "test" {
			t.Errorf("Name = %q, want %q", cap.Name, "test")
		}
	})

	t.Run("UpdateCapability", func(t *testing.T) {
		cap := UpdateCapability{
			Name:        "test",
			Description: "test update",
			Spec:        UpdateSpec{Set: map[string]string{"name": "new"}},
			Params:      []ParamSpec{{Name: "id", Required: true}},
			Tags:        []string{"test"},
		}
		if cap.Name != "test" {
			t.Errorf("Name = %q, want %q", cap.Name, "test")
		}
	})

	t.Run("DeleteCapability", func(t *testing.T) {
		cap := DeleteCapability{
			Name:        "test",
			Description: "test delete",
			Spec:        DeleteSpec{},
			Params:      []ParamSpec{{Name: "id", Required: true}},
			Tags:        []string{"test"},
		}
		if cap.Name != "test" {
			t.Errorf("Name = %q, want %q", cap.Name, "test")
		}
	})

	t.Run("AggregateCapability", func(t *testing.T) {
		cap := AggregateCapability{
			Name:        "test",
			Description: "test aggregate",
			Spec:        AggregateSpec{Field: "age"},
			Func:        AggSum,
			Params:      []ParamSpec{},
			Tags:        []string{"test"},
		}
		if cap.Name != "test" {
			t.Errorf("Name = %q, want %q", cap.Name, "test")
		}
		if cap.Func != AggSum {
			t.Errorf("Func = %q, want %q", cap.Func, AggSum)
		}
	})
}

func TestParamSpec(t *testing.T) {
	param := ParamSpec{
		Name:        "user_id",
		Type:        "integer",
		Required:    true,
		Default:     nil,
		Description: "The user ID",
	}

	if param.Name != "user_id" {
		t.Errorf("Name = %q, want %q", param.Name, "user_id")
	}
	if param.Type != "integer" {
		t.Errorf("Type = %q, want %q", param.Type, "integer")
	}
	if !param.Required {
		t.Error("Required = false, want true")
	}
}
