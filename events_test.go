package edamame

import "testing"

func TestEventKeys(t *testing.T) {
	keys := []struct {
		name string
		key  interface{}
	}{
		{"KeyTable", KeyTable},
		{"KeyError", KeyError},
		{"KeyDuration", KeyDuration},
	}

	for _, k := range keys {
		t.Run(k.name, func(t *testing.T) {
			if k.key == nil {
				t.Errorf("%s is nil", k.name)
			}
		})
	}
}

func TestSignals(t *testing.T) {
	signals := []struct {
		name   string
		signal interface{}
	}{
		{"ExecutorCreated", ExecutorCreated},
	}

	for _, s := range signals {
		t.Run(s.name, func(t *testing.T) {
			if s.signal == nil {
				t.Errorf("%s is nil", s.name)
			}
		})
	}
}
