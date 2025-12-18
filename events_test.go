package edamame

import "testing"

func TestEventKeys(t *testing.T) {
	// Verify all event keys are defined
	keys := []struct {
		name string
		key  interface{}
	}{
		{"KeyTable", KeyTable},
		{"KeyCapability", KeyCapability},
		{"KeyType", KeyType},
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
	// Verify all signals are defined
	signals := []struct {
		name   string
		signal interface{}
	}{
		{"FactoryCreated", FactoryCreated},
		{"CapabilityAdded", CapabilityAdded},
		{"CapabilityRemoved", CapabilityRemoved},
		{"CapabilityNotFound", CapabilityNotFound},
	}

	for _, s := range signals {
		t.Run(s.name, func(t *testing.T) {
			if s.signal == nil {
				t.Errorf("%s is nil", s.name)
			}
		})
	}
}
