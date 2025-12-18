package edamame

import "github.com/zoobzio/capitan"

// Event keys for structured logging.
var (
	KeyTable      = capitan.NewStringKey("table")
	KeyCapability = capitan.NewStringKey("capability")
	KeyType       = capitan.NewStringKey("type")
	KeyError      = capitan.NewStringKey("error")
	KeyDuration   = capitan.NewDurationKey("duration")
)

// Signals emitted by edamame.
var (
	FactoryCreated     = capitan.NewSignal("edamame.factory.created", "Factory instance created")
	CapabilityAdded    = capitan.NewSignal("edamame.capability.added", "Capability registered")
	CapabilityRemoved  = capitan.NewSignal("edamame.capability.removed", "Capability removed")
	CapabilityNotFound = capitan.NewSignal("edamame.capability.not_found", "Capability not found")
)
