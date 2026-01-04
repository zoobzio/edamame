package edamame

import "github.com/zoobzio/capitan"

// Event keys for structured logging.
var (
	KeyTable    = capitan.NewStringKey("table")
	KeyError    = capitan.NewStringKey("error")
	KeyDuration = capitan.NewDurationKey("duration")
)

// Signals emitted by edamame.
var (
	ExecutorCreated = capitan.NewSignal("edamame.executor.created", "Executor instance created")
)
