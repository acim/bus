package bus_test

import "go.acim.net/bus"

var _ bus.Queue[string] = (*bus.PubSubQueue[string])(nil)
