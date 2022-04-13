package acclocator

import (
	"time"
)

type Meta struct {
	ID                string
	Address           string
	StartedAt         time.Time
	LastHeartbeatTime time.Time
}
