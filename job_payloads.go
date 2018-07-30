package exec

import "time"

const (
    NeverExpires time.Duration = -1
)

// job payload
type Payload interface{}

// job payload with expiration time
type ExpiringPayload interface {
    ExpiresAfter() time.Duration
}
