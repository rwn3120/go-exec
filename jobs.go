package exec

import (
    "time"
    "github.com/satori/go.uuid"
)

const (
    NeverExpires time.Duration = -1
)

type Payload interface{}

type ExpiringPayload interface {
    ExpiresAfter() time.Duration
}

type output struct {
    correlationId string
    result        Result
}

func newOutput(correlationId string, userResult Result) *output {
    return &output{
        correlationId: correlationId,
        result:        userResult,
    }
}

type job struct {
    correlationId string
    expiresAfter  time.Duration
    payload       Payload
    output        chan *output
}

func newJob(payload Payload) (*job, error) {
    expiresAfter := NeverExpires
    if expiringJob, ok := payload.(ExpiringPayload); ok {
        expiresAfter = expiringJob.ExpiresAfter()
    }
    return &job{
        correlationId: uuid.NewV4().String(),
        payload:       payload,
        expiresAfter:  expiresAfter,
        output:        make(chan *output)}, nil
}