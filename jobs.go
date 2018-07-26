package exec

import (
    "time"
    "errors"
    "github.com/satori/go.uuid"
)

const (
    NeverExpires time.Duration = -1
)

type Payload interface{}

type Result interface {
    Err() error
}

type job struct {
    correlationId string
    expiresAfter  time.Duration
    payload       Payload
    output        chan *output
}

type output struct {
    correlationId string
    result        Result
}

type ExpiringPayload interface {
    ExpiresAfter() time.Duration
}

func newJob(payload Payload) (*job, error) {
    if payload == nil {
        return nil, errors.New("job can't be nil")
    }
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

func newOutput(correlationId string, userResult Result) *output {
    return &output{
        correlationId: correlationId,
        result:        userResult,
    }
}

