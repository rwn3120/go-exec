package exec

import (
    "time"
    "errors"
)

const (
    NeverExpires time.Duration = -1
)

type Job interface {
    CorrelationId() string
}

type ExpiringJob interface {
    CorrelationId() string
    ExpiresAfter() time.Duration
}

type Result interface {
    CorrelationId() string
    Error() error
}

type job struct {
    userJob      Job
    expiresAfter time.Duration
    result       chan Result
}

func newJob(userJob Job) (*job, error) {
    if userJob == nil {
        return nil, errors.New("job can't be nil")
    }
    expiresAfter := NeverExpires
    if expiringJob, ok := userJob.(ExpiringJob); ok {
        expiresAfter = expiringJob.ExpiresAfter()
    }
    return &job{
        userJob:      userJob,
        expiresAfter: expiresAfter,
        result:       make(chan Result)}, nil
}

type simpleResult struct {
    id    string
    error error
}

func (gr *simpleResult) CorrelationId() string {
    return gr.id
}

func (gr *simpleResult) Error() error {
    return gr.error
}

func NewResult(id string, err error) Result {
    return &simpleResult{id, err}
}
