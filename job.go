package gojobs

type Job interface {
    CorrelationId() string
    ResultChannel() *chan Result
}

type Result interface {
    CorrelationId() string
    Error() error
}

type SimpleResult struct {
    id    string
    error error
}

func NewResult(id string, err error) *SimpleResult {
    return &SimpleResult{id, err}
}

func (gr *SimpleResult) CorrelationId() string {
    return gr.id
}

func (gr *SimpleResult) Err() error {
    return gr.error
}
