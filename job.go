package gojobs

type Job interface {
    CorrelationId() string
    ResultChannel() *chan JobResult
}

type JobResult interface {
    CorrelationId() string
    Err() error
}

type SimpleJobResult struct {
    id    string
    error error
}

func CreateJobResult(id string, err error) *SimpleJobResult {
    return &SimpleJobResult{id, err}
}

func (gr *SimpleJobResult) CorrelationId() string {
    return gr.id
}

func (gr *SimpleJobResult) Err() error {
    return gr.error
}
