package gojobs

type Job interface {
    correlationId() string
    resultChannel() *chan JobResult
}

type JobResult interface {
    correlationId() string
    err() error
}

type SimpleJobResult struct {
    id    string
    error error
}

func CreateJobResult(id string, err error) *SimpleJobResult {
    return &SimpleJobResult{id, err}
}

func (gr *SimpleJobResult) correlationId() string {
    return gr.id
}

func (gr *SimpleJobResult) err() error {
    return gr.error
}
