package exec

type Job interface {
    CorrelationId() string
    ResultChannel() *chan Result
}

type Result interface {
    CorrelationId() string
    Error() error
}

type GenericResult struct {
    id    string
    error error
}

func NewResult(id string, err error) *GenericResult {
    return &GenericResult{id, err}
}

func (gr *GenericResult) CorrelationId() string {
    return gr.id
}

func (gr *GenericResult) Error() error {
    return gr.error
}
