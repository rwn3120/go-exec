package exec

type Handler interface {
    Initialize() error

    Destroy()

    Handle(job Job) Result
}