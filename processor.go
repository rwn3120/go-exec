package exec

type Processor interface {
    Initialize() error

    Process(payload Payload) Result

    Destroy()
}