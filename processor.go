package exec

type Processor interface {
    // called once
    Initialize() error
    // called for every payload
    Process(payload Payload) Result
    // called once
    Destroy()
}

type ProcessorFactory interface {
    // called for every processor
    Processor() Processor
}
