package exec

type Factory interface {
    Processor(uuid string) Processor
}