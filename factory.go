package exec

type Factory interface {
    Handler(uuid string) Handler
}