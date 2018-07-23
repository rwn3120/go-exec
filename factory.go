package gojobs

type Factory interface {
    Handler(uuid string) Handler
}