package gojobs

import "time"

type Handler interface {
    Initialize() error

    Destroy()

    HeartBeat() time.Duration

    Handle(task Job)
}