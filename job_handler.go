package gojobs

import "time"

type JobHandler interface {
    Initialize() error

    Destroy()

    HeartBeat() time.Duration

    Handle(task Job)
}

type JobHandlerFactory interface {
    JobHandler(uuid string) JobHandler
}