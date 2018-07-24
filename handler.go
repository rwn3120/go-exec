package exec

import "time"

type Handler interface {
    Initialize() error

    Destroy()

    HeartBeat() time.Duration

    Handle(job Job)
}