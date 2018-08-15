package exec

import (
    "time"
    "github.com/rwn3120/go-logger"
    "fmt"
    "github.com/rwn3120/go-conf"
    me "github.com/rwn3120/go-multierror"
    "errors"
)

const (
    MinimumHeartbeat = 100 * time.Millisecond
)

type Configuration struct {
    Workers   int
    Heartbeat time.Duration
    Logger    *logger.Configuration
}

func (c *Configuration) Validate() []error {
    multiError := me.New()

    if c.Workers <= 0 {
        multiError.Add(errors.New("configuration: Workers count must be larger than 0"))
    }
    if c.Heartbeat < MinimumHeartbeat {
        multiError.Add(errors.New(fmt.Sprintf("configuration: Heartbeat must be larger or equeal to %v", MinimumHeartbeat)))
    }

    multiError.Add(conf.Validate(c.Logger)...)

    return multiError.ErrorsOrNil()
}
