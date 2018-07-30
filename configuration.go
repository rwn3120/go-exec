package exec

import (
    "time"
    "github.com/rwn3120/go-logger"
    "fmt"
    "github.com/rwn3120/go-conf"
)

const (
    MinimumHeartbeat = 100 * time.Millisecond
)

type Configuration struct {
    Workers   int
    Heartbeat time.Duration
    Logger    *logger.Configuration
}

func (c *Configuration) Validate() *[]string {
    var errorList []string

    if c.Workers <= 0 {
        errorList = append(errorList, "Configuration: Workers count must be larger than 0")
    }
    if c.Heartbeat < MinimumHeartbeat {
        errorList = append(errorList, fmt.Sprintf("Configuration: Heartbeat must be larger or equeal to %v", MinimumHeartbeat))
    }
    if errorsCount := len(errorList); errorsCount > 0 {
        return &errorList
    }

    otherErrors := conf.Validate(c.Logger)
    if otherErrors != nil {
        errorList = append(errorList, *otherErrors...)
    }

    return nil
}
