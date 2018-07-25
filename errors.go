package exec

import "fmt"

type ExpiredError struct {
    job Job
}

func (e *ExpiredError) Error() string {
    jobUuid := "unknown"
    if e.job != nil {
        jobUuid = e.job.CorrelationId()
    }
    return fmt.Sprintf("job %s has expired", jobUuid)
}

func (e *ExpiredError) Job() Job {
    return e.job
}