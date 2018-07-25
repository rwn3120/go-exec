package exec

import (
    "fmt"
    "time"
    "github.com/rwn3120/go-conf"
    "github.com/rwn3120/go-logger"
)

const (
    MinimumRegistrationTimeout = 100 * time.Millisecond
    MinimumWorkerHeartbeat     = 500 * time.Millisecond
)

type Configuration struct {
    WorkersCount        int
    WorkerHeartbeat     time.Duration
    RegistrationTimeout time.Duration
    CallbackHeartbeat   time.Duration
    Logger              *logger.Configuration
}

func (c *Configuration) Validate() *[]string {
    var errorList []string

    if c.WorkersCount <= 0 {
        errorList = append(errorList, "Configuration: Workers count must be larger than 0")
    }
    if c.RegistrationTimeout < MinimumRegistrationTimeout {
        errorList = append(errorList, fmt.Sprintf("Configuration: Registration timeout must be larger or equeal to %v", MinimumRegistrationTimeout))
    }
    if c.WorkerHeartbeat < MinimumWorkerHeartbeat {
        errorList = append(errorList, fmt.Sprintf("Configuration: Worker heartbeat must be larger or equeal to %v", MinimumWorkerHeartbeat))
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

type Executor struct {
    name          string
    configuration *Configuration
    jobs          chan *job
    workers       map[string]*Worker
    logger        *logger.Logger
}

func New(name string, configuration *Configuration, factory Factory) *Executor {
    conf.Check(configuration)

    executor := &Executor{
        name:          name,
        configuration: configuration,
        jobs:          make(chan *job, configuration.WorkersCount),
        workers:       make(map[string]*Worker, configuration.WorkersCount),
        logger:        logger.New(name+"-executor", configuration.Logger)}

    // start workers
    for index := 0; index < executor.configuration.WorkersCount; index++ {
        workerUuid := fmt.Sprintf("%s-worker-%d", name, index+1)
        executor.logger.Trace("Creating worker %s (%d/%d)", workerUuid, index+1, configuration.WorkersCount)
        worker := newWorker(
            workerUuid,
            executor.configuration.WorkerHeartbeat,
            executor.configuration.Logger,
            executor.jobs,
            factory)
        executor.logger.Trace("Registering worker %s", workerUuid)
        executor.workers[worker.uuid] = worker
    }
    return executor
}

func (e *Executor) Destroy() {
    unregisteredWorkers := make(chan string, len(e.workers))

    workersCount := len(e.workers)

    for _, worker := range e.workers {
        e.logger.Trace("Waiting for worker %s...", worker.uuid)
        go func(worker *Worker) {
            e.logger.Trace("Killing worker %s...", worker.uuid)
            worker.kill()
            e.logger.Trace("Waiting for worker's %s death...", worker.uuid)
            worker.wait()
            e.logger.Trace("Unregistering worker %s (he is gone)", worker.uuid)
            unregisteredWorkers <- worker.uuid
        }(worker)
    }

    for i := 0; i < workersCount; i++ {
        unregisteredWorkerUUid := <-unregisteredWorkers
        e.logger.Trace("Worker %s unregistered", unregisteredWorkerUUid)
        delete(e.workers, unregisteredWorkerUUid)
    }
    e.logger.Trace("Closing jobs channel")
    close(e.jobs)
    e.logger.Trace("Destroyed")
}

func minExpiration(first time.Duration, second time.Duration) time.Duration {
    if first == NeverExpires {
        return second
    }
    if second == NeverExpires {
        return first
    }

    if first < second {
        return first
    }
    return second
}

func (e *Executor) processResult(job *job, callbacks ...func(result Result)) {
    for {
        select {
        case result := <-job.result:
            if result.Error() == nil {
                e.logger.Trace("Job %s done. Calling %d callbacks", result.CorrelationId(), len(callbacks))
            } else {
                e.logger.Warn("Job %s failed: %s. Calling %d callbacks", result.CorrelationId(), result.Error(), len(callbacks))
            }
            go func() {
                for index, callback := range callbacks {
                    if callback != nil {
                        e.logger.Trace("Running callback function (%d/%d)", index+1, len(callbacks))
                        callback(result)
                    } else {
                        e.logger.Trace("Skipping callback function (%d/%d)", index+1, len(callbacks))
                    }
                }
            }()
            return
        case <-time.After(e.configuration.CallbackHeartbeat):
            e.logger.Trace("Waiting for result of job %s", job.userJob.CorrelationId())
        }
    }
}

func (e *Executor) registerAndFire(job *job, callbacks ...func(result Result)) {
    e.logger.Trace("Registering job %s with %d callbacks", job.userJob.CorrelationId(), len(callbacks))
    registeredAt := time.Now()
    for job.expiresAfter == NeverExpires || time.Since(registeredAt) < job.expiresAfter {
        select {
        case e.jobs <- job:
            e.logger.Trace("Job %s has been fired", job.userJob.CorrelationId())
            e.processResult(job, callbacks...)
            return
        case <-time.After(minExpiration(e.configuration.RegistrationTimeout, job.expiresAfter)):
            e.logger.Trace("Job %s has not been registered yet. Waiting for free worker", job.userJob.CorrelationId())
        }
    }
    e.logger.Warn("Job %s has expired", job.userJob.CorrelationId())
    job.result <- NewResult(job.userJob.CorrelationId(), &ExpiredError{job: job.userJob})
}

func (e *Executor) Fire(job Job, callbacks ...func(result Result)) error {
    j, err := newJob(job)
    if err != nil {
        return err
    }
    go e.registerAndFire(j, callbacks...)
    return nil
}

func (e *Executor) FireAndForget(job Job) error {
    return e.Fire(job, func(result Result) {
        // dummy callback
    })
}

func (e *Executor) Execute(job Job) Result {
    j, err := newJob(job)
    if err != nil {
        return NewResult("", err)
    }
    channel := make(chan Result)
    e.registerAndFire(j, func(result Result) {
        channel <- result
    })
    e.logger.Trace("Waiting for result of job %s", job.CorrelationId())
    result := <-channel
    e.logger.Trace("Received result of job %s", job.CorrelationId())
    return result
}
