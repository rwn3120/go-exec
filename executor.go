package exec

import (
    "fmt"
    "time"
    "github.com/rwn3120/go-conf"
    "github.com/rwn3120/go-logger"
)

const (
    MinimumPullInterval = time.Millisecond
)

type Configuration struct {
    WorkersCount          int
    JobPullInterval       time.Duration
    CallbackRetryInterval time.Duration
    Logger                *logger.Configuration
}

func (c *Configuration) Validate() *[]string {
    var errorList []string

    if c.WorkersCount <= 0 {
        errorList = append(errorList, "Configuration: Workers count must be larger than 0")
    }
    if c.JobPullInterval < MinimumPullInterval {
        errorList = append(errorList, fmt.Sprintf("Configuration: Job pull interval must be larger or equeal to %v", MinimumPullInterval))
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
    name              string
    configuration     *Configuration
    jobs              chan Job
    workers           map[string]*Worker
    logger            *logger.Logger
}

func New(name string, configuration *Configuration, factory Factory) *Executor {
    conf.Check(configuration)

    executor := &Executor{
        name:              name,
        configuration:     configuration,
        jobs:              make(chan Job, configuration.WorkersCount),
        workers:           make(map[string]*Worker, configuration.WorkersCount),
        logger:            logger.NewLogger(name + "-backend", configuration.Logger)}

    // start workers
    for index := 0; index < configuration.WorkersCount; index++ {
        workerUuid := fmt.Sprintf("%s-worker-%d", name, index+1)
        executor.logger.Trace("Creating %s", workerUuid)
        worker := createWorker(
            workerUuid,
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
            delete(e.workers, worker.uuid)
            unregisteredWorkers <- worker.uuid
        }(worker)
    }

    for i := 0; i < workersCount; i++ {
        unregisteredWorkerUUid := <-unregisteredWorkers
        e.logger.Trace("Worker %s unregistered", unregisteredWorkerUUid)
    }
    e.logger.Trace("Closing jobs channel")
    close(e.jobs)
    e.logger.Trace("Destroyed")
}

func (e *Executor) exec(job Job, callback func(result Result)) {
    for {
        select {
        case result := <-*job.ResultChannel():
            go func() {
                e.logger.Trace("Calling callback function")
                defer e.logger.Trace("Callback function done")
                callback(result)
            }()
            return
        case <-time.After(e.configuration.CallbackRetryInterval):
            e.logger.Trace("Waiting for result of job %s", job.CorrelationId())
        }
    }
}

func (e *Executor) ExecWithCallback(job Job, callback func(result Result)) Result {
    for {
        select {
        case e.jobs <- job:
            if job.ResultChannel() != nil {
                e.exec(job, callback)
            } else {
                e.logger.Trace("Job %s has no result channel", job.CorrelationId())
            }
            return nil
        case <-time.After(e.configuration.JobPullInterval):
            e.logger.Trace("Waiting for free worker for job %s", job.CorrelationId())
        }
    }
}

func (e *Executor) Exec(job Job) Result {
    jobResult := make(chan Result)
    callback := func(result Result) {
        e.logger.Trace("Internal callback done, passing result of job %s to manager", result.CorrelationId())
        jobResult <- result
    }
    e.ExecWithCallback(job, callback)
    e.logger.Trace("Waiting for result of job %s", job.CorrelationId())
    result := <- jobResult
    e.logger.Trace("Received result of job %s", job.CorrelationId())
    return result
}
