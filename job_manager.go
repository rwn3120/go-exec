package gojobs

import (
    "fmt"
    "time"
    "github.com/rwn3120/goconf"
    "github.com/rwn3120/gologger"
)

const (
    MinimumPullInterval = time.Millisecond
)

type JobManagerConfiguration struct {
    WorkersCount          int
    JobPullInterval       time.Duration
    CallbackRetryInterval time.Duration
    Logger                *gologger.LoggerConfiguration
}

func (c *JobManagerConfiguration) Validate() *[]string {
    var errorList []string

    if c.WorkersCount <= 0 {
        errorList = append(errorList, "JobManagerConfiguration: Workers count must be larger than 0")
    }
    if c.JobPullInterval < MinimumPullInterval {
        errorList = append(errorList, fmt.Sprintf("JobManagerConfiguration: Job pull interval must be larger or equeal to %v", MinimumPullInterval))
    }
    if errorsCount := len(errorList); errorsCount > 0 {
        return &errorList
    }

    otherErrors := goconf.Validate(c.Logger)
    if otherErrors != nil {
        errorList = append(errorList, *otherErrors...)
    }

    return nil
}

type JobManager struct {
    name              string
    configuration     *JobManagerConfiguration
    jobs              chan Job
    workers           map[string]*Worker
    logger            *gologger.Logger
}

func CreateJobManager(name string, configuration *JobManagerConfiguration, jobHandlerBuilder JobHandlerFactory) *JobManager {
    goconf.Check(configuration)

    manager := &JobManager{
        name:              name,
        configuration:     configuration,
        jobs:              make(chan Job, configuration.WorkersCount),
        workers:           make(map[string]*Worker, configuration.WorkersCount),
        logger:            gologger.NewLogger(name + "-backend", configuration.Logger)}

    // start workers
    for index := 0; index < configuration.WorkersCount; index++ {
        workerUuid := fmt.Sprintf("%s-worker-%d", name, index+1)
        manager.logger.Trace("Creating %s", workerUuid)
        worker := createWorker(
            workerUuid,
            manager.configuration.Logger,
            manager.jobs,
            jobHandlerBuilder)
        manager.logger.Trace("Registering worker %s", workerUuid)
        manager.workers[worker.uuid] = worker
    }
    return manager
}

func (m *JobManager) Destroy() {
    unregisteredWorkers := make(chan string, len(m.workers))

    workersCount := len(m.workers)

    for _, worker := range m.workers {
        m.logger.Trace("Waiting for worker %s...", worker.uuid)
        go func(worker *Worker) {
            m.logger.Trace("Killing worker %s...", worker.uuid)
            worker.kill()
            m.logger.Trace("Waiting for worker's %s death...", worker.uuid)
            worker.wait()
            m.logger.Trace("Unregistering worker %s (he is gone)", worker.uuid)
            delete(m.workers, worker.uuid)
            unregisteredWorkers <- worker.uuid
        }(worker)
    }

    for i := 0; i < workersCount; i++ {
        unregisteredWorkerUUid := <-unregisteredWorkers
        m.logger.Trace("Worker %s unregistered", unregisteredWorkerUUid)
    }
    m.logger.Trace("Closing jobs channel")
    close(m.jobs)
    m.logger.Trace("Destroyed")
}

func (m *JobManager) process(job Job, callback func(result JobResult)) {
    for {
        select {
        case result := <-*job.ResultChannel():
            go func() {
                m.logger.Trace("Calling callback function")
                defer m.logger.Trace("Callback function done")
                callback(result)
            }()
            return
        case <-time.After(m.configuration.CallbackRetryInterval):
            m.logger.Trace("Waiting for result of job %s", job.CorrelationId())
        }
    }
}

func (m *JobManager) PerformWithCallback(job Job, callback func(result JobResult)) JobResult {
    for {
        select {
        case m.jobs <- job:
            if job.ResultChannel() != nil {
                m.process(job, callback)
            } else {
                m.logger.Trace("Job %s has no result channel", job.CorrelationId())
            }
            return nil
        case <-time.After(m.configuration.JobPullInterval):
            m.logger.Trace("Waiting for free worker for job %s", job.CorrelationId())
        }
    }
}

func (m *JobManager) Perform(job Job) JobResult {
    jobResult := make(chan JobResult)
    callback := func(result JobResult) {
        m.logger.Trace("Internal callback done, passing result of job %s to manager", result.CorrelationId())
        jobResult <- result
    }
    m.PerformWithCallback(job, callback)
    m.logger.Trace("Waiting for result of job %s", job.CorrelationId())
    result := <- jobResult
    m.logger.Trace("Received result of job %s", job.CorrelationId())
    return result
}
