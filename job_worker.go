package gojobs

import (
    "time"
    "github.com/rwn3120/gologger"
    "github.com/rwn3120/goconf"
)

type Status int
type Signal int

const (
    Alive  Status = 1
    Zombie Status = 2

    KillSignal Signal = 9
)

type Worker struct {
    uuid       string
    jobs       chan Job
    signals    chan Signal
    done       chan bool
    status     Status
    jobHandler JobHandler
    logger     *gologger.Logger
}

func createWorker(uuid string, loggerConfiguration *gologger.LoggerConfiguration, jobs chan Job, jobHandlerBuilder JobHandlerFactory) *Worker {
    goconf.Check(loggerConfiguration)

    worker := &Worker{
        uuid:       uuid,
        jobs:       jobs,
        signals:    make(chan Signal, 1),
        done:       make(chan bool, 1),
        status:     Alive,
        jobHandler: jobHandlerBuilder.JobHandler(uuid),
        logger:     gologger.NewLogger(uuid, loggerConfiguration)}
    go worker.run()
    return worker
}

func (w *Worker) isAlive() bool {
    return w.status == Alive
}

func (w *Worker) kill() {
    w.logger.Trace("Sending kill signal to worker %s...", w.uuid)
    w.signals <- KillSignal
}

func (w *Worker) wait() bool {
    return <-w.done
}

func (w *Worker) die() {
    if w.isAlive() {
        defer w.jobHandler.Destroy()
        w.logger.Trace("Dying...")
        <-time.After(time.Second)
        w.status = Zombie
        w.done <- true
        close(w.done)
        w.logger.Trace("Become a zombie...")
    }
}

func (w *Worker) run() {
    defer w.die()
    if err:= w.jobHandler.Initialize(); err != nil {
        w.logger.Error("Could not initialize worker: %s", err.Error())
    }

runLoop:
    for jobCounter := 0; w.isAlive(); {
        select {
        // process signals
        case signal := <-w.signals:
            w.logger.Trace("Handling signal %d", signal)
            switch signal {
            case KillSignal:
                w.logger.Trace("Killed")
                break runLoop
            default:
                w.logger.Warn("Unknown signal (%d) received", signal)
            }

            // process jobs
        case job, more := <-w.jobs:
            if more {
                jobCounter++
                w.logger.Trace("Received job %v #%06d", job.correlationId(), jobCounter)
                w.jobHandler.Handle(job)
            } else {
                w.logger.Trace("Received all jobs")
                break runLoop
            }
            jobCounter++
        case <-time.After(w.jobHandler.HeartBeat()):
            w.logger.Trace("Nothing to do")
        }
    }
    w.logger.Trace("Finished")
}
