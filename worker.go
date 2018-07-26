package exec

import (
    "time"
    "github.com/rwn3120/go-logger"
    "github.com/rwn3120/go-conf"
)

type Status int
type Signal int

const (
    Alive  Status = 1
    Zombie Status = 2

    KillSignal Signal = 9
)

type Worker struct {
    uuid      string
    heartbeat time.Duration
    jobs      chan *job
    signals   chan Signal
    done      chan bool
    status    Status
    processor Processor
    logger    *logger.Logger
}

func newWorker(uuid string, heartbeat time.Duration, logging *logger.Configuration, jobs chan *job, factory Factory) *Worker {
    conf.Check(logging)

    worker := &Worker{
        uuid:      uuid,
        heartbeat: heartbeat,
        jobs:      jobs,
        signals:   make(chan Signal, 1),
        done:      make(chan bool, 1),
        status:    Alive,
        processor: factory.Processor(uuid),
        logger:    logger.New(uuid, logging)}
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
        defer w.processor.Destroy()
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
    if err := w.processor.Initialize(); err != nil {
        w.logger.Error("Could not initialize worker: %s", err.Error())
    }

runLoop:
    for counter := 0; w.isAlive(); {
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
                counter++
                w.logger.Trace("Received job %v #%06d", job.correlationId, counter)
                result := w.processor.Process(job.payload)
                w.logger.Trace("Reporting result of job %v #%06d", job.correlationId, counter)
                job.output <- newOutput(job.correlationId, result)
            } else {
                w.logger.Trace("Received all jobs")
                break runLoop
            }
            counter++
        case <-time.After(w.heartbeat):
            w.logger.Trace("Nothing to do")
        }
    }
    w.logger.Trace("Finished")
}
