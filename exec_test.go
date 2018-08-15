package exec

import (
    "testing"
    "fmt"
    "time"
    "github.com/rwn3120/go-logger"
    "strconv"
)

type exampleResult struct {
    data string
    err error
}

func (tr *exampleResult) Err() error {
    return tr.err
}

type examplePayload struct {
    data string
}

type exampleProcessor struct {
    id int
}

func (tp *exampleProcessor) Initialize() error {
    println("processor", tp.id, "initializing")
    return nil
}

func (tp *exampleProcessor) Process(payload Payload) Result {
    testPayload := payload.(*examplePayload)
    println("processor", tp.id, "processing payload:", testPayload.data)
    <-time.After(100 * time.Millisecond)
    return &exampleResult{
        data: "successfully processed " + testPayload.data,
        err:  nil,
    }
}

func (tp *exampleProcessor) Destroy() {
    println("processor", tp.id, "destroying")
}

type exampleFactory struct {
    count int
}

func (pf *exampleFactory) Processor() Processor {
    pf.count++
    return &exampleProcessor{pf.count}
}

func TestExecutor(t *testing.T) {
    fmt.Println(t.Name(), "... running")

    // Create executor
    executor, err := New("executor", &Configuration{
        Workers:   10,
        Heartbeat: 200 * time.Millisecond,
        Logger:    &logger.Configuration{Level: "DEBUG"}},
        &exampleFactory{})
    if err != nil {
        t.Fatal(err.Error())
    }

    // Fire and forget 100 jobs
    for i := 0; i < 100; i++ {
        err := executor.FireAndForgetJob(&examplePayload{"FireAndForgetJob-"+strconv.Itoa(i)})
        if err != nil {
            t.Fatal(err.Error())
        }
    }

    // Fire 100 jobs
    for i := 0; i < 100; i++ {
        err := executor.FireJob(&examplePayload{"FireJob-"+strconv.Itoa(i)}, func(result Result) {
            testResult := result.(*exampleResult)
            println("Result (callback):", testResult.data)
        })
        if err != nil {
            t.Fatal(err.Error())
        }
    }

    // Execute 100 jobs
    for i := 0; i < 100; i++ {
        result, err := executor.ExecuteJob(&examplePayload{"ExecuteJob-"+strconv.Itoa(i)})
        if err != nil {
            t.Fatal(err.Error())
        }
        if result.Err() != nil {
            t.Error(result.Err().Error())
        }
    }

    // Destroy executor
    executor.Destroy()
}
