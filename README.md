# queuer

[![Go Reference](https://pkg.go.dev/badge/github.com/siherrmann/queuer.svg)](https://pkg.go.dev/github.com/siherrmann/queuer)
[![Go Coverage](https://github.com/siherrmann/queuer/wiki/coverage.svg)](https://raw.githack.com/wiki/siherrmann/queuer/coverage.html)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/siherrmann/queuer/blob/master/LICENSE)

Queueing package based on postgres written in Go.

## Goal of this package

This queuer is meant to be as easy as possible to use. No specific function signature (except for an error as the last output parameter, if you want to give back an error), easy setup and still fast.

The job table contains only queued, scheduled and running tasks. The ended jobs (succeeded, cancelled, failed) are moved to a timescaleDB table.

---

## Getting started

The full initialisation is (in the easiest case):

```go
// Create a new queuer instance
q := queuer.NewQueuer("exampleWorker", 3)

// Add a task to the queuer
q.AddTask(ExampleTask)

// Start the queuer
ctx, cancel := context.WithCancel(context.Background())
defer cancel()
q.Start(ctx, cancel)
```

That's easy, right? Adding a job is just as easy:

```go
// Add a job to the queue
_, err := q.AddJob(ExampleTask, 5, "12")
if err != nil {
    log.Fatalf("Error adding job: %v", err)
}
```

In the initialisation of the queuer the existance of the necessary database tables is checked and if they don't exist they get created. The database is configured with these environment variables:

```shell
QUEUER_DB_HOST=localhost
QUEUER_DB_PORT=5432
QUEUER_DB_DATABASE=postgres
QUEUER_DB_USERNAME=username
QUEUER_DB_PASSWORD=password1234
QUEUER_DB_SCHEMA=public
```

You can find a full example (the same as above plus a more detailed example) in the example folder. In there you'll also find a docker-compose file with the timescaleDB/postgres service that is needed for the running the queuer (it's just postgres with an extension).

---

## New queuer

The `NewQueuer` function initializes a new Queuer instance, setting up the core components for job processing.

```go
func NewQueuer(name string, maxConcurrency int, opts ...WorkerOption) *Queuer
```

- `name`: A string identifier for this specific queuer instance. This is useful for distinguishing between multiple queuers in your application or logs.
- `maxConcurrency`: An integer representing the maximum number of jobs that this queuer can process concurrently. This controls the worker's parallelism.
- `opts ...WorkerOption`: Optional `WorkerOption` configurations that allow you to customize the worker's behavior, such as error handling strategies.

This function handles the entire setup process: it establishes the database connection, configures the necessary job listeners, and creates an associated worker. If any part of this initialization fails, `NewQueuer` will log a panic error and exit the program to prevent an improperly configured queuer from running. It returns a pointer to the newly created `Queuer` instance, ready to accept and process jobs.

---

## New queuer without worker

The `NewQueuerWithoutWorker` function provides a way to initialize a `Queuer` instance without an active worker. This is particularly useful for scenarios where you need to interact with the job queue (e.g., add jobs, check job status) but don't intend for this specific instance to actively process them.

```go
func NewQueuerWithoutWorker() *Queuer
```

This function only initializes the database connection. It omits the worker component, making it suitable for services that might, for example, serve job status endpoints or solely add jobs to the queue, without consuming computational resources for job execution. Similar to `NewQueuer`, any initialization errors will result in a panic and program exit. It returns a pointer to the newly created `Queuer` instance.

---

## Worker Options

The OnError struct defines how a worker should handle errors when processing a job. This allows for configurable retry behavior.

```go
type OnError struct {
    Timeout      float64 `json:"timeout"`
    MaxRetries   int     `json:"max_retries"`
    RetryDelay   float64 `json:"retry_delay"`
    RetryBackoff string  `json:"retry_backoff"`
}
```

- `Timeout`: The maximum time (in seconds) allowed for a single attempt of a job. If the job exceeds this duration, it's considered to have timed out.
- `MaxRetries`: The maximum number of times a job will be retried after a failure.
- `RetryDelay`: The initial delay (in seconds) before the first retry attempt. This delay can be modified by the `RetryBackoff` strategy.
- `RetryBackoff`: Specifies the strategy used to increase the delay between subsequent retries.

#### Retry Backoff Strategies

The RetryBackoff constant defines the available strategies for increasing retry delays:

```go
const (
    RETRY_BACKOFF_NONE        = "none"
    RETRY_BACKOFF_LINEAR      = "linear"
    RETRY_BACKOFF_EXPONENTIAL = "exponential"
)
```

- `RETRY_BACKOFF_NONE`: No backoff. The RetryDelay remains constant for all retries.
- `RETRY_BACKOFF_LINEAR`: The retry delay increases linearly with each attempt (e.g., delay, 2*delay, 3*delay).
- `RETRY_BACKOFF_EXPONENTIAL`: The retry delay increases exponentially with each attempt (e.g., delay, delay*2, delay*2*2).

---

## Job options

Job Options
The Options struct allows you to define specific behaviors for individual jobs, overriding default worker settings where applicable.

```go
type Options struct {
    OnError  *OnError
    Schedule *Schedule
}
```

- `OnError`: An optional `OnError` configuration that will override the worker's default error handling for this specific job. This allows you to define unique retry logic per job.
- `Schedule`: An optional `Schedule` configuration for jobs that need to be executed at recurring intervals.

### OnError for jobs

OnError for Jobs
The OnError struct for jobs is identical to the one used for worker options, allowing granular control over error handling for individual jobs.

```go
type OnError struct {
    Timeout      float64 `json:"timeout"`
    MaxRetries   int     `json:"max_retries"`
    RetryDelay   float64 `json:"retry_delay"`
    RetryBackoff string  `json:"retry_backoff"`
}
```

### Schedule

The Schedule struct is used to define recurring jobs.

```go
type Schedule struct {
    Start        time.Time       `json:"start"`
    Interval     time.Duration   `json:"interval"`
    MaxCount     int             `json:"max_count"`
}
```

- `Start`: The initial time at which the scheduled job should first run.
- `Interval`: The duration between consecutive executions of the scheduled job.
- `MaxCount`: The maximum number of times the job should be executed. A value 0 indicates an indefinite number of repetitions (run forever).

---

# Features

- Insert job batches using the `COPY FROM` postgres feature.
- Panic recovery for all running jobs.
- Error handling by checking last output parameter for error.
- Multiple queuers can be started in different microservices while maintaining job start order and isolation.
- Scheduled and periodic jobs.
- Easy functions to get jobs and workers.
- Retry mechanism for ended jobs which creates a new job with the same parameters.

## Coming soon

- Add a new job in a transaction to rollback if for example step after job insertion fails.
- Helper function to listen for a specific finished job.
- Custom NextInterval functions to address custom needs for scheduling (eg. scheduling with timezone offset)
