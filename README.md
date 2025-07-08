# queuer

[![Go Reference](https://pkg.go.dev/badge/github.com/siherrmann/queuer.svg)](https://pkg.go.dev/github.com/siherrmann/queuer)
[![Go Coverage](https://github.com/siherrmann/queuer/wiki/coverage.svg)](https://raw.githack.com/wiki/siherrmann/queuer/coverage.html)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/siherrmann/queuer/blob/master/LICENSE)

Queueing package based on postgres written in Go.

## 💡 Goal of this package

This queuer is meant to be as easy as possible to use. No specific function signature (except for an error as the last output parameter, if you want to give back an error), easy setup and still fast.

The job table contains only queued, scheduled and running tasks. The ended jobs (succeeded, cancelled, failed) are moved to a timescaleDB table.

---

## 🚀 Getting started

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

## NewQueuer

`NewQueuer` is a convenience constructor that creates a new Queuer instance using default database configuration derived from environment variables. It acts as a wrapper around `NewQueuerWithDB`.
`NewQueuerWithDB` is the primary constructor for creating a new Queuer instance. It allows for explicit database configuration and initializes all necessary components, including database handlers, internal event listeners, and the worker.

```go
func NewQueuer(name string, maxConcurrency int, options ...*model.OnError) *Queuer

func NewQueuerWithDB(name string, maxConcurrency int, dbConfig *helper.DatabaseConfiguration, options ...*model.OnError) *Queuer
```

- `name`: A `string` identifier for this queuer instance.
- `maxConcurrency`: An `int` specifying the maximum number of jobs this queuer can process concurrently.
- `dbConfig`: An optional `*helper.DatabaseConfiguration`. If nil, the configuration will be loaded from environment variables.
- `options`: Optional `OnError` configurations to apply to the worker.

This function performs the following setup:
- Initializes a logger.
- Sets up the database connection using the provided `dbConfig` or environment variables.
- Creates `JobDBHandler` and `WorkerDBHandler` instances for database interactions.
- Initializes internal `core.Listener` instances for `jobInsert`, `jobUpdate`, and `jobDelete` events.
- Creates and inserts a new `model.Worker` into the database based on the provided `name`, `maxConcurrency`, and `options`.
- If any critical error occurs during this initialization (e.g., database connection failure, worker creation error), the function will log a panic error and exit the program. It returns a pointer to the newly configured `Queuer` instance.

---

## Start

The Start method initiates the operational lifecycle of the Queuer. It sets up the main context, initializes database listeners, and begins the job processing and polling loops in a dedicated goroutine.

```go
func (q *Queuer) Start(ctx context.Context, cancel context.CancelFunc)
```

- `ctx`: The parent context.Context for the queuer's operations. This context will control the overall lifetime of the queuer.
- `cancel`: The context.CancelFunc associated with the provided ctx. This function should be called to gracefully stop the queuer.

Upon calling Start:
- It performs a basic check to ensure internal listeners are initialized.
- Db listeners and broadcasters are created to listen to job events (inserts, updates, deletes).
- It starts a poller to periodically poll the database for new jobs to process (5 minute interval).
- It signals its readiness via an internal channel, ensuring the `Start` method returns only when the core loops are active.

The method includes a timeout mechanism (5 seconds) to detect if the queuer fails to start its internal processes promptly, panicking if the timeout is exceeded.
If the queuer is not not properly initialized (created by calling `NewQueuer`), or if there's an error creating the database listeners, the function will panic.

---

## StartWithoutWorker

The `StartWithoutWorker` method provides a way to start the `Queuer` instance without an active worker. This is particularly useful for scenarios where you need to interact with the job queue (e.g., add jobs, check job status) but don't intend for this specific instance to actively process them.

```go
func (q *Queuer) StartWithoutWorker(ctx context.Context, cancel context.CancelFunc, withoutListeners bool, dbConnection ...*sql.DB)
```

- `ctx`: The parent context.Context for the queuer's operations.
- `cancel`: The context.CancelFunc associated with the provided ctx.
- `withoutListeners`: A `bool` flag. If true, the database.NewQueuerDBListener instances for job and job_archive tables will not be created.
- `dbConnection`: An optional existing `*sql.DB` connection to use. If provided, the queuer will use this connection; otherwise, it will create a new one based on environment variables.

---

## Stop

The `Stop` method gracefully shuts down the Queuer instance, releasing resources and ensuring ongoing operations are properly concluded.

```go
func (q *Queuer) Stop() error
```

The `Stop` method cancels all jobs, closes db listeners and returns an error if any step of the stopping process encounters an issue

---

## Add Task

The `AddTask` method registers a new job task with the queuer. A task is the actual function that will be executed when a job associated with it is processed.

```go
func (q *Queuer) AddTask(task interface{}) *model.Task

func (q *Queuer) AddTaskWithName(task interface{}, name string) *model.Task
```

- `task`: An `interface{}` representing the function that will serve as the job's executable logic. The queuer will automatically derive a name for this task based on its function signature (e.g., `main.MyTaskFunction`). The derived name must be unique if no `name` is given.
- `name`: A `string` specifying the custom name for this task. This name must be unique within the queuer's tasks.

This method handles the registration of a task, making the worker able to pick up and execute a job of this task type. It also updates the worker's available tasks in the database. The task should be added before starting the queuer. If there's an issue during task creation or database update, the program will panic.

---

## Add NextIntervalFunc

The `AddNextIntervalFunc` method registers a custom function that determines the next execution time for scheduled jobs. This is useful for implementing complex scheduling logic beyond simple fixed intervals.

```go
func (q *Queuer) AddNextIntervalFunc(nif model.NextIntervalFunc) *model.Worker

func (q *Queuer) AddNextIntervalFuncWithName(nif model.NextIntervalFunc, name string) *model.Worker
```

- `nif`: An instance of `model.NextIntervalFunc`, which is a function type defining custom logic for calculating the next interval. The queuer will automatically derive a name for this function. The derived name must be unique if no `name` is given.
- `name`: A string specifying the custom name for this `NextIntervalFunc`. This name must be unique within the queuer's NextIntervalFuncs.

This method adds the provided `NextIntervalFunc` to the queuer's available functions, making it usable for jobs with custom scheduling requirements. It updates the worker's configuration in the database. If `nif` is nil, if the function name cannot be derived, or if a function with the same name already exists, the program will panic.

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
    MaxCount     int             `json:"max_count"`
    Interval     time.Duration   `json:"interval"`
    NextInterval string          `json:"next_interval"`
}
```

- `Start`: The initial time at which the scheduled job should first run.
- `MaxCount`: The maximum number of times the job should be executed. A value 0 indicates an indefinite number of repetitions (run forever).
- `Interval`: The duration between consecutive executions of the scheduled job.
- `NextInterval`: Function name of the `NextIntervalFunc` returning the time of the next execution of the scheduled job. **Either `Interval` or `NextInterval` have to be set if the `MaxCount` is 0 or greater 1.**

---

# ⭐ Features

- Insert job batches using the `COPY FROM` postgres feature.
- Insert a job in a transaction to rollback if eg. the step after job insertion fails.
- Panic recovery for all running jobs.
- Error handling by checking last output parameter for error.
- Multiple queuers can be started in different microservices while maintaining job start order and isolation.
- Scheduled and periodic jobs.
- Easy functions to get jobs and workers.
- Listener functions for job updates and deletion (ended jobs).
- Helper function to listen for a specific finished job.
- Retry mechanism for ended jobs which creates a new job with the same parameters.
- Custom NextInterval functions to address custom needs for scheduling (eg. scheduling with timezone offset)
