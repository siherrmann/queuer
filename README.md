# queuer

[![Go Reference](https://pkg.go.dev/badge/github.com/siherrmann/queuer.svg)](https://pkg.go.dev/github.com/siherrmann/queuer)
[![Go Coverage](https://github.com/siherrmann/queuer/wiki/coverage.svg)](https://raw.githack.com/wiki/siherrmann/queuer/coverage.html)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/siherrmann/queuer/blob/master/LICENSE)

Queueing package based on postgres written in Go.

## Goal of this package

This queuer is meant to be as easy as possible to use. No specific function signature (except for an error as the last output parameter, if you want to give back an error), easy setup and still fast.

The job table contains only queued, scheduled and running tasks. The ended jobs (succeeded, cancelled, failed) are moved to a timescaleDB table.

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