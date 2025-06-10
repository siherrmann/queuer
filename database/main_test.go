package database

import (
	"context"
	"log"
	"queuer/helper"
	"testing"

	"github.com/testcontainers/testcontainers-go"
)

var port string

func TestMain(m *testing.M) {
	var teardown func(ctx context.Context, opts ...testcontainers.TerminateOption) error
	var err error
	teardown, port, err = helper.MustStartPostgresContainer()
	if err != nil {
		log.Fatalf("error starting postgres container: %v", err)
	}

	m.Run()

	if teardown != nil && teardown(context.Background()) != nil {
		log.Fatalf("error tearing down postgres container: %v", err)
	}
}
