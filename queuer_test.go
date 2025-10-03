package queuer

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/siherrmann/queuer/helper"
	"github.com/siherrmann/queuer/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewQueuer(t *testing.T) {
	tests := []struct {
		name           string
		maxConcurrency int
		options        []*model.OnError
		dbEnvs         map[string]string
		expectError    bool
	}{
		{
			name:           "Valid queuer",
			maxConcurrency: 100,
			options:        nil,
			dbEnvs: map[string]string{
				"QUEUER_DB_HOST":     "localhost",
				"QUEUER_DB_PORT":     dbPort,
				"QUEUER_DB_DATABASE": "database",
				"QUEUER_DB_USERNAME": "user",
				"QUEUER_DB_PASSWORD": "password",
				"QUEUER_DB_SCHEMA":   "public",
			},
			expectError: false,
		},
		{
			name:           "Valid queuer with options",
			maxConcurrency: 100,
			options: []*model.OnError{
				{
					Timeout:      10.0,
					MaxRetries:   3,
					RetryDelay:   1.0,
					RetryBackoff: model.RETRY_BACKOFF_LINEAR,
				},
			},
			dbEnvs: map[string]string{
				"QUEUER_DB_HOST":     "localhost",
				"QUEUER_DB_PORT":     dbPort,
				"QUEUER_DB_DATABASE": "database",
				"QUEUER_DB_USERNAME": "user",
				"QUEUER_DB_PASSWORD": "password",
				"QUEUER_DB_SCHEMA":   "public",
			},
			expectError: false,
		},
		{
			name:           "Invalid max concurrency",
			maxConcurrency: -1,
			options:        nil,
			dbEnvs: map[string]string{
				"QUEUER_DB_HOST":     "localhost",
				"QUEUER_DB_PORT":     dbPort,
				"QUEUER_DB_DATABASE": "database",
				"QUEUER_DB_USERNAME": "user",
				"QUEUER_DB_PASSWORD": "password",
				"QUEUER_DB_SCHEMA":   "public",
			},
			expectError: true,
		},
		{
			name:           "Invalid options",
			maxConcurrency: 100,
			options: []*model.OnError{
				{
					Timeout:      -10.0, // Invalid timeout value
					MaxRetries:   3,
					RetryDelay:   1.0,
					RetryBackoff: model.RETRY_BACKOFF_LINEAR,
				},
			},
			dbEnvs: map[string]string{
				"QUEUER_DB_HOST":     "localhost",
				"QUEUER_DB_PORT":     dbPort,
				"QUEUER_DB_DATABASE": "database",
				"QUEUER_DB_USERNAME": "user",
				"QUEUER_DB_PASSWORD": "password",
				"QUEUER_DB_SCHEMA":   "public",
			},
			expectError: true,
		},
		{
			name:           "Missing DB environment variable",
			maxConcurrency: 100,
			options:        nil,
			dbEnvs: map[string]string{
				"QUEUER_DB_HOST": "localhost",
				"QUEUER_DB_PORT": dbPort,
				// "QUEUER_DB_DATABASE": "database", // Intentionally missing
				"QUEUER_DB_USERNAME": "user",
				"QUEUER_DB_PASSWORD": "password",
				"QUEUER_DB_SCHEMA":   "public",
			},
			expectError: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for key, value := range test.dbEnvs {
				t.Setenv(key, value)
			}

			if test.expectError {
				defer func() {
					if r := recover(); r == nil {
						t.Errorf("Expected panic for %s, but did not get one", test.name)
					}
				}()
			}

			queuer := NewQueuer(test.name, test.maxConcurrency, test.options...)
			if !test.expectError {
				require.NotNil(t, queuer, "Expected Queuer to be created successfully")
				assert.Equal(t, test.name, queuer.worker.Name, "Expected Queuer name to match")
				assert.Equal(t, test.maxConcurrency, queuer.worker.MaxConcurrency, "Expected Queuer max concurrency to match")
			}
		})
	}
}

func TestNewStaticQueuer(t *testing.T) {
	tests := []struct {
		name        string
		logLevel    slog.Leveler
		dbConfig    *helper.DatabaseConfiguration
		dbEnvs      map[string]string
		expectError bool
	}{
		{
			name:     "Valid static queuer with nil dbConfig",
			logLevel: slog.LevelInfo,
			dbConfig: nil,
			dbEnvs: map[string]string{
				"QUEUER_DB_HOST":     "localhost",
				"QUEUER_DB_PORT":     dbPort,
				"QUEUER_DB_DATABASE": "database",
				"QUEUER_DB_USERNAME": "user",
				"QUEUER_DB_PASSWORD": "password",
				"QUEUER_DB_SCHEMA":   "public",
			},
			expectError: false,
		},
		{
			name:     "Valid static queuer with provided dbConfig",
			logLevel: slog.LevelInfo,
			dbConfig: &helper.DatabaseConfiguration{
				Host:          "localhost",
				Port:          dbPort,
				Database:      "database",
				Username:      "user",
				Password:      "password",
				Schema:        "public",
				WithTableDrop: true,
			},
			dbEnvs:      map[string]string{}, // No env vars needed when dbConfig is provided
			expectError: false,
		},
		{
			name:     "Missing DB environment variable when dbConfig is nil",
			logLevel: slog.LevelInfo,
			dbConfig: nil,
			dbEnvs: map[string]string{
				"QUEUER_DB_HOST": "localhost",
				"QUEUER_DB_PORT": dbPort,
				// "QUEUER_DB_DATABASE": "database", // Intentionally missing
				"QUEUER_DB_USERNAME": "user",
				"QUEUER_DB_PASSWORD": "password",
				"QUEUER_DB_SCHEMA":   "public",
			},
			expectError: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for key, value := range test.dbEnvs {
				t.Setenv(key, value)
			}

			if test.expectError {
				defer func() {
					if r := recover(); r == nil {
						t.Errorf("Expected panic for %s, but did not get one", test.name)
					}
				}()
			}

			queuer := NewStaticQueuer(test.logLevel, test.dbConfig)
			if !test.expectError {
				require.NotNil(t, queuer, "Expected StaticQueuer to be created successfully")
				assert.NotNil(t, queuer.log, "Expected logger to be initialized")
				assert.NotNil(t, queuer.DB, "Expected database connection to be initialized")
				assert.NotNil(t, queuer.dbJob, "Expected job database handler to be initialized")
				assert.NotNil(t, queuer.dbWorker, "Expected worker database handler to be initialized")
				assert.NotNil(t, queuer.dbMaster, "Expected master database handler to be initialized")
				assert.NotNil(t, queuer.tasks, "Expected tasks map to be initialized")
				assert.NotNil(t, queuer.nextIntervalFuncs, "Expected nextIntervalFuncs map to be initialized")
				assert.Nil(t, queuer.worker, "Expected worker to be nil in StaticQueuer")
			}
		})
	}
}

func TestStart(t *testing.T) {
	envs := map[string]string{
		"QUEUER_DB_HOST":     "localhost",
		"QUEUER_DB_PORT":     dbPort,
		"QUEUER_DB_DATABASE": "database",
		"QUEUER_DB_USERNAME": "user",
		"QUEUER_DB_PASSWORD": "password",
		"QUEUER_DB_SCHEMA":   "public",
	}
	for key, value := range envs {
		t.Setenv(key, value)
	}

	t.Run("Start Queuer with valid context", func(t *testing.T) {
		queuer := NewQueuer("test", 10)
		require.NotNil(t, queuer, "Expected Queuer to be created successfully")

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		startQueuer := func() {
			queuer.Start(ctx, cancel)
		}
		assert.NotPanics(t, startQueuer, "Expected no panic when starting Queuer with valid context")
		queuer.Stop()
	})

	t.Run("Start Queuer with nil context", func(t *testing.T) {
		queuer := NewQueuer("test", 10)
		require.NotNil(t, queuer, "Expected Queuer to be created successfully")

		startQueuer := func() {
			queuer.Start(context.TODO(), nil)
		}
		assert.Panics(t, startQueuer, "Expected panic when starting Queuer with nil context")
	})

	t.Run("Start Queuer with MasterSettings", func(t *testing.T) {
		queuer := NewQueuer("test", 10)
		require.NotNil(t, queuer, "Expected Queuer to be created successfully")

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		masterSettings := &model.MasterSettings{
			MasterPollInterval: 5 * time.Second,
			RetentionArchive:   30,
		}
		queuer.Start(ctx, cancel, masterSettings)

		time.Sleep(6 * time.Second) // Wait for the ticker to run at least once

		master, err := queuer.dbMaster.SelectMaster()
		assert.NoError(t, err, "Expected no error selecting master")
		require.NotNil(t, master, "Expected master to not be nil")
		assert.Equal(t, queuer.worker.RID, master.WorkerRID, "Expected master RID to match worker RID")
		assert.Equal(t, queuer.worker.ID, master.WorkerID, "Expected master ID to match worker ID")
		assert.Equal(t, queuer.worker.Status, model.WorkerStatusRunning, "Expected worker status to be RUNNING")
		assert.Equal(t, masterSettings.RetentionArchive, master.Settings.RetentionArchive, "Expected master retention archive to match")

		// Check if the master updated_at is within the last 2 seconds,
		// as the ticker runs every 5 seconds and we waited 6 seconds.
		assert.GreaterOrEqual(t, master.UpdatedAt.Unix(), time.Now().Add(-2*time.Second).Unix(), "Expected master updated_at to be less than or equal to current time")
	})

	t.Run("Start 2 Queuers with MasterSettings and cancel first", func(t *testing.T) {
		queuer1 := NewQueuer("test", 10)
		require.NotNil(t, queuer1, "Expected Queuer 1 to be created successfully")
		ctx1, cancel1 := context.WithCancel(context.Background())
		defer cancel1()
		masterSettings1 := &model.MasterSettings{
			MasterPollInterval: 5 * time.Second,
			RetentionArchive:   30 * 24 * time.Hour,
		}
		queuer1.Start(ctx1, cancel1, masterSettings1)

		queuer2 := NewQueuer("test", 20)
		require.NotNil(t, queuer2, "Expected Queuer 2 to be created successfully")
		ctx2, cancel2 := context.WithCancel(context.Background())
		defer cancel2()
		masterSettings2 := &model.MasterSettings{
			MasterPollInterval: 3 * time.Second,
			RetentionArchive:   20 * 24 * time.Hour,
		}
		queuer2.Start(ctx2, cancel2, masterSettings2)

		time.Sleep(6 * time.Second) // Wait for the ticker to run at least once

		master, err := queuer1.dbMaster.SelectMaster()
		assert.NoError(t, err, "Expected no error selecting master")
		require.NotNil(t, master, "Expected master to not be nil")
		assert.Equal(t, queuer1.worker.RID, master.WorkerRID, "Expected master RID to match worker RID")
		assert.Equal(t, queuer1.worker.ID, master.WorkerID, "Expected master ID to match worker ID")
		assert.Equal(t, *masterSettings1, master.Settings, "Expected master settings to match")

		// Check if the master updated_at is within the last 5 seconds,
		// as the ticker runs every 5 seconds and we waited 6 seconds.
		assert.GreaterOrEqual(t, master.UpdatedAt.Unix(), time.Now().Add(-5*time.Second).Unix(), "Expected master updated_at to be in the last 5 seconds")

		// Cancel the first queuer
		cancel1()

		time.Sleep(6 * time.Second) // Wait for the ticker to run at least once

		master, err = queuer2.dbMaster.SelectMaster()
		assert.NoError(t, err, "Expected no error selecting master")
		require.NotNil(t, master, "Expected master to not be nil")
		assert.Equal(t, queuer2.worker.RID, master.WorkerRID, "Expected master RID to match worker 2 RID")
		assert.Equal(t, queuer2.worker.ID, master.WorkerID, "Expected master ID to match worker 2 ID")
		assert.Equal(t, queuer2.worker.Status, model.WorkerStatusRunning, "Expected worker status to be RUNNING")
		assert.Equal(t, *masterSettings2, master.Settings, "Expected master settings to match")

		// Check if the master updated_at is within the last 5 seconds,
		// as the ticker runs every 5 seconds and we waited 6 seconds.
		assert.GreaterOrEqual(t, master.UpdatedAt.Unix(), time.Now().Add(-5*time.Second).Unix(), "Expected master updated_at to be in the last 5 seconds")

		err = queuer2.Stop()
		assert.NoError(t, err, "Expected Stop to complete without error")
	})
}

func TestStartWithoutWorker(t *testing.T) {
	envs := map[string]string{
		"QUEUER_DB_HOST":     "localhost",
		"QUEUER_DB_PORT":     dbPort,
		"QUEUER_DB_DATABASE": "database",
		"QUEUER_DB_USERNAME": "user",
		"QUEUER_DB_PASSWORD": "password",
		"QUEUER_DB_SCHEMA":   "public",
	}
	for key, value := range envs {
		t.Setenv(key, value)
	}

	t.Run("Start Queuer without worker", func(t *testing.T) {
		queuer := NewQueuer("test", 10)
		require.NotNil(t, queuer, "Expected Queuer to be created successfully")

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		startQueuer := func() {
			queuer.StartWithoutWorker(ctx, cancel, true)
		}
		assert.NotPanics(t, startQueuer, "Expected no panic when starting Queuer without worker")

		err := queuer.Stop()
		assert.NoError(t, err, "Expected Stop to complete without error")
	})

	t.Run("Start 2 Queuers with MasterSettings and cancel first", func(t *testing.T) {
		queuer1 := NewQueuer("test", 10)
		require.NotNil(t, queuer1, "Expected Queuer 1 to be created successfully")
		ctx1, cancel1 := context.WithCancel(context.Background())
		defer cancel1()
		masterSettings1 := &model.MasterSettings{
			MasterPollInterval: 5 * time.Second,
			RetentionArchive:   30 * 24 * time.Hour,
		}
		queuer1.StartWithoutWorker(ctx1, cancel1, true, masterSettings1)

		queuer2 := NewQueuer("test", 20)
		require.NotNil(t, queuer2, "Expected Queuer 2 to be created successfully")
		ctx2, cancel2 := context.WithCancel(context.Background())
		defer cancel2()
		masterSettings2 := &model.MasterSettings{
			MasterPollInterval: 3 * time.Second,
			RetentionArchive:   20 * 24 * time.Hour,
		}
		queuer2.StartWithoutWorker(ctx2, cancel2, true, masterSettings2)

		time.Sleep(6 * time.Second) // Wait for the ticker to run at least once

		master, err := queuer1.dbMaster.SelectMaster()
		assert.NoError(t, err, "Expected no error selecting master")
		require.NotNil(t, master, "Expected master to not be nil")
		assert.Equal(t, queuer1.worker.RID, master.WorkerRID, "Expected master RID to match worker RID")
		assert.Equal(t, queuer1.worker.ID, master.WorkerID, "Expected master ID to match worker ID")
		assert.Equal(t, *masterSettings1, master.Settings, "Expected master settings to match")

		// Check if the master updated_at is within the last 5 seconds,
		// as the ticker runs every 5 seconds and we waited 6 seconds.
		assert.GreaterOrEqual(t, master.UpdatedAt.Unix(), time.Now().Add(-5*time.Second).Unix(), "Expected master updated_at to be in the last 5 seconds")

		// Cancel the first queuer
		cancel1()

		time.Sleep(6 * time.Second) // Wait for the ticker to run at least once

		master, err = queuer2.dbMaster.SelectMaster()
		assert.NoError(t, err, "Expected no error selecting master")
		require.NotNil(t, master, "Expected master to not be nil")
		assert.Equal(t, queuer2.worker.RID, master.WorkerRID, "Expected master RID to match worker 2 RID")
		assert.Equal(t, queuer2.worker.ID, master.WorkerID, "Expected master ID to match worker 2 ID")
		assert.Equal(t, *masterSettings2, master.Settings, "Expected master settings to match")

		// Check if the master updated_at is within the last 5 seconds,
		// as the ticker runs every 5 seconds and we waited 6 seconds.
		assert.GreaterOrEqual(t, master.UpdatedAt.Unix(), time.Now().Add(-5*time.Second).Unix(), "Expected master updated_at to be in the last 5 seconds")

		err = queuer2.Stop()
		assert.NoError(t, err, "Expected Stop to complete without error")
	})
}

func TestStop(t *testing.T) {
	envs := map[string]string{
		"QUEUER_DB_HOST":     "localhost",
		"QUEUER_DB_PORT":     dbPort,
		"QUEUER_DB_DATABASE": "database",
		"QUEUER_DB_USERNAME": "user",
		"QUEUER_DB_PASSWORD": "password",
		"QUEUER_DB_SCHEMA":   "public",
	}
	for key, value := range envs {
		t.Setenv(key, value)
	}

	queuer := NewQueuer("test", 10)
	require.NotNil(t, queuer, "Expected Queuer to be created successfully")

	err := queuer.Stop()
	assert.NoError(t, err, "Expected Stop to complete without error")
}

func TestMasterTicker(t *testing.T) {
	envs := map[string]string{
		"QUEUER_DB_HOST":     "localhost",
		"QUEUER_DB_PORT":     dbPort,
		"QUEUER_DB_DATABASE": "database",
		"QUEUER_DB_USERNAME": "user",
		"QUEUER_DB_PASSWORD": "password",
		"QUEUER_DB_SCHEMA":   "public",
	}
	for key, value := range envs {
		t.Setenv(key, value)
	}

	queuer := NewQueuer("test", 10)
	require.NotNil(t, queuer, "Expected Queuer to be created successfully")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Run("Master ticker fails with nil old master", func(t *testing.T) {
		var oldMaster *model.Master
		masterSettings := &model.MasterSettings{
			MasterPollInterval: 5 * time.Second,
		}

		err := queuer.masterTicker(ctx, oldMaster, masterSettings)
		assert.Error(t, err, "Expected error starting master ticker with nil old master")
	})
}
