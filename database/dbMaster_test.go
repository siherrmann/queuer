package database

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/siherrmann/queuer/helper"
	"github.com/siherrmann/queuer/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMasterNewMasterDBHandler(t *testing.T) {
	t.Run("Valid call NewMasterDBHandler", func(t *testing.T) {
		helper.SetTestDatabaseConfigEnvs(t, dbPort)
		dbConfig, err := helper.NewDatabaseConfiguration()
		if err != nil {
			t.Fatalf("failed to create database configuration: %v", err)
		}
		database := helper.NewTestDatabase(dbConfig)

		workerDbHandler, err := NewMasterDBHandler(database, true)
		assert.NoError(t, err)
		require.NotNil(t, workerDbHandler, "MasterDBHandler should not be nil")
		require.NotNil(t, workerDbHandler.db, "Database connection should not be nil")
		require.NotNil(t, workerDbHandler.db.Instance, "Database instance should not be nil")

		exists, err := workerDbHandler.CheckTableExistance()
		assert.NoError(t, err)
		assert.True(t, exists)

		err = workerDbHandler.DropTable()
		assert.NoError(t, err)
	})

	t.Run("Invalid call NewMasterDBHandler with nil database", func(t *testing.T) {
		_, err := NewMasterDBHandler(nil, true)
		assert.Error(t, err, "Expected error when creating MasterDBHandler with nil database")
		assert.Equal(t, "database connection is nil", err.Error(), "Expected specific error message for nil database connection")
	})
}

func TestMasterDropTable(t *testing.T) {
	helper.SetTestDatabaseConfigEnvs(t, dbPort)
	dbConfig, err := helper.NewDatabaseConfiguration()
	if err != nil {
		t.Fatalf("failed to create database configuration: %v", err)
	}
	database := helper.NewTestDatabase(dbConfig)

	workerDbHandler, err := NewMasterDBHandler(database, true)
	require.NoError(t, err, "Expected no error creating MasterDBHandler")

	err = workerDbHandler.DropTable()
	assert.NoError(t, err, "Expected no error dropping table")

	exists, err := workerDbHandler.CheckTableExistance()
	assert.NoError(t, err, "Expected no error checking table existence after drop")
	assert.False(t, exists, "Expected table to not exist after drop")
}

func TestMasterCreateTable(t *testing.T) {
	helper.SetTestDatabaseConfigEnvs(t, dbPort)
	dbConfig, err := helper.NewDatabaseConfiguration()
	if err != nil {
		t.Fatalf("failed to create database configuration: %v", err)
	}
	database := helper.NewTestDatabase(dbConfig)

	workerDbHandler, err := NewMasterDBHandler(database, true)
	require.NoError(t, err, "Expected no error creating MasterDBHandler")
	require.NotNil(t, workerDbHandler, "MasterDBHandler should not be nil")

	// Have to drop here because NewMasterDBHandler creates the table.
	err = workerDbHandler.DropTable()
	require.NoError(t, err, "Expected no error dropping table")

	err = workerDbHandler.CreateTable()
	assert.NoError(t, err, "Expected no error creating table")

	exists, err := workerDbHandler.CheckTableExistance()
	assert.NoError(t, err, "Expected no error checking table existence after create")
	assert.True(t, exists, "Expected table to exist after create")
}

func TestMasterUpdateMasterInitial(t *testing.T) {
	helper.SetTestDatabaseConfigEnvs(t, dbPort)
	dbConfig, err := helper.NewDatabaseConfiguration()
	if err != nil {
		t.Fatalf("failed to create database configuration: %v", err)
	}
	database := helper.NewTestDatabase(dbConfig)

	workerDbHandler, err := NewMasterDBHandler(database, true)
	require.NoError(t, err, "Expected no error creating MasterDBHandler")
	require.NotNil(t, workerDbHandler, "MasterDBHandler should not be nil")

	worker1 := &model.Worker{
		ID:  1,
		RID: uuid.New(),
	}
	settings := &model.MasterSettings{
		RetentionArchive:  30,
		MasterLockTimeout: 10 * time.Minute,
	}
	master, err := workerDbHandler.UpdateMasterInitial(worker1, settings)
	assert.NoError(t, err, "Expected no error updating master")
	require.NotNil(t, master, "Expected master to not be nil")
	assert.Equal(t, worker1.ID, master.WorkerID, "Expected master worker ID to match worker ID")
	assert.Equal(t, worker1.RID, master.WorkerRID, "Expected master worker RID to match worker RID")
	assert.Equal(t, settings.RetentionArchive, master.Settings.RetentionArchive, "Expected master retention archive to match")

	worker2 := &model.Worker{
		ID:  2,
		RID: uuid.New(),
	}
	master, err = workerDbHandler.UpdateMasterInitial(worker2, settings)
	assert.NoError(t, err, "Expected no error updating master again")
	assert.Nil(t, master, "Expected master to be nil")
}

func TestMasterSelectMaster(t *testing.T) {
	helper.SetTestDatabaseConfigEnvs(t, dbPort)
	dbConfig, err := helper.NewDatabaseConfiguration()
	if err != nil {
		t.Fatalf("failed to create database configuration: %v", err)
	}
	database := helper.NewTestDatabase(dbConfig)

	workerDbHandler, err := NewMasterDBHandler(database, true)
	require.NoError(t, err, "Expected no error creating MasterDBHandler")
	require.NotNil(t, workerDbHandler, "MasterDBHandler should not be nil")

	worker := &model.Worker{
		ID:  2,
		RID: uuid.New(),
	}
	settings := &model.MasterSettings{
		RetentionArchive:  30,
		MasterLockTimeout: 10 * time.Minute,
	}
	master, err := workerDbHandler.UpdateMasterInitial(worker, settings)
	require.NoError(t, err, "Expected no error updating master")
	require.NotNil(t, master, "Expected master to not be nil")

	master, err = workerDbHandler.SelectMaster()
	assert.NoError(t, err, "Expected no error selecting master")
	assert.NotNil(t, master, "Expected master to not be nil")
	assert.Equal(t, 1, master.ID, "Expected master ID to be 1")
	assert.Equal(t, worker.ID, master.WorkerID, "Expected master worker ID to be 1")
	assert.Equal(t, worker.RID, master.WorkerRID, "Expected master worker RID to not equal to worker RID")
	assert.Equal(t, settings.RetentionArchive, master.Settings.RetentionArchive, "Expected master retention archive to be 30")
}
