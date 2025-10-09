package loadSql

import (
	"database/sql"
	"testing"

	"github.com/siherrmann/queuer/helper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper function to create a test database connection
func setupTestDatabase(t *testing.T) *sql.DB {
	helper.SetTestDatabaseConfigEnvs(t, dbPort)
	dbConfig, err := helper.NewDatabaseConfiguration()
	require.NoError(t, err, "Expected to create database configuration")

	database := helper.NewTestDatabase(dbConfig)
	require.NotNil(t, database, "Expected to create test database")
	require.NotNil(t, database.Instance, "Expected database instance to be not nil")

	return database.Instance
}

func TestLoadJobSql(t *testing.T) {
	t.Run("Load job SQL functions successfully", func(t *testing.T) {
		db := setupTestDatabase(t)
		defer db.Close()

		// First load should succeed
		err := LoadJobSql(db, false)
		assert.NoError(t, err, "Expected LoadJobSql to succeed on first run")

		// Verify functions exist
		exist, err := checkFunctions(db, JobFunctions)
		assert.NoError(t, err, "Expected checkFunctions to complete without error")
		assert.True(t, exist, "Expected all job functions to exist after loading")
	})

	t.Run("Skip loading when functions already exist", func(t *testing.T) {
		db := setupTestDatabase(t)
		defer db.Close()

		// Load functions first time
		err := LoadJobSql(db, false)
		require.NoError(t, err, "Expected first LoadJobSql to succeed")

		// Second load should skip (not force)
		err = LoadJobSql(db, false)
		assert.NoError(t, err, "Expected LoadJobSql to succeed when functions already exist")
	})

	t.Run("Force reload existing functions", func(t *testing.T) {
		db := setupTestDatabase(t)
		defer db.Close()

		// Load functions first time
		err := LoadJobSql(db, false)
		require.NoError(t, err, "Expected first LoadJobSql to succeed")

		// Force reload should succeed
		err = LoadJobSql(db, true)
		assert.NoError(t, err, "Expected LoadJobSql with force=true to succeed")

		// Verify functions still exist
		exist, err := checkFunctions(db, JobFunctions)
		assert.NoError(t, err, "Expected checkFunctions to complete without error")
		assert.True(t, exist, "Expected all job functions to exist after force reload")
	})
}

func TestLoadWorkerSql(t *testing.T) {
	t.Run("Load worker SQL functions successfully", func(t *testing.T) {
		db := setupTestDatabase(t)
		defer db.Close()

		// First load should succeed
		err := LoadWorkerSql(db, false)
		assert.NoError(t, err, "Expected LoadWorkerSql to succeed on first run")

		// Verify functions exist
		exist, err := checkFunctions(db, WorkerFunctions)
		assert.NoError(t, err, "Expected checkFunctions to complete without error")
		assert.True(t, exist, "Expected all worker functions to exist after loading")
	})

	t.Run("Skip loading when functions already exist", func(t *testing.T) {
		db := setupTestDatabase(t)
		defer db.Close()

		// Load functions first time
		err := LoadWorkerSql(db, false)
		require.NoError(t, err, "Expected first LoadWorkerSql to succeed")

		// Second load should skip (not force)
		err = LoadWorkerSql(db, false)
		assert.NoError(t, err, "Expected LoadWorkerSql to succeed when functions already exist")
	})

	t.Run("Force reload existing functions", func(t *testing.T) {
		db := setupTestDatabase(t)
		defer db.Close()

		// Load functions first time
		err := LoadWorkerSql(db, false)
		require.NoError(t, err, "Expected first LoadWorkerSql to succeed")

		// Force reload should succeed
		err = LoadWorkerSql(db, true)
		assert.NoError(t, err, "Expected LoadWorkerSql with force=true to succeed")

		// Verify functions still exist
		exist, err := checkFunctions(db, WorkerFunctions)
		assert.NoError(t, err, "Expected checkFunctions to complete without error")
		assert.True(t, exist, "Expected all worker functions to exist after force reload")
	})
}

func TestLoadNotifySql(t *testing.T) {
	t.Run("Load notify SQL functions successfully", func(t *testing.T) {
		db := setupTestDatabase(t)
		defer db.Close()

		// First load should succeed
		err := LoadNotifySql(db, false)
		assert.NoError(t, err, "Expected LoadNotifySql to succeed on first run")

		// Verify functions exist
		exist, err := checkFunctions(db, NotifyFunctions)
		assert.NoError(t, err, "Expected checkFunctions to complete without error")
		assert.True(t, exist, "Expected all notify functions to exist after loading")
	})

	t.Run("Skip loading when functions already exist", func(t *testing.T) {
		db := setupTestDatabase(t)
		defer db.Close()

		// Load functions first time
		err := LoadNotifySql(db, false)
		require.NoError(t, err, "Expected first LoadNotifySql to succeed")

		// Second load should skip (not force)
		err = LoadNotifySql(db, false)
		assert.NoError(t, err, "Expected LoadNotifySql to succeed when functions already exist")
	})

	t.Run("Force reload existing functions", func(t *testing.T) {
		db := setupTestDatabase(t)
		defer db.Close()

		// Load functions first time
		err := LoadNotifySql(db, false)
		require.NoError(t, err, "Expected first LoadNotifySql to succeed")

		// Force reload should succeed
		err = LoadNotifySql(db, true)
		assert.NoError(t, err, "Expected LoadNotifySql with force=true to succeed")

		// Verify functions still exist
		exist, err := checkFunctions(db, NotifyFunctions)
		assert.NoError(t, err, "Expected checkFunctions to complete without error")
		assert.True(t, exist, "Expected all notify functions to exist after force reload")
	})
}

func TestCheckFunctions(t *testing.T) {
	t.Run("Check existing functions", func(t *testing.T) {
		db := setupTestDatabase(t)
		defer db.Close()

		// Load job functions first
		err := LoadJobSql(db, false)
		require.NoError(t, err, "Expected LoadJobSql to succeed")

		// Check if job functions exist
		exist, err := checkFunctions(db, JobFunctions)
		assert.NoError(t, err, "Expected checkFunctions to complete without error")
		assert.True(t, exist, "Expected checkFunctions to return true for existing functions")
	})

	t.Run("Check non-existing functions", func(t *testing.T) {
		db := setupTestDatabase(t)
		defer db.Close()

		// Check for functions that don't exist
		nonExistentFunctions := []string{"non_existent_function_1", "non_existent_function_2"}
		exist, err := checkFunctions(db, nonExistentFunctions)
		assert.NoError(t, err, "Expected checkFunctions to complete without error")
		assert.False(t, exist, "Expected checkFunctions to return false for non-existing functions")
	})

	t.Run("Check mixed existing and non-existing functions", func(t *testing.T) {
		db := setupTestDatabase(t)
		defer db.Close()

		// Load job functions first
		err := LoadJobSql(db, false)
		require.NoError(t, err, "Expected LoadJobSql to succeed")

		// Mix existing and non-existing functions
		mixedFunctions := []string{"update_job_initial", "non_existent_function"}
		exist, err := checkFunctions(db, mixedFunctions)
		assert.NoError(t, err, "Expected checkFunctions to complete without error")
		assert.False(t, exist, "Expected checkFunctions to return false when any function doesn't exist")
	})

	t.Run("Check empty function list", func(t *testing.T) {
		db := setupTestDatabase(t)
		defer db.Close()

		// Check empty list
		exist, err := checkFunctions(db, []string{})
		assert.NoError(t, err, "Expected checkFunctions to complete without error")
		assert.False(t, exist, "Expected checkFunctions to return false for empty function list (due to zero value)")
	})

	t.Run("Check all function types together", func(t *testing.T) {
		db := setupTestDatabase(t)
		defer db.Close()

		// Load all SQL types
		err := LoadJobSql(db, false)
		require.NoError(t, err, "Expected LoadJobSql to succeed")

		err = LoadWorkerSql(db, false)
		require.NoError(t, err, "Expected LoadWorkerSql to succeed")

		err = LoadNotifySql(db, false)
		require.NoError(t, err, "Expected LoadNotifySql to succeed")

		// Combine all function lists
		allFunctions := append(JobFunctions, WorkerFunctions...)
		allFunctions = append(allFunctions, NotifyFunctions...)

		exist, err := checkFunctions(db, allFunctions)
		assert.NoError(t, err, "Expected checkFunctions to complete without error")
		assert.True(t, exist, "Expected all functions to exist after loading all SQL types")
	})
}
