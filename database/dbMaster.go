package database

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/siherrmann/queuer/helper"
	"github.com/siherrmann/queuer/model"
)

// MasterDBHandlerFunctions defines the interface for Master database operations.
type MasterDBHandlerFunctions interface {
	CheckTableExistance() (bool, error)
	CreateTable() error
	DropTable() error
	UpdateMaster(worker *model.Worker, settings *model.MasterSettings) (*model.Master, error)
	SelectMaster() (*model.Master, error)
}

// MasterDBHandler implements MasterDBHandlerFunctions and holds the database connection.
type MasterDBHandler struct {
	db *helper.Database
}

// NewMasterDBHandler creates a new instance of MasterDBHandler.
// It initializes the database connection and creates the master table if it does not exist.
func NewMasterDBHandler(dbConnection *helper.Database, withTableDrop bool) (*MasterDBHandler, error) {
	if dbConnection == nil {
		return nil, fmt.Errorf("database connection is nil")
	}

	masterDbHandler := &MasterDBHandler{
		db: dbConnection,
	}

	if withTableDrop {
		err := masterDbHandler.DropTable()
		if err != nil {
			return nil, fmt.Errorf("error dropping master table: %w", err)
		}
	}

	err := masterDbHandler.CreateTable()
	if err != nil {
		return nil, fmt.Errorf("error creating mater table: %#v", err)
	}

	return masterDbHandler, nil
}

// CheckTableExistance checks if the 'master' table exists in the database.
// It returns true if the table exists, otherwise false.
func (r MasterDBHandler) CheckTableExistance() (bool, error) {
	masterTableExists, err := r.db.CheckTableExistance("master")
	if err != nil {
		return false, fmt.Errorf("error checking master table existence: %w", err)
	}
	return masterTableExists, err
}

// CreateTable creates the 'mater' and 'mater_archive' tables in the database.
// If the tables already exist, it does not create them again.
// It also creates a trigger for notifying events on the table and all necessary indexes.
func (r MasterDBHandler) CreateTable() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := r.db.Instance.ExecContext(
		ctx,
		`CREATE TABLE IF NOT EXISTS master (
			id INTEGER PRIMARY KEY DEFAULT 1,
			worker_id BIGINT DEFAULT 0,
			worker_rid UUID,
			settings JSONB DEFAULT '{}'::JSONB,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);`,
	)
	if err != nil {
		log.Panicf("error creating master table: %#v", err)
	}

	_, err = r.db.Instance.ExecContext(
		ctx,
		`INSERT INTO master DEFAULT VALUES
		ON CONFLICT (id) DO NOTHING;`,
	)
	if err != nil {
		log.Panicf("error inserting initial master entry: %#v", err)
	}

	r.db.Logger.Println("created table master")
	return nil
}

// DropTables drops the 'mater' and 'mater_archive' tables from the database.
func (r MasterDBHandler) DropTable() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	query := `DROP TABLE IF EXISTS master`
	_, err := r.db.Instance.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("error dropping master table: %#v", err)
	}

	r.db.Logger.Printf("Dropped table master")
	return nil
}

// UpdateMaster updates the master entry with the given worker's ID and settings.
// It locks the row for update to ensure that only one worker can update the master at a time.
// It returns the old master entry if it was successfully updated, or nil if no update was done.
func (r MasterDBHandler) UpdateMaster(worker *model.Worker, settings *model.MasterSettings) (*model.Master, error) {
	row := r.db.Instance.QueryRow(
		`WITH current_master AS (
			SELECT
				id,
				worker_id,
				worker_rid,
				settings,
				created_at,
				updated_at
			FROM master
			WHERE id = 1
			AND (
				updated_at < (CURRENT_TIMESTAMP - ($4 * INTERVAL '1 minute'))
				OR worker_id = $1
				OR worker_id = 0
			)
			FOR UPDATE SKIP LOCKED
		)
		UPDATE master
		SET
			worker_id = $1,
			worker_rid = $2,
			settings = $3::JSONB,
			updated_at = CURRENT_TIMESTAMP
		FROM current_master
		WHERE master.id = current_master.id
		RETURNING
			current_master.id,
			current_master.worker_id,
			current_master.worker_rid,
			current_master.settings,
			current_master.created_at,
			current_master.updated_at;`,
		worker.ID,
		worker.RID,
		settings,
		int(settings.MasterLockTimeout.Minutes()),
	)

	master := &model.Master{}
	err := row.Scan(
		&master.ID,
		&master.WorkerID,
		&master.WorkerRID,
		&master.Settings,
		&master.CreatedAt,
		&master.UpdatedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("error scanning master for worker id %v: %w", worker.ID, err)
	}

	return master, nil
}

// SelectMaster retrieves the current master entry from the database.
func (r MasterDBHandler) SelectMaster() (*model.Master, error) {
	row := r.db.Instance.QueryRow(
		`SELECT
			id,
			worker_id,
			worker_rid,
			settings,
			created_at,
			updated_at
        FROM master
        WHERE id = 1;`,
	)

	master := &model.Master{}
	err := row.Scan(
		&master.ID,
		&master.WorkerID,
		&master.WorkerRID,
		&master.Settings,
		&master.CreatedAt,
		&master.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("error scanning master: %w", err)
	}

	return master, nil
}
