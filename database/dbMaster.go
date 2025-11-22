package database

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/siherrmann/queuer/helper"
	"github.com/siherrmann/queuer/model"
	loadSql "github.com/siherrmann/queuer/sql"
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
		return nil, helper.NewError("check", fmt.Errorf("database connection is nil"))
	}

	masterDbHandler := &MasterDBHandler{
		db: dbConnection,
	}

	err := loadSql.LoadMasterSql(masterDbHandler.db.Instance, false)
	if err != nil {
		return nil, helper.NewError("load master sql", err)
	}

	if withTableDrop {
		err = masterDbHandler.DropTable()
		if err != nil {
			return nil, helper.NewError("drop master table", err)
		}
	}

	err = masterDbHandler.CreateTable()
	if err != nil {
		return nil, helper.NewError("create master table", err)
	}

	return masterDbHandler, nil
}

// CheckTableExistance checks if the 'master' table exists in the database.
// It returns true if the table exists, otherwise false.
func (r MasterDBHandler) CheckTableExistance() (bool, error) {
	masterTableExists, err := r.db.CheckTableExistance("master")
	if err != nil {
		return false, helper.NewError("master table", err)
	}
	return masterTableExists, nil
}

// CreateTable creates the 'master' and 'master_archive' tables in the database.
// If the tables already exist, it does not create them again.
// It also creates a trigger for notifying events on the table and all necessary indexes.
func (r MasterDBHandler) CreateTable() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Use the SQL init_master() function to create the table and initial data
	_, err := r.db.Instance.ExecContext(ctx, `SELECT init_master();`)
	if err != nil {
		log.Panicf("error initializing master table: %#v", err)
	}

	r.db.Logger.Info("Checked/created table master")

	return nil
}

// DropTables drops the 'master' and 'master_archive' tables from the database.
func (r MasterDBHandler) DropTable() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	query := `DROP TABLE IF EXISTS master`
	_, err := r.db.Instance.ExecContext(ctx, query)
	if err != nil {
		return helper.NewError("drop", err)
	}

	r.db.Logger.Info("Dropped table master")

	return nil
}

// UpdateMaster updates the master entry with the given worker's ID and settings.
// It locks the row for update to ensure that only one worker can update the master at a time.
// It returns the old master entry if it was successfully updated, or nil if no update was done.
func (r MasterDBHandler) UpdateMaster(worker *model.Worker, settings *model.MasterSettings) (*model.Master, error) {
	row := r.db.Instance.QueryRow(
		`SELECT * FROM update_master($1, $2, $3, $4)`,
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
		return nil, helper.NewError("scan", err)
	}

	return master, nil
}

// SelectMaster retrieves the current master entry from the database.
func (r MasterDBHandler) SelectMaster() (*model.Master, error) {
	row := r.db.Instance.QueryRow(
		`SELECT * FROM select_master()`,
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
		return nil, helper.NewError("scan", err)
	}

	return master, nil
}
