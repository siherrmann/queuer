package loadSql

import (
	"database/sql"
	_ "embed"
	"fmt"
	"log"
)

//go:embed job.sql
var jobSQL string

//go:embed worker.sql
var workerSQL string

//go:embed notify.sql
var notifySQL string

var JobFunctions = []string{"update_job_initial", "update_job_final", "update_job_final_encrypted"}
var WorkerFunctions = []string{"insert_worker", "update_worker", "delete_worker"}
var NotifyFunctions = []string{"notify_event"}

func LoadJobSql(db *sql.DB, force bool) error {
	if !force {
		exist, err := checkFunctions(db, JobFunctions)
		if err != nil {
			return fmt.Errorf("error checking existing job functions: %w", err)
		}
		if exist {
			return nil
		}
	}

	_, err := db.Exec(jobSQL)
	if err != nil {
		return fmt.Errorf("error executing job SQL: %w", err)
	}

	exist, err := checkFunctions(db, JobFunctions)
	if err != nil {
		return fmt.Errorf("error checking existing functions: %w", err)
	}
	if !exist {
		return fmt.Errorf("not all required SQL functions were created")
	}

	fmt.Println("SQL job functions loaded successfully")

	return nil
}

func LoadWorkerSql(db *sql.DB, force bool) error {
	if !force {
		exist, err := checkFunctions(db, WorkerFunctions)
		if err != nil {
			return fmt.Errorf("error checking existing worker functions: %w", err)
		}
		if exist {
			return nil
		}
	}

	_, err := db.Exec(workerSQL)
	if err != nil {
		return fmt.Errorf("error executing worker SQL: %w", err)
	}

	exist, err := checkFunctions(db, WorkerFunctions)
	if err != nil {
		return fmt.Errorf("error checking existing functions: %w", err)
	}
	if !exist {
		return fmt.Errorf("not all required SQL functions were created")
	}

	fmt.Println("SQL worker functions loaded successfully")

	return nil
}

func LoadNotifySql(db *sql.DB, force bool) error {
	if !force {
		exist, err := checkFunctions(db, NotifyFunctions)
		if err != nil {
			return fmt.Errorf("error checking existing notify functions: %w", err)
		}
		if exist {
			return nil
		}
	}

	_, err := db.Exec(notifySQL)
	if err != nil {
		return fmt.Errorf("error executing notify SQL: %w", err)
	}

	exist, err := checkFunctions(db, NotifyFunctions)
	if err != nil {
		return fmt.Errorf("error checking existing functions: %w", err)
	}
	if !exist {
		return fmt.Errorf("not all required SQL functions were created")
	}

	log.Println("SQL notify functions loaded successfully")

	return nil
}

func checkFunctions(db *sql.DB, sqlFunctions []string) (bool, error) {
	var allExist bool
	for _, f := range sqlFunctions {
		err := db.QueryRow(
			`SELECT EXISTS(SELECT 1 FROM pg_proc WHERE proname = $1);`,
			f,
		).Scan(&allExist)
		if err != nil {
			return false, fmt.Errorf("error checking existence of function %s: %w", f, err)
		}

		if !allExist {
			log.Printf("Function %s does not exist", f)
			break
		}
	}
	return allExist, nil
}
