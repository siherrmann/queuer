package cmd

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/siherrmann/queuer/cli/helper"
	"github.com/spf13/cobra"
)

var lastIdWorker int
var limitWorker int
var idWorker string

func init() {
	workerCmd.Flags().IntVarP(&lastIdWorker, "lastId", "i", 0, "Last worker ID from previous call")
	workerCmd.Flags().IntVarP(&limitWorker, "limit", "l", 10, "Maximum number of workers to return")
	workerCmd.Flags().StringVarP(&idWorker, "rid", "r", "", "Worker RID (UUID format) to retrieve")

	rootCmd.AddCommand(workerCmd)
}

var workerCmd = &cobra.Command{
	Use:   "worker",
	Short: "List all workers or get a worker by RID",
	Long: `List all workers registered in the database.
	If lastId and limit are not provided, it lists the first 10 workers.
	If a RID is provided, it retrieves the worker with that specific RID.`,
	Run: RunWorkerCommand,
}

func RunWorkerCommand(cmd *cobra.Command, args []string) {
	if len(idWorker) > 0 {
		parsedID, err := uuid.Parse(idWorker)
		if err != nil {
			fmt.Printf("Invalid UUID format for worker RID: %s\n", idWorker)
			return
		}

		worker, err := queuerInstance.GetWorker(parsedID)
		if err != nil {
			fmt.Printf("Error retrieving worker: %v\n", err)
			return
		}
		if worker == nil {
			fmt.Printf("No worker found with ID: %s\n", idWorker)
			return
		}

		helper.PrintTabbedLines([]helper.TabLine{
			{Key: "ID", Value: worker.ID},
			{Key: "RID", Value: worker.RID.String()},
			{Key: "Name", Value: worker.Name},
			{Key: "Status", Value: worker.Status},
			{Key: "Last update", Value: worker.UpdatedAt.Format("2006-01-02 15:04:05")},
		})

		return
	} else {
		workers, err := queuerInstance.GetWorkers(lastIdWorker, limitWorker)
		if err != nil {
			fmt.Printf("Error retrieving workers: %v\n", err)
			return
		}

		header := []string{"ID", "RID", "Name", "Status", "Last update"}
		var rows [][]interface{}
		for _, worker := range workers {
			rows = append(rows, []interface{}{worker.ID, worker.RID.String(), worker.Name, worker.Status, worker.UpdatedAt.Format("2006-01-02 15:04:05")})
		}
		helper.PrintTable(header, rows)
	}
}
