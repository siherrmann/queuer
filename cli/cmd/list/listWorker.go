package list

import (
	"fmt"

	"github.com/siherrmann/queuer/cli/helper"
	"github.com/siherrmann/queuer/cli/model"
	"github.com/spf13/cobra"
)

type WorkerCommand struct {
	Cmd       *cobra.Command
	RootFlags *model.RootFlags
	ListFlags *ListFlags
}

func AddWorkerCommand(cmd *cobra.Command, listFlags *ListFlags, rootFlags *model.RootFlags) {
	workerCmd := &WorkerCommand{
		Cmd: &cobra.Command{
			Use:   "worker",
			Short: "List all registered workers with their current status and pagination support",
			Long: `Display a paginated list of all workers registered in the queuer system.

This command shows worker information including:
- Worker ID and RID (Resource ID)
- Worker name and type
- Current status (READY, RUNNING, FAILED, STOPPED)
- Timestamp of the last update

Pagination options:
- limit: Number of workers to display (default: 10, max: 100)
- lastId: Start listing from workers after this RID for pagination

Examples:
  queuer list worker                        # Show first 10 workers
  queuer list worker --limit 50            # Show first 50 workers
  queuer list worker --limit 20 --lastId "123e4567-e89b-12d3-a456-426614174000"`,
		},
		RootFlags: rootFlags,
		ListFlags: listFlags,
	}
	workerCmd.Cmd.Run = workerCmd.RunWorkerCommand

	cmd.AddCommand(workerCmd.Cmd)
}

func (r *WorkerCommand) RunWorkerCommand(cmd *cobra.Command, args []string) {
	workers, err := r.RootFlags.QueuerInstance.GetWorkers(r.ListFlags.LastId, r.ListFlags.Limit)
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
