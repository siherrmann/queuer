package get

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/siherrmann/queuer/cli/helper"
	"github.com/siherrmann/queuer/cli/model"
	"github.com/spf13/cobra"
)

type WorkerCommand struct {
	Cmd       *cobra.Command
	RootFlags *model.RootFlags
	GetFlags  *GetFlags
}

func AddWorkerCommand(cmd *cobra.Command, getFlags *GetFlags, rootFlags *model.RootFlags) {
	workerCmd := &WorkerCommand{
		Cmd: &cobra.Command{
			Use:   "worker",
			Short: "Get detailed information about a specific worker by RID",
			Long: `Retrieve comprehensive details about a specific worker using its RID (Resource ID).

This command displays complete worker information including:
- Worker ID and RID
- Worker name
- Worker status (READY, RUNNING, FAILED, STOPPED)
- Timestamp of the last update

Example:
  queuer get worker --rid "123e4567-e89b-12d3-a456-426614174000"`,
		},
		RootFlags: rootFlags,
		GetFlags:  getFlags,
	}
	workerCmd.Cmd.Run = workerCmd.RunWorkerCommand

	cmd.AddCommand(workerCmd.Cmd)
}

func (r *WorkerCommand) RunWorkerCommand(cmd *cobra.Command, args []string) {
	parsedID, err := uuid.Parse(r.GetFlags.RID)
	if err != nil {
		fmt.Printf("Invalid UUID format for worker RID: %s\n", r.GetFlags.RID)
		return
	}

	worker, err := r.RootFlags.QueuerInstance.GetWorker(parsedID)
	if err != nil {
		fmt.Printf("Error retrieving worker: %v\n", err)
		return
	}
	if worker == nil {
		fmt.Printf("No worker found with ID: %s\n", r.GetFlags.RID)
		return
	}

	helper.PrintTabbedLines([]helper.TabLine{
		{Key: "ID", Value: worker.ID},
		{Key: "RID", Value: worker.RID.String()},
		{Key: "Name", Value: worker.Name},
		{Key: "Status", Value: worker.Status},
		{Key: "Last update", Value: worker.UpdatedAt.Format("2006-01-02 15:04:05")},
	})
}
