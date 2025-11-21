package cancel

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/siherrmann/queuer/cli/helper"
	"github.com/siherrmann/queuer/cli/model"
	"github.com/spf13/cobra"
)

type WorkerCommand struct {
	Cmd         *cobra.Command
	RootFlags   *model.RootFlags
	CancelFlags *CancelFlags
}

func AddWorkerCommand(cmd *cobra.Command, cancelFlags *CancelFlags, rootFlags *model.RootFlags) {
	workerCmd := &WorkerCommand{
		Cmd: &cobra.Command{
			Use:   "worker",
			Short: "Cancel/shut down a specific worker by RID",
			Long: `Cancel/shut down a worker using its RID (Resource ID).

This command will:
- Signal the worker to stop accepting new jobs
- Allow currently running jobs to complete
- Shut down the worker process
- Update the worker status to offline
- Cancel any jobs assigned to this worker that haven't started`,
			Example: helper.FormatCmdExamples("cancel worker", []helper.CmdExample{
				{Cmd: `--rid "123e4567-e89b-12d3-a456-426614174000"`, Description: "Cancel worker by RID"},
			}),
		},
		RootFlags:   rootFlags,
		CancelFlags: cancelFlags,
	}
	workerCmd.Cmd.Run = workerCmd.RunWorkerCommand

	cmd.AddCommand(workerCmd.Cmd)
}

func (r *WorkerCommand) RunWorkerCommand(cmd *cobra.Command, args []string) {
	if r.CancelFlags.RID == "" {
		fmt.Println("Error: RID is required. Use --rid flag to specify the worker RID.")
		return
	}

	parsedID, err := uuid.Parse(r.CancelFlags.RID)
	if err != nil {
		fmt.Printf("Invalid UUID format for worker RID: %s\n", r.CancelFlags.RID)
		return
	}

	// First, get the worker to verify it exists
	worker, err := r.RootFlags.QueuerInstance.GetWorker(parsedID)
	if err != nil {
		fmt.Printf("Error retrieving worker: %v\n", err)
		return
	}
	if worker == nil {
		fmt.Printf("No worker found with ID: %s\n", r.CancelFlags.RID)
		return
	}

	// Check if worker is already offline
	if worker.Status == "OFFLINE" {
		fmt.Printf("Worker %s is already offline\n", r.CancelFlags.RID)
		return
	}

	// Cancel all jobs assigned to this worker
	err = r.RootFlags.QueuerInstance.CancelAllJobsByWorker(parsedID, 100) // Cancel up to 100 jobs
	if err != nil {
		fmt.Printf("Error cancelling jobs for worker: %v\n", err)
		return
	}

	fmt.Printf("Successfully initiated shutdown for worker: %s\n", r.CancelFlags.RID)
	fmt.Println("All assigned jobs have been cancelled.")

	// Display the worker details
	helper.PrintTabbedLines([]helper.TabLine{
		{Key: "ID", Value: worker.ID},
		{Key: "RID", Value: worker.RID.String()},
		{Key: "Status", Value: worker.Status},
		{Key: "Last update", Value: worker.UpdatedAt.Format("2006-01-02 15:04:05")},
		{Key: "Created", Value: worker.CreatedAt.Format("2006-01-02 15:04:05")},
	})
}
