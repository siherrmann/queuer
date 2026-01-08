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
	Graceful    bool
}

func AddWorkerCommand(cmd *cobra.Command, cancelFlags *CancelFlags, rootFlags *model.RootFlags) {
	workerCmd := &WorkerCommand{
		Cmd: &cobra.Command{
			Use:   "worker",
			Short: "Cancel/shut down a specific worker by RID",
			Long: `Cancel/shut down a worker using its RID (Resource ID).

This command will:
- Allow currently running jobs to complete with graceful shutdown
- Shut down the worker process
- Signal the worker to stop accepting new jobs`,
			Example: helper.FormatCmdExamples("cancel worker", []helper.CmdExample{
				{Cmd: `--rid "123e4567-e89b-12d3-a456-426614174000"`, Description: "Cancel worker by RID"},
				{Cmd: `--rid "123e4567-e89b-12d3-a456-426614174000" --graceful`, Description: "Gracefully cancel worker by RID allowing current jobs to complete"},
			}),
		},
		RootFlags:   rootFlags,
		CancelFlags: cancelFlags,
	}
	workerCmd.Cmd.Run = workerCmd.RunWorkerCommand

	workerCmd.Cmd.PersistentFlags().BoolVarP(&workerCmd.Graceful, "graceful", "g", false, "Allow currently running jobs to complete with graceful shutdown")

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

	if r.Graceful {
		err = r.RootFlags.QueuerInstance.StopWorkerGracefully(parsedID)
		if err != nil {
			fmt.Printf("Error retrieving worker: %v\n", err)
			return
		}
		fmt.Printf("Successfully initiated graceful shutdown for worker: %s\n", r.CancelFlags.RID)
		fmt.Println("The worker will finish current jobs before shutting down.")
		return
	}

	err = r.RootFlags.QueuerInstance.StopWorker(parsedID)
	if err != nil {
		fmt.Printf("Error retrieving worker: %v\n", err)
		return
	}
	fmt.Printf("Successfully initiated shutdown for worker: %s\n", r.CancelFlags.RID)
	fmt.Println("All assigned jobs will be cancelled.")
}
