package cancel

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/siherrmann/queuer/cli/helper"
	"github.com/siherrmann/queuer/cli/model"
	"github.com/spf13/cobra"
)

type JobCommand struct {
	Cmd         *cobra.Command
	RootFlags   *model.RootFlags
	CancelFlags *CancelFlags
}

func AddJobCommand(cmd *cobra.Command, cancelFlags *CancelFlags, rootFlags *model.RootFlags) {
	jobCmd := &JobCommand{
		Cmd: &cobra.Command{
			Use:   "job",
			Short: "Cancel a specific job by RID",
			Long: `Cancel a running or queued job using its RID (Resource ID).

This command will:
- Stop execution of a currently running job
- Remove a queued job from the execution queue
- Mark the job status as cancelled
- Free up the assigned worker (if any)
- Prevent any further retry attempts`,
			Example: helper.FormatCmdExamples("cancel job", []helper.CmdExample{
				{Cmd: `--rid "550e8400-e29b-41d4-a716-446655440000"`, Description: "Cancel job by RID"},
			}),
		},
		RootFlags:   rootFlags,
		CancelFlags: cancelFlags,
	}
	jobCmd.Cmd.Run = jobCmd.RunJobCommand

	cmd.AddCommand(jobCmd.Cmd)
}

func (r *JobCommand) RunJobCommand(cmd *cobra.Command, args []string) {
	if r.CancelFlags.RID == "" {
		fmt.Println("Error: RID is required. Use --rid flag to specify the job RID.")
		return
	}

	parsedID, err := uuid.Parse(r.CancelFlags.RID)
	if err != nil {
		fmt.Printf("Invalid UUID format for job RID: %s\n", r.CancelFlags.RID)
		return
	}

	cancelledJob, err := r.RootFlags.QueuerInstance.CancelJob(parsedID)
	if err != nil {
		fmt.Printf("Error cancelling job: %v\n", err)
		return
	}

	fmt.Printf("Successfully cancelled job: %s\n", r.CancelFlags.RID)

	// Display the cancelled job details
	helper.PrintTabbedLines([]helper.TabLine{
		{Key: "ID", Value: cancelledJob.ID},
		{Key: "RID", Value: cancelledJob.RID.String()},
		{Key: "Task name", Value: cancelledJob.TaskName},
		{Key: "Status", Value: cancelledJob.Status},
		{Key: "Last update", Value: cancelledJob.UpdatedAt.Format("2006-01-02 15:04:05")},
		{Key: "Error", Value: cancelledJob.Error},
	})
}
