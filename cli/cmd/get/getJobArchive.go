package get

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/siherrmann/queuer/cli/helper"
	"github.com/siherrmann/queuer/cli/model"
	"github.com/spf13/cobra"
)

type JobArchiveCommand struct {
	Cmd       *cobra.Command
	RootFlags *model.RootFlags
	GetFlags  *GetFlags
}

func AddJobArchiveCommand(cmd *cobra.Command, getFlags *GetFlags, rootFlags *model.RootFlags) {
	jobArchiveCmd := &JobArchiveCommand{
		Cmd: &cobra.Command{
			Use:   "jobArchive",
			Short: "Get detailed information about a specific archived job by RID",
			Long: `Retrieve comprehensive details about a specific archived (completed or failed) job using its RID.

This command displays complete archived job information including:
- Archived Job ID and RID
- Task name and type
- Final job status (FAILED, SUCCEEDED, CANCELLED)
- Timestamp of the last update
- Error details (if applicable)

Archived jobs are historical records of completed job executions that have been
moved from the active jobs table for performance and storage optimization.

Example:
  queuer get jobArchive --rid "789f0123-e45b-67c8-d901-234567890abc"`,
		},
		RootFlags: rootFlags,
		GetFlags:  getFlags,
	}
	jobArchiveCmd.Cmd.Run = jobArchiveCmd.RunJobArchiveCommand

	cmd.AddCommand(jobArchiveCmd.Cmd)
}

func (r *JobArchiveCommand) RunJobArchiveCommand(cmd *cobra.Command, args []string) {
	parsedID, err := uuid.Parse(r.GetFlags.RID)
	if err != nil {
		fmt.Printf("Invalid UUID format for job RID: %s\n", r.GetFlags.RID)
		return
	}

	job, err := r.RootFlags.QueuerInstance.GetJobEnded(parsedID)
	if err != nil {
		fmt.Printf("Error retrieving job: %v\n", err)
		return
	}
	if job == nil {
		fmt.Printf("No job found with ID: %s\n", r.GetFlags.RID)
		return
	}

	helper.PrintTabbedLines([]helper.TabLine{
		{Key: "ID", Value: job.ID},
		{Key: "RID", Value: job.RID.String()},
		{Key: "Task name", Value: job.TaskName},
		{Key: "Status", Value: job.Status},
		{Key: "Last update", Value: job.UpdatedAt.Format("2006-01-02 15:04:05")},
		{Key: "Error", Value: job.Error},
	})
}
