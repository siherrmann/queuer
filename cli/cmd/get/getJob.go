package get

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/siherrmann/queuer/cli/helper"
	"github.com/siherrmann/queuer/cli/model"
	"github.com/spf13/cobra"
)

type JobCommand struct {
	Cmd       *cobra.Command
	RootFlags *model.RootFlags
	GetFlags  *GetFlags
}

func AddJobCommand(cmd *cobra.Command, getFlags *GetFlags, rootFlags *model.RootFlags) {
	jobCmd := &JobCommand{
		Cmd: &cobra.Command{
			Use:   "job",
			Short: "Get detailed information about a specific job by RID",
			Long: `Retrieve comprehensive details about a specific job using its RID (Resource ID).

This command displays complete job information including:
- Job ID and RID
- Task name and type
- Job status (QUEUED, SCHEDULED, RUNNING)
- Timestamp of the last update
- Error details (if applicable)`,
			Example: helper.FormatCmdExamples("get job", []helper.CmdExample{
				{Cmd: `--rid "550e8400-e29b-41d4-a716-446655440000"`, Description: "Get job details by RID"},
			}),
		},
		RootFlags: rootFlags,
		GetFlags:  getFlags,
	}
	jobCmd.Cmd.Run = jobCmd.RunJobCommand

	cmd.AddCommand(jobCmd.Cmd)
}

func (r *JobCommand) RunJobCommand(cmd *cobra.Command, args []string) {
	parsedID, err := uuid.Parse(r.GetFlags.RID)
	if err != nil {
		fmt.Printf("Invalid UUID format for job RID: %s\n", r.GetFlags.RID)
		return
	}

	job, err := r.RootFlags.QueuerInstance.GetJob(parsedID)
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
