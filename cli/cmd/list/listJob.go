package list

import (
	"fmt"

	"github.com/siherrmann/queuer/cli/helper"
	"github.com/siherrmann/queuer/cli/model"
	"github.com/spf13/cobra"
)

type JobCommand struct {
	Cmd       *cobra.Command
	RootFlags *model.RootFlags
	ListFlags *ListFlags
}

func AddJobCommand(cmd *cobra.Command, listFlags *ListFlags, rootFlags *model.RootFlags) {
	jobCmd := &JobCommand{
		Cmd: &cobra.Command{
			Use:   "job",
			Short: "List all active jobs in the queue with pagination support",
			Long: `Display a paginated list of all active jobs currently in the queue system.

This command shows active jobs with their current status and key information:
- Job ID and RID (Resource ID)
- Task name and type
- Current status (QUEUED, SCHEDULED, RUNNING)
- Timestamp of the last update

Pagination options:
- limit: Number of jobs to display (default: 10, max: 100)
- lastId: Start listing from jobs after this RID for pagination

Examples:
  queuer list job                           # Show first 10 jobs
  queuer list job --limit 25               # Show first 25 jobs
  queuer list job --limit 20 --lastId "550e8400-e29b-41d4-a716-446655440000"`,
		},
		RootFlags: rootFlags,
		ListFlags: listFlags,
	}
	jobCmd.Cmd.Run = jobCmd.RunJobCommand

	cmd.AddCommand(jobCmd.Cmd)
}

func (r *JobCommand) RunJobCommand(cmd *cobra.Command, args []string) {
	jobs, err := r.RootFlags.QueuerInstance.GetJobs(r.ListFlags.LastId, r.ListFlags.Limit)
	if err != nil {
		fmt.Printf("Error retrieving jobs: %v\n", err)
		return
	}

	header := []string{"ID", "RID", "Task name", "Status", "Last update"}
	var rows [][]interface{}
	for _, job := range jobs {
		rows = append(rows, []interface{}{job.ID, job.RID.String(), job.TaskName, job.Status, job.UpdatedAt.Format("2006-01-02 15:04:05")})
	}
	helper.PrintTable(header, rows)
}
