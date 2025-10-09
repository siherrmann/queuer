package list

import (
	"fmt"

	"github.com/siherrmann/queuer/cli/helper"
	"github.com/siherrmann/queuer/cli/model"
	"github.com/spf13/cobra"
)

type JobArchiveCommand struct {
	Cmd       *cobra.Command
	RootFlags *model.RootFlags
	ListFlags *ListFlags
}

func AddJobArchiveCommand(cmd *cobra.Command, listFlags *ListFlags, rootFlags *model.RootFlags) {
	jobArchiveCmd := &JobArchiveCommand{
		Cmd: &cobra.Command{
			Use:   "jobArchive",
			Short: "List all archived jobs (completed/failed) with pagination support",
			Long: `Display a paginated list of all archived jobs that have completed execution.

This command shows historical job data including:
- Job ID and RID (Resource ID)
- Task name and type
- Final status (FAILED, SUCCEEDED, CANCELLED)
- Timestamp of the last update

Archived jobs are moved from the active jobs table for:
- Performance optimization of active job queries
- Long-term historical record keeping
- Audit trails and compliance requirements

Pagination options:
- limit: Number of archived jobs to display (default: 10, max: 100)
- lastId: Start listing from jobs after this RID for pagination

Examples:
  queuer list jobArchive                    # Show first 10 archived jobs
  queuer list jobArchive --limit 50        # Show first 50 archived jobs
  queuer list jobArchive --limit 20 --lastId "789f0123-e45b-67c8-d901-234567890abc"`,
		},
		RootFlags: rootFlags,
		ListFlags: listFlags,
	}
	jobArchiveCmd.Cmd.Run = jobArchiveCmd.RunJobArchiveCommand

	cmd.AddCommand(jobArchiveCmd.Cmd)
}

func (r *JobArchiveCommand) RunJobArchiveCommand(cmd *cobra.Command, args []string) {
	jobs, err := r.RootFlags.QueuerInstance.GetJobsEnded(r.ListFlags.LastId, r.ListFlags.Limit)
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
