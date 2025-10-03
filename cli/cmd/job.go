package cmd

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/siherrmann/queuer/cli/helper"
	"github.com/spf13/cobra"
)

var lastIdJob int
var limitJob int
var idJob string

func init() {
	jobCmd.Flags().IntVarP(&lastIdJob, "lastId", "i", 0, "Last job ID from previous call")
	jobCmd.Flags().IntVarP(&limitJob, "limit", "l", 10, "Maximum number of jobs to return")
	jobCmd.Flags().StringVarP(&idJob, "rid", "r", "", "Job RID (UUID format) to retrieve")

	rootCmd.AddCommand(jobCmd)
}

var jobCmd = &cobra.Command{
	Use:   "job",
	Short: "List all jobs or get a job by RID",
	Long: `List all jobs registered in the database.
	If lastId and limit are not provided, it lists the first 10 jobs.
	If a RID is provided, it retrieves the job with that specific RID.`,
	Run: RunJobCommand,
}

func RunJobCommand(cmd *cobra.Command, args []string) {
	if len(idJob) > 0 {
		parsedID, err := uuid.Parse(idJob)
		if err != nil {
			fmt.Printf("Invalid UUID format for job RID: %s\n", idJob)
			return
		}

		job, err := queuerInstance.GetJob(parsedID)
		if err != nil {
			fmt.Printf("Error retrieving job: %v\n", err)
			return
		}
		if job == nil {
			fmt.Printf("No job found with ID: %s\n", idJob)
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

		return
	} else {
		jobs, err := queuerInstance.GetJobs(lastIdJob, limitJob)
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
}
