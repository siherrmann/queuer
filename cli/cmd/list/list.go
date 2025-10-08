package list

import (
	"github.com/siherrmann/queuer/cli/model"
	"github.com/spf13/cobra"
)

type ListCommand struct {
	Cmd       *cobra.Command
	RootFlags *model.RootFlags
	ListFlags *ListFlags
}

type ListFlags struct {
	LastId int
	Limit  int
}

func AddListCommand(cmd *cobra.Command, rootFlags *model.RootFlags) {
	listFlags := &ListFlags{}
	listCmd := &ListCommand{
		Cmd: &cobra.Command{
			Use:   "list",
			Short: "List queuer resources with pagination support",
			Long: `List various queuer resources including jobs, workers, connections, and archived jobs.

Supports pagination through lastId and limit parameters. Use subcommands to specify 
the type of resource to list:
- job         - List active jobs (queued, scheduled, running)
- worker      - List registered workers and their status  
- connection  - List active database connections
- jobArchive  - List completed/archived jobs

Examples:
  queuer list worker --limit 5
  queuer list job --lastId 100 --limit 20`,
		},
		ListFlags: listFlags,
	}

	listCmd.RootFlags = rootFlags
	listCmd.Cmd.PersistentFlags().IntVarP(&listCmd.ListFlags.LastId, "lastId", "i", 0, "Last job ID from previous call")
	listCmd.Cmd.PersistentFlags().IntVarP(&listCmd.ListFlags.Limit, "limit", "l", 10, "Maximum number of jobs to return")

	AddWorkerCommand(listCmd.Cmd, listCmd.ListFlags, listCmd.RootFlags)
	AddJobCommand(listCmd.Cmd, listCmd.ListFlags, listCmd.RootFlags)
	AddJobArchiveCommand(listCmd.Cmd, listCmd.ListFlags, listCmd.RootFlags)
	AddConnectionCommand(listCmd.Cmd, listCmd.ListFlags, listCmd.RootFlags)

	cmd.AddCommand(listCmd.Cmd)
}
