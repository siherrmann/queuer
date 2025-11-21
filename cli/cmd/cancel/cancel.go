package cancel

import (
	"github.com/siherrmann/queuer/cli/helper"
	"github.com/siherrmann/queuer/cli/model"
	"github.com/spf13/cobra"
)

type CancelCommand struct {
	Cmd         *cobra.Command
	RootFlags   *model.RootFlags
	CancelFlags *CancelFlags
}

type CancelFlags struct {
	RID string
}

func AddCancelCommand(cmd *cobra.Command, rootFlags *model.RootFlags) {
	cancelCmd := &CancelCommand{
		Cmd: &cobra.Command{
			Use:   "cancel",
			Short: "Cancel a specific resource by its RID (Resource ID)",
			Long: `Cancel operations on specific queuer resources using their RID.
The RID (Resource ID) is a UUID that uniquely identifies each resource in the system.
Canceling a job will stop its execution and mark it as cancelled.
Canceling a worker will shut down the worker after completing current tasks.

Use subcommands to specify the type of resource to cancel:
- job    - Cancel a running or queued job by RID
- worker - Cancel/shutdown a worker by RID`,
			Example: helper.FormatCmdExamples("cancel", []helper.CmdExample{
				{Cmd: `job --rid "550e8400-e29b-41d4-a716-446655440000"`, Description: "Cancel job by RID"},
				{Cmd: `worker --rid "123e4567-e89b-12d3-a456-426614174000"`, Description: "Cancel worker by RID"},
			}),
		},
		RootFlags:   rootFlags,
		CancelFlags: &CancelFlags{},
	}

	cancelCmd.Cmd.PersistentFlags().StringVarP(&cancelCmd.CancelFlags.RID, "rid", "r", "", "Resource ID (RID) to cancel")

	AddJobCommand(cancelCmd.Cmd, cancelCmd.CancelFlags, cancelCmd.RootFlags)
	AddWorkerCommand(cancelCmd.Cmd, cancelCmd.CancelFlags, cancelCmd.RootFlags)

	cmd.AddCommand(cancelCmd.Cmd)
}
