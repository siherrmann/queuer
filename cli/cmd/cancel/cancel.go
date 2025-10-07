package cancel

import (
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
	cancelFlags := &CancelFlags{}
	cancelCmd := &CancelCommand{
		Cmd: &cobra.Command{
			Use:   "cancel",
			Short: "Cancel a specific resource by its RID (Resource ID)",
			Long: `Cancel operations on specific queuer resources using their RID.

Use subcommands to specify the type of resource to cancel:
- job    - Cancel a running or queued job by RID
- worker - Cancel/shutdown a worker by RID

The RID (Resource ID) is a UUID that uniquely identifies each resource in the system.
Canceling a job will stop its execution and mark it as cancelled.
Canceling a worker will gracefully shutdown the worker after completing current tasks.

Examples:
  queuer cancel job --rid "550e8400-e29b-41d4-a716-446655440000"
  queuer cancel worker --rid "123e4567-e89b-12d3-a456-426614174000"`,
		},
		RootFlags:   rootFlags,
		CancelFlags: cancelFlags,
	}

	cancelCmd.Cmd.Flags().StringVarP(&cancelCmd.CancelFlags.RID, "rid", "r", "", "Resource ID (RID) to cancel")

	AddJobCommand(cancelCmd.Cmd, cancelCmd.CancelFlags, cancelCmd.RootFlags)
	AddWorkerCommand(cancelCmd.Cmd, cancelCmd.CancelFlags, cancelCmd.RootFlags)

	cmd.AddCommand(cancelCmd.Cmd)
}
