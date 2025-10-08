package get

import (
	"github.com/siherrmann/queuer/cli/model"
	"github.com/spf13/cobra"
)

type GetCommand struct {
	Cmd       *cobra.Command
	RootFlags *model.RootFlags
	GetFlags  *GetFlags
}

type GetFlags struct {
	RID string
}

func AddGetCommand(cmd *cobra.Command, rootFlags *model.RootFlags) {
	getFlags := &GetFlags{}
	getCmd := &GetCommand{
		Cmd: &cobra.Command{
			Use:   "get",
			Short: "Get a specific resource by its RID (Resource ID)",
			Long: `Retrieve detailed information about a specific queuer resource using its RID.

Use subcommands to specify the type of resource to retrieve:
- job         - Get details of a specific job by RID
- worker      - Get worker information and status by RID  
- jobArchive  - Get archived job details by RID

The RID (Resource ID) is a UUID that uniquely identifies each resource in the system.

Examples:
  queuer get worker --rid "123e4567-e89b-12d3-a456-426614174000"
  queuer get job --rid "550e8400-e29b-41d4-a716-446655440000"`,
		},
		RootFlags: rootFlags,
		GetFlags:  getFlags,
	}

	getCmd.Cmd.PersistentFlags().StringVarP(&getCmd.GetFlags.RID, "rid", "r", "", "Resource ID (RID) to retrieve")

	AddWorkerCommand(getCmd.Cmd, getCmd.GetFlags, getCmd.RootFlags)
	AddJobCommand(getCmd.Cmd, getCmd.GetFlags, getCmd.RootFlags)
	AddJobArchiveCommand(getCmd.Cmd, getCmd.GetFlags, getCmd.RootFlags)

	cmd.AddCommand(getCmd.Cmd)
}
