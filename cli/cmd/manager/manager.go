package manager

import (
	"github.com/spf13/cobra"

	"github.com/siherrmann/queuer/cli/helper"
	"github.com/siherrmann/queuer/cli/model"
)

type ManagerCommand struct {
	Cmd          *cobra.Command
	RootFlags    *model.RootFlags
	ManagerFlags *ManagerFlags
}

type ManagerFlags struct {
	Port int
}

func AddManagerCommand(cmd *cobra.Command, rootFlags *model.RootFlags) {
	managerCmd := &ManagerCommand{
		Cmd: &cobra.Command{
			Use:   "manager",
			Short: "Manager server to handle queuer operations",
			Long: `Start a manager server to handle queuer operations.

Use subcommands to specify the type of operation to perform:
- start  - Start the manager server
- stop   - Stop a running manager server  
- health - Check manager server health and status`,
			Example: helper.FormatCmdExamples("manager", []helper.CmdExample{
				{Cmd: `start --port 8080`, Description: "Start manager server on port 8080"},
				{Cmd: `health --port 8080`, Description: "Check manager server health"},
			}),
		},
		ManagerFlags: &ManagerFlags{},
	}

	managerCmd.RootFlags = rootFlags
	managerCmd.Cmd.PersistentFlags().IntVarP(&managerCmd.ManagerFlags.Port, "port", "p", 8080, "Port to run the manager server on")

	AddStartCommand(managerCmd.Cmd, managerCmd.ManagerFlags, managerCmd.RootFlags)
	AddStopCommand(managerCmd.Cmd, managerCmd.ManagerFlags, managerCmd.RootFlags)
	AddHealthCommand(managerCmd.Cmd, managerCmd.ManagerFlags, managerCmd.RootFlags)

	cmd.AddCommand(managerCmd.Cmd)
}
