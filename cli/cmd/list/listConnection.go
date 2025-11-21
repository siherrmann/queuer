package list

import (
	"fmt"
	"strings"

	"github.com/siherrmann/queuer/cli/helper"
	"github.com/siherrmann/queuer/cli/model"
	"github.com/spf13/cobra"
)

type ConnectionCommand struct {
	Cmd       *cobra.Command
	RootFlags *model.RootFlags
	ListFlags *ListFlags
}

func AddConnectionCommand(cmd *cobra.Command, listFlags *ListFlags, rootFlags *model.RootFlags) {
	connectionCmd := &ConnectionCommand{
		Cmd: &cobra.Command{
			Use:   "connection",
			Short: "List all active database connections to the queuer system",
			Long: `Display detailed information about all active database connections to the queuer system.

This command provides real-time visibility into database connectivity:
- Process ID (PID) and application name
- Database username and target database
- Current query being executed (if any)
- Connection state and status`,
			Example: helper.FormatCmdExamples("list connection", []helper.CmdExample{
				{Cmd: "", Description: "Show all active connections"},
			}),
		},
		ListFlags: listFlags,
		RootFlags: rootFlags,
	}
	connectionCmd.Cmd.Run = connectionCmd.RunConnectionCommand

	cmd.AddCommand(connectionCmd.Cmd)
}

func (r *ConnectionCommand) RunConnectionCommand(cmd *cobra.Command, args []string) {
	connections, err := r.RootFlags.QueuerInstance.GetConnections()
	if err != nil {
		fmt.Printf("Error retrieving connections: %v\n", err)
		return
	}

	header := []string{"PID", "Appname", "Username", "Database", "Query", "State"}
	var rows [][]interface{}
	for _, connection := range connections {
		connection.Query = strings.ReplaceAll(strings.ReplaceAll(connection.Query, "\n", " "), "\t", "")
		if len(connection.Query) > 50 {
			connection.Query = connection.Query[:50] + "..."
		}

		rows = append(rows, []interface{}{connection.PID, connection.ApplicationName, connection.Username, connection.Database, connection.Query, connection.State})
	}
	helper.PrintTable(header, rows)
}
