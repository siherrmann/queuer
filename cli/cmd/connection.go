package cmd

import (
	"fmt"
	"strings"

	"github.com/siherrmann/queuer/cli/helper"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(connectionCmd)
}

var connectionCmd = &cobra.Command{
	Use:   "connection",
	Short: "List all connections",
	Long:  `List all connections registered in the database.`,
	Run:   RunConnectionCommand,
}

func RunConnectionCommand(cmd *cobra.Command, args []string) {
	connections, err := queuerInstance.GetConnections()
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
