package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

func init() {
	rootCmd.Cmd.AddCommand(versionCmd)
}

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Display the version information of the Queuer CLI",
	Long: `Display detailed version information for the Queuer CLI tool.

Use this command to verify your CLI installation and for troubleshooting
or compatibility checks with the Queuer job queue system.

Example:
  queuer version`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("Queuer v0.1.0 -- HEAD")
	},
}
