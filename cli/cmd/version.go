package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(versionCmd)
}

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print the version number of the Queuer",
	Long:  `All software has versions. This is the Queuer's`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("Queuer v0.1.0 -- HEAD")
	},
}
