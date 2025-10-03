package cmd

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/siherrmann/queuer"
	"github.com/spf13/cobra"
)

var verbose bool
var queuerInstance *queuer.Queuer

var rootCmd = &cobra.Command{
	Use:   "queuer",
	Short: "Queuer is a simple job queue system written in Go",
	Long: `Queuer is a simple job queue system written in Go.
	It provides a way to manage and process jobs asynchronously using a PostgreSQL database as the backend.`,
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		logLevel := slog.LevelError
		if verbose {
			logLevel = slog.LevelInfo
		}
		queuerInstance = queuer.NewStaticQueuer(logLevel, nil)
	},
}

func Execute() {
	rootCmd.PersistentFlags().BoolVarP(&verbose, "verbose", "v", false, "Enable verbose output")

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
