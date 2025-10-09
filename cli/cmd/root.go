package cmd

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/siherrmann/queuer"
	"github.com/siherrmann/queuer/cli/cmd/cancel"
	"github.com/siherrmann/queuer/cli/cmd/get"
	"github.com/siherrmann/queuer/cli/cmd/list"
	"github.com/siherrmann/queuer/cli/model"
	"github.com/spf13/cobra"
)

type RootCommand struct {
	Cmd  *cobra.Command
	Opts *model.RootFlags
}

var rootFlags = &model.RootFlags{}
var rootCmd = &RootCommand{
	Cmd: &cobra.Command{
		Use:   "queuer",
		Short: "A PostgreSQL-based job queue system written in Go",
		Long: `Queuer is a robust job queue system built on PostgreSQL with TimescaleDB support.
It provides efficient job scheduling, processing, and monitoring capabilities for distributed systems.

Features:
- Job batching with PostgreSQL COPY FROM
- Scheduled and periodic jobs with custom intervals
- Panic recovery and comprehensive error handling
- Multi-service deployment support
- Real-time job monitoring through custom listen/notify channels
- Encryption support for sensitive job data
- Master worker coordination for centralized settings`,
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			logLevel := slog.LevelError
			if rootFlags.Verbose {
				logLevel = slog.LevelInfo
			}
			rootFlags.QueuerInstance = queuer.NewStaticQueuer(logLevel, nil)
		},
	},
}

func Execute() {
	rootCmd.Opts = rootFlags
	rootCmd.Cmd.PersistentFlags().BoolVarP(&rootFlags.Verbose, "verbose", "v", false, "Enable verbose output")

	list.AddListCommand(rootCmd.Cmd, rootFlags)
	get.AddGetCommand(rootCmd.Cmd, rootFlags)
	cancel.AddCancelCommand(rootCmd.Cmd, rootFlags)

	if err := rootCmd.Cmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
