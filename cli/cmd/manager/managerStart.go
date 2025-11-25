package manager

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/siherrmann/queuer/cli/helper"
	"github.com/siherrmann/queuer/cli/model"
	"github.com/siherrmann/queuer/manager"
)

type StartCommand struct {
	Cmd          *cobra.Command
	RootFlags    *model.RootFlags
	ManagerFlags *ManagerFlags
	Background   bool
}

func AddStartCommand(cmd *cobra.Command, managerFlags *ManagerFlags, rootFlags *model.RootFlags) {
	managerCmd := &StartCommand{
		Cmd: &cobra.Command{
			Use:   "start",
			Short: "Start a manager server to handle queuer operations",
			Long: `Display a paginated list of all managers registered in the queuer system.

This started server contains these routes:
- POST /api/v1/job
- GET /api/v1/jobs
- GET /api/v1/jobs/{rid}
- DELETE /api/v1/jobs/{rid}
- GET /api/v1/job-archives
- GET /api/v1/job-archives/{rid}
- GET /api/v1/workers
- GET /api/v1/workers/{rid}
- DELETE /api/v1/workers/{rid}
- GET /api/v1/connections`,
			Example: helper.FormatCmdExamples("manager", []helper.CmdExample{
				{Cmd: "", Description: "Run the queuer manager server"},
				{Cmd: "--help", Description: "Show help for the manager command"},
			}),
		},
		ManagerFlags: managerFlags,
		RootFlags:    rootFlags,
	}
	managerCmd.Cmd.Run = managerCmd.RunStartCommand

	managerCmd.Cmd.Flags().BoolVarP(&managerCmd.Background, "detach", "d", false, "Run the manager server in the background")

	cmd.AddCommand(managerCmd.Cmd)
}

func (r *StartCommand) RunStartCommand(cmd *cobra.Command, args []string) {
	if r.ManagerFlags.Port <= 1000 || r.ManagerFlags.Port > 65535 {
		fmt.Printf("Invalid port number: %d. Please specify a port between 1001 and 65535.\n", r.ManagerFlags.Port)
		return
	}

	if r.Background {
		err := r.startDetached()
		if err != nil {
			fmt.Printf("Failed to start manager server in background: %v\n", err)
			return
		}
		fmt.Printf("✅ Manager server started in background on port %d\n", r.ManagerFlags.Port)
		fmt.Printf("💡 Use 'queuer manager health --port %d' to check status\n", r.ManagerFlags.Port)
	} else {
		fmt.Printf("Starting manager server on port %d...\n", r.ManagerFlags.Port)
		manager.ManagerServer(r.ManagerFlags.Port, 10)
	}
}

func (r *StartCommand) startDetached() error {
	executable, err := os.Executable()
	if err != nil {
		return fmt.Errorf("failed to get executable path: %w", err)
	}

	// Validate executable path to prevent command injection
	if !filepath.IsAbs(executable) {
		return fmt.Errorf("executable path is not absolute: %s", executable)
	}

	// Prepare arguments for the detached process (without -d flag to avoid recursion)
	args := []string{
		"manager", "start",
		"--port", fmt.Sprintf("%d", r.ManagerFlags.Port),
	}

	// Run the command in new process group
	// #nosec G204 - executable path is validated and args are controlled
	cmd := exec.Command(executable, args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true,
	}

	var devNull *os.File
	if !r.RootFlags.Verbose {
		devNull, err = os.OpenFile(os.DevNull, os.O_WRONLY, 0)
		if err != nil {
			return fmt.Errorf("failed to open /dev/null: %w", err)
		}

		cmd.Stdout = devNull
		cmd.Stderr = devNull
		cmd.Stdin = nil
	}

	err = cmd.Start()
	if err != nil {
		if devNull != nil {
			if closeErr := devNull.Close(); closeErr != nil {
				return fmt.Errorf("failed to start detached process: %w (also failed to close devNull: %v)", err, closeErr)
			}
		}
		return fmt.Errorf("failed to start detached process: %w", err)
	}

	// Close devNull if it was opened
	if devNull != nil {
		if closeErr := devNull.Close(); closeErr != nil {
			return fmt.Errorf("process started but failed to close devNull: %w", closeErr)
		}
	}

	return nil
}
