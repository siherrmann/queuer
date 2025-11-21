package manager

import (
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/siherrmann/queuer/cli/helper"
	"github.com/siherrmann/queuer/cli/model"
)

type StopCommand struct {
	Cmd          *cobra.Command
	RootFlags    *model.RootFlags
	ManagerFlags *ManagerFlags
	Force        bool
}

func AddStopCommand(cmd *cobra.Command, managerFlags *ManagerFlags, rootFlags *model.RootFlags) {
	stopCmd := &StopCommand{
		Cmd: &cobra.Command{
			Use:   "stop",
			Short: "Stop a running manager server",
			Long: `Stop a running manager server by finding the process using the specified port.

This command will:
- Find the process running on the specified port
- Attempt a graceful shutdown first
- Use force termination if graceful shutdown fails
- Clean up any remaining processes`,
			Example: helper.FormatCmdExamples("manager stop", []helper.CmdExample{
				{Cmd: "--port 3000", Description: "Stop manager server on port 3000"},
				{Cmd: "--port 3000 --force", Description: "Force stop manager server on port 3000"},
			}),
		},
		ManagerFlags: managerFlags,
		RootFlags:    rootFlags,
	}
	stopCmd.Cmd.Run = stopCmd.RunStopCommand

	stopCmd.Cmd.Flags().BoolVarP(&stopCmd.Force, "force", "f", false, "Force stop the server without graceful shutdown")

	cmd.AddCommand(stopCmd.Cmd)
}

func (r *StopCommand) RunStopCommand(cmd *cobra.Command, args []string) {
	if r.ManagerFlags.Port <= 1000 || r.ManagerFlags.Port > 65535 {
		fmt.Printf("Invalid port number: %d. Please specify a port between 1001 and 65535.\n", r.ManagerFlags.Port)
		return
	}

	fmt.Printf("Stopping manager server on port %d...\n", r.ManagerFlags.Port)

	pids, err := r.findProcessByPort(r.ManagerFlags.Port)
	if err != nil {
		fmt.Printf("❌ Error finding process: %v\n", err)
		return
	}
	if len(pids) == 0 {
		fmt.Printf("⚠️  No manager server found running on port %d\n", r.ManagerFlags.Port)
		return
	}

	for _, pid := range pids {
		if r.terminateProcess(pid) {
			fmt.Printf("✅ Manager server process (PID: %d) stopped\n", pid)
		} else {
			fmt.Printf("❌ Failed to stop process (PID: %d)\n", pid)
		}
	}
}

func (r *StopCommand) findProcessByPort(port int) ([]int, error) {
	cmd := exec.Command("lsof", "-ti", fmt.Sprintf(":%d", port))
	output, err := cmd.Output()
	if err != nil {
		return []int{}, nil
	}

	var pids []int
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	for _, line := range lines {
		if line == "" {
			continue
		}
		pid, err := strconv.Atoi(strings.TrimSpace(line))
		if err != nil {
			continue
		}
		pids = append(pids, pid)
	}

	return pids, nil
}

func (r *StopCommand) terminateProcess(pid int) bool {
	cmd := exec.Command("kill", "-TERM", strconv.Itoa(pid))
	err := cmd.Run()
	if err != nil {
		cmd = exec.Command("kill", "-KILL", strconv.Itoa(pid))
		err = cmd.Run()
		if err != nil {
			return false
		}
	}

	// Wait a moment and check if process is still running
	time.Sleep(1 * time.Second)

	cmd = exec.Command("kill", "-0", strconv.Itoa(pid))
	err = cmd.Run()

	return err != nil
}
