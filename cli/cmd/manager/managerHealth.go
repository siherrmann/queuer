package manager

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/spf13/cobra"

	"github.com/siherrmann/queuer/cli/helper"
	"github.com/siherrmann/queuer/cli/model"
)

type HealthCommand struct {
	Cmd          *cobra.Command
	RootFlags    *model.RootFlags
	ManagerFlags *ManagerFlags
}

type HealthResponse struct {
	Status  string `json:"status"`
	Service string `json:"service"`
}

func AddHealthCommand(cmd *cobra.Command, managerFlags *ManagerFlags, rootFlags *model.RootFlags) {
	healthCmd := &HealthCommand{
		Cmd: &cobra.Command{
			Use:   "health",
			Short: "Check the health status of the manager server",
			Long: `Send a health check request to the manager server to verify it's running and responsive.

This command will connect to the manager server on the specified port and retrieve
the health status. It's useful for monitoring and ensuring the server is operational.`,
			Example: helper.FormatCmdExamples("manager health", []helper.CmdExample{
				{Cmd: "", Description: "Check health on default port"},
				{Cmd: "--port 3000", Description: "Check health on port 3000"},
			}),
		},
		ManagerFlags: managerFlags,
		RootFlags:    rootFlags,
	}
	healthCmd.Cmd.Run = healthCmd.RunHealthCommand

	cmd.AddCommand(healthCmd.Cmd)
}

func (r *HealthCommand) RunHealthCommand(cmd *cobra.Command, args []string) {
	if r.ManagerFlags.Port <= 1000 || r.ManagerFlags.Port > 65535 {
		fmt.Printf("Invalid port number: %d. Please specify a port between 1001 and 65535.\n", r.ManagerFlags.Port)
		return
	}

	fmt.Printf("Checking health of manager server on port %d...\n", r.ManagerFlags.Port)

	url := fmt.Sprintf("http://localhost:%d/health", r.ManagerFlags.Port)
	client := &http.Client{
		Timeout: 10 * time.Second,
	}
	resp, err := client.Get(url)
	if err != nil {
		helper.PrintError("Failed to connect to manager server: %v", err)
		helper.PrintInfo("Make sure the manager server is running on port %d", r.ManagerFlags.Port)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		helper.PrintError("Received non-OK response: %s", resp.Status)
		return
	}

	var healthResp HealthResponse
	err = json.NewDecoder(resp.Body).Decode(&healthResp)
	if err != nil {
		helper.PrintError("Failed to parse health response: %v", err)
		return
	}

	// Display health status
	helper.PrintSuccess("Manager server is healthy!")
	helper.PrintTabbedLines([]helper.TabLine{
		{Key: "Status", Value: healthResp.Status},
		{Key: "Service", Value: healthResp.Service},
		{Key: "URL", Value: url},
	})
}
