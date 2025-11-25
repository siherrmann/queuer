package helper

import (
	"fmt"
)

type CmdExample struct {
	Cmd         string
	Description string
}

func FormatCmdExamples(cmd string, examples []CmdExample) (res string) {
	for i, example := range examples {
		if i > 0 {
			res += "\n\n"
		}
		res += fmt.Sprintf("- %s\n$ %s %s", example.Description, cmd, example.Cmd)
	}
	return
}
