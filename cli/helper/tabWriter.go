package helper

import (
	"fmt"
	"os"
	"text/tabwriter"
)

type TabLine struct {
	Key   string
	Value interface{}
}

func PrintTabbedLines(lines []TabLine) {
	w := tabwriter.NewWriter(os.Stdout, 10, 0, 1, ' ', tabwriter.TabIndent)
	for _, t := range lines {
		fmt.Fprintf(w, "%s:\t%v\n", t.Key, t.Value)
	}
	err := w.Flush()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error flushing tab writer: %v\n", err)
	}
}
