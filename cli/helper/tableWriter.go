package helper

import (
	"os"

	"github.com/jedib0t/go-pretty/v6/table"
)

func PrintTable(header []string, rows [][]interface{}) {
	t := table.NewWriter()
	t.SetStyle(table.StyleDefault)
	t.SetOutputMirror(os.Stdout)

	headerRow := table.Row{}
	for h := range header {
		headerRow = append(headerRow, header[h])
	}
	t.AppendHeader(headerRow)

	for _, row := range rows {
		t.AppendRow(row)
	}
	t.Render()
}
