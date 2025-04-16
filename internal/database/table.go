package database

import (
	"fmt"
)

// Table represents a database table
type Table struct {
	Name       string
	Columns    []Column
	Rows       []Row
	PrimaryKey string
}

func (t Table) String() string {
	name := "Table " + t.Name + "\n"
	columns := "Columns:\n"
	for _, col := range t.Columns {
		columns += fmt.Sprintf("%s\n", col.String())
	}
	rows := "Rows:\n"
	for _, row := range t.Rows {
		rows += fmt.Sprintf("%s\n", row.String())
	}
	return name + columns + rows
}
