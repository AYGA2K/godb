package database

import (
	"encoding/gob"
	"fmt"
	"strings"
)

// Column represents a table column
type Column struct {
	Name string
	Type ColumnType
}
type ColumnType string

const (
	COLUMN_TYPE_INT         ColumnType = "INT"
	COLUMN_TYPE_STRING      ColumnType = "STRING"
	COLUMN_TYPE_TEXT        ColumnType = "TEXT"
	COLUMN_TYPE_FLOAT       ColumnType = "FLOAT"
	COLUMN_TYPE_BOOL        ColumnType = "BOOL"
	COLUMN_TYPE_DATE        ColumnType = "DATE"
	COLUMN_TYPE_PRIMARY_KEY ColumnType = "PRIMARYKEY"
)

// Table represents a database table
type Table struct {
	Name    string
	Columns []Column
	Rows    []Row
}

// Row represents a table row (record)
type Row map[string]any

func init() {
	gob.Register(Row{})
	gob.Register(Column{})
	gob.Register(Table{})
	gob.Register(Database{})
	gob.Register(ColumnType(""))
}

func isValidColumnType(t ColumnType) bool {
	switch t {
	case COLUMN_TYPE_INT,
		COLUMN_TYPE_STRING,
		COLUMN_TYPE_TEXT,
		COLUMN_TYPE_FLOAT,
		COLUMN_TYPE_BOOL,
		COLUMN_TYPE_DATE,
		COLUMN_TYPE_PRIMARY_KEY:
		return true
	default:
		return false
	}
}

func (r Row) Update(columns []string, values []string) {
	if len(columns) != len(values) {
		return
	}
	for i, col := range columns {
		col = strings.TrimSpace(col)
		val := strings.TrimSpace(values[i])
		r[col] = val
	}
}

func (r Row) String() string {
	var result strings.Builder
	result.WriteString("{")
	first := true
	for col, val := range r {
		if !first {
			result.WriteString(", ")
		}
		first = false
		result.WriteString(col)
		result.WriteString(": ")
		result.WriteString(fmt.Sprint(val))
	}
	result.WriteString("}")
	return result.String()
}
