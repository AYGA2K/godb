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
	COLUMN_TYPE_INT     ColumnType = "INT"
	COLUMN_TYPE_DOUBLE  ColumnType = "DOUBLE"
	COLUMN_TYPE_FLOAT   ColumnType = "FLOAT"
	COLUMN_TYPE_VARCHAR ColumnType = "VARCHAR"
	COLUMN_TYPE_BOOL    ColumnType = "BOOL"
	COLUMN_TYPE_DATE    ColumnType = "DATE"
	COLUMN_TYPE_ENUM    ColumnType = "ENUM"
)

type ColumnConstraint string

const (
	COLUMN_CONSTRAINT_NULL           ColumnConstraint = "NULL"
	COLUMN_CONSTRAINT_NOT_NULL       ColumnConstraint = "NOT NULL"
	COLUMN_CONSTRAINT_UNIQUE         ColumnConstraint = "UNIQUE"
	COLUMN_CONSTRAINT_PRIMARY_KEY    ColumnConstraint = "PRIMARY KEY"
	COLUMN_CONSTRAINT_FOREIGN_KEY    ColumnConstraint = "FOREIGN KEY"
	COLUMN_CONSTRAINT_AUTO_INCREMENT ColumnConstraint = "AUTO_INCREMENT"
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
		COLUMN_TYPE_VARCHAR,
		COLUMN_TYPE_DOUBLE,
		COLUMN_TYPE_FLOAT,
		COLUMN_TYPE_BOOL,
		COLUMN_TYPE_DATE,
		COLUMN_TYPE_ENUM:
		return true
	default:
		return false
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
