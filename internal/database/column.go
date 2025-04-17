package database

import (
	"fmt"
	"strings"
)

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

// Column represents a table column
type Column struct {
	Name        string
	Type        ColumnType
	Constraints []ColumnConstraint
}

func (c *Column) String() string {
	return "Name: " + c.Name + "\nType: " + string(c.Type) + "\nConstraints: " + fmt.Sprint(c.Constraints) + "\n"
}

func parseColumnDef(columnDef string) (Column, error) {
	parts := strings.Fields(strings.TrimSpace(columnDef))
	if len(parts) < 2 {
		return Column{}, fmt.Errorf("invalid column definition")
	}

	colName := parts[0]
	colType := ColumnType(strings.ToUpper(parts[1]))

	if !isValidColumnType(colType) {
		return Column{}, fmt.Errorf("invalid column type")
	}

	constraints, err := parseConstraints(parts[2:])
	if err != nil {
		return Column{}, err
	}

	return Column{
		Name:        colName,
		Type:        colType,
		Constraints: constraints,
	}, nil
}

func parseConstraints(parts []string) ([]ColumnConstraint, error) {
	constraints := make([]ColumnConstraint, 0, len(parts))

	for i := 0; i < len(parts); i++ {
		constraint := strings.ToUpper(parts[i])

		switch constraint {
		case "NOT":
			if i+1 < len(parts) && parts[i+1] == "NULL" {
				constraints = append(constraints, COLUMN_CONSTRAINT_NOT_NULL)
				i++ // Skip next part ("NULL")
			}
		case "PRIMARY":
			if i+1 < len(parts) && parts[i+1] == "KEY" {
				constraints = append(constraints, COLUMN_CONSTRAINT_PRIMARY_KEY)
				i++ // Skip next part ("KEY")
			}
		case "FOREIGN":
			if i+1 < len(parts) && parts[i+1] == "KEY" {
				constraints = append(constraints, COLUMN_CONSTRAINT_FOREIGN_KEY)
				i++ // Skip next part ("KEY")
			}
		default:
			if !isValidColumnConstraint(ColumnConstraint(constraint)) {
				return nil, fmt.Errorf("invalid constraint: %s", constraint)
			}
			constraints = append(constraints, ColumnConstraint(constraint))
		}
	}

	return constraints, nil
}

func (c *Column) HasConstraint(constraint ColumnConstraint) bool {
	for _, con := range c.Constraints {
		if con == constraint {
			return true
		}
	}
	return false
}
