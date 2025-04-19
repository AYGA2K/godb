package database

import (
	"fmt"
	"slices"
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
	Name            string
	Type            ColumnType
	Constraints     []ColumnConstraint
	ReferenceTable  string
	ReferenceColumn string
}

func (c *Column) String() string {
	return "Name: " + c.Name + "\nType: " + string(c.Type) + "\nConstraints: " + fmt.Sprint(c.Constraints) + "\n"
}

func (c *Column) parseColumnDef(columnDef string) error {
	parts := strings.Fields(strings.TrimSpace(columnDef))
	if len(parts) < 2 {
		return fmt.Errorf("invalid column definition")
	}

	colName := parts[0]
	colType := ColumnType(strings.ToUpper(parts[1]))
	if !isValidColumnType(ColumnType(colType)) {
		return fmt.Errorf("invalid column type")
	}

	if err := c.parseConstraints(parts[2:]); err != nil {
		return err
	}
	c.Name = colName
	c.Type = colType
	return nil
}

func (c *Column) parseConstraints(parts []string) error {
	for i := 0; i < len(parts); i++ {
		constraint := strings.ToUpper(parts[i])
		switch {
		case constraint == "NOT" && i+1 < len(parts) && parts[i+1] == "NULL":
			c.Constraints = append(c.Constraints, COLUMN_CONSTRAINT_NOT_NULL)
			i++ // Skip next part ("NULL")
		case constraint == "PRIMARY" && i+1 < len(parts) && parts[i+1] == "KEY":
			c.Constraints = append(c.Constraints, COLUMN_CONSTRAINT_PRIMARY_KEY)
			i++ // Skip next part ("KEY")
		case constraint == "FOREIGN" && i+3 < len(parts) &&
			strings.ToUpper(parts[i+1]) == "KEY" &&
			strings.ToUpper(parts[i+2]) == "REFERENCES":

			c.Constraints = append(c.Constraints, COLUMN_CONSTRAINT_FOREIGN_KEY)

			ref := parts[i+3]
			open := strings.Index(ref, "(")
			close := strings.Index(ref, ")")

			if open == -1 || close == -1 || close <= open+1 {
				return fmt.Errorf("invalid foreign key reference")
			}
			c.ReferenceTable = ref[:open]
			c.ReferenceColumn = ref[open+1 : close]
			i += 3
		default:
			if !isValidColumnConstraint(ColumnConstraint(constraint)) {
				return fmt.Errorf("invalid constraint: %s", constraint)
			}
			c.Constraints = append(c.Constraints, ColumnConstraint(constraint))
		}
	}

	return nil
}

func (c *Column) HasConstraint(constraint ColumnConstraint) bool {
	return slices.Contains(c.Constraints, constraint)
}
