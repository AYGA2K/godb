package database

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

type Table struct {
	Name        string
	Columns     []Column
	Rows        []Row
	PrimaryKey  string
	ForeignKeys map[string]string
}

func newTable(name string) *Table {
	return &Table{
		Name:    name,
		Columns: []Column{},
		Rows:    []Row{},
	}
}

func (t Table) GetColumns() []Column {
	return t.Columns
}

func (t Table) GetColumn(name string) (Column, error) {
	for _, column := range t.Columns {
		if column.Name == name {
			return column, nil
		}
	}
	return Column{}, fmt.Errorf("column %s does not exist", name)
}

func (t Table) GetRows() []Row {
	return t.Rows
}

func (t *Table) addColumn(column Column) {
	t.Columns = append(t.Columns, column)
}

func (t *Table) addRow(row Row) error {
	if err := t.validatePrimaryKey(row); err != nil {
		return err
	}
	if err := t.validateUnique(row); err != nil {
		return err
	}
	if err := t.applyAutoIncrement(&row); err != nil {
		return err
	}
	t.Rows = append(t.Rows, row)
	return nil
}

func (t Table) columnExists(columnName string) bool {
	for _, column := range t.Columns {
		if column.Name == columnName {
			return true
		}
	}
	return false
}

func (t *Table) validatePrimaryKey(row Row) error {
	if t.PrimaryKey == "" {
		return nil
	}

	pkValue, exists := row[t.PrimaryKey]
	if !exists {
		return fmt.Errorf("primary key column %s not provided", t.PrimaryKey)
	}

	for _, existingRow := range t.Rows {
		if existingRow[t.PrimaryKey] == pkValue {
			return fmt.Errorf("primary key value %v already exists", pkValue)
		}
	}
	return nil
}

func (t *Table) validateUnique(row Row) error {
	for _, column := range t.Columns {
		if column.HasConstraint(COLUMN_CONSTRAINT_UNIQUE) {
			val := row[column.Name]
			for _, existingRow := range t.Rows {
				if existingRow[column.Name] == val {
					return fmt.Errorf("unique constraint violation on column %s", column.Name)
				}
			}
		}
	}
	return nil
}

func (t *Table) applyAutoIncrement(row *Row) error {
	for _, col := range t.Columns {
		if col.HasConstraint(COLUMN_CONSTRAINT_AUTO_INCREMENT) {
			if _, exists := (*row)[col.Name]; !exists {
				max := 0
				for _, existingRow := range t.Rows {
					if val, ok := existingRow[col.Name].(int); ok && val > max {
						max = val
					}
				}
				(*row)[col.Name] = max + 1
			}
		}
	}
	return nil
}

func (t Table) String() string {
	name := "Table " + t.Name + "\n"
	columns := "Columns:\n"
	for _, col := range t.Columns {
		columns += fmt.Sprintf("%s\n", col.Name)
	}
	rows := "Rows:\n"
	for _, row := range t.Rows {
		rows += fmt.Sprintf("%v\n", row)
	}
	return name + columns + rows
}

func sortRows(rows []Row, col Column, dir string) []Row {
	sort.Slice(rows, func(i, j int) bool {
		vi, iok := rows[i][col.Name]
		vj, jok := rows[j][col.Name]
		if !iok || !jok {
			return false
		}

		switch col.Type {
		case COLUMN_TYPE_INT:
			viInt, ok1 := vi.(int)
			vjInt, ok2 := vj.(int)
			if !ok1 || !ok2 {
				return false
			}
			if dir == "ASC" {
				return viInt < vjInt
			} else {
				return viInt > vjInt
			}

		case COLUMN_TYPE_DOUBLE, COLUMN_TYPE_FLOAT:
			viFloat, ok1 := vi.(float64)
			vjFloat, ok2 := vj.(float64)
			if !ok1 || !ok2 {
				return false
			}
			if dir == "ASC" {
				return viFloat < vjFloat
			} else {
				return viFloat > vjFloat
			}

		case COLUMN_TYPE_VARCHAR:
			viStr, ok1 := vi.(string)
			vjStr, ok2 := vj.(string)
			if !ok1 || !ok2 {
				return false
			}
			if dir == "ASC" {
				return viStr < vjStr
			} else {
				return viStr > vjStr
			}

		case COLUMN_TYPE_BOOL:
			viBool, ok1 := vi.(bool)
			vjBool, ok2 := vj.(bool)
			if !ok1 || !ok2 {
				return false
			}
			// false is considered "less than" true
			if dir == "ASC" {
				return !viBool && vjBool
			} else {
				return viBool && !vjBool
			}
		case COLUMN_TYPE_DATE:
			viStr, ok1 := vi.(string)
			vjStr, ok2 := vj.(string)
			if !ok1 || !ok2 {
				return false
			}
			viTime, err1 := time.Parse("2006-01-02", viStr)
			vjTime, err2 := time.Parse("2006-01-02", vjStr)
			if err1 != nil || err2 != nil {
				return false // handle invalid dates
			}

			if dir == "ASC" {
				return viTime.Before(vjTime)
			} else {
				return viTime.After(vjTime)
			}

		case COLUMN_TYPE_ENUM:
			viStr, ok1 := vi.(string)
			vjStr, ok2 := vj.(string)
			if !ok1 || !ok2 {
				return false
			}
			if dir == "ASC" {
				return strings.ToLower(viStr) < strings.ToLower(vjStr)
			} else {
				return strings.ToLower(viStr) > strings.ToLower(vjStr)
			}

		default:
			return false
		}
	})
	return rows
}
