package database

import (
	"fmt"
)

// Table represents a database table
type Table struct {
	name       string
	columns    []Column
	rows       []Row
	primaryKey string
}

func newTable(name string) *Table {
	return &Table{
		name:    name,
		columns: []Column{},
		rows:    []Row{},
	}
}

func (t Table) GetColumns() []Column {
	return t.columns
}

func (t Table) GetRows() []Row {
	return t.rows
}

func (t *Table) addColumn(column Column) {
	t.columns = append(t.columns, column)
}

func (t *Table) addRow(row Row) error {
	if err := t.validatePrimaryKey(row); err != nil {
		return err
	}
	if t.hasUnique() {
		if err := t.validateUnique(row); err != nil {
			return err
		}
	}
	if err := t.autoIncrement(); err != nil {
		return err
	}
	t.rows = append(t.rows, row)
	return nil
}

func (t *Table) hasUnique() bool {
	for _, column := range t.columns {
		for _, constraint := range column.Constraints {
			if constraint == COLUMN_CONSTRAINT_UNIQUE {
				return true
			}
		}
	}
	return false
}

// handle constraints :
// handle primary key
func (t *Table) validatePrimaryKey(row Row) error {
	for _, row := range t.rows {
		if row[t.primaryKey] == row[t.primaryKey] {
			return fmt.Errorf("primary key already exists")
		}
	}
	return fmt.Errorf("primary key can not be duplicated")
}

// validate unique
func (t *Table) validateUnique(row Row) error {
	var unique string
	for _, column := range t.columns {
		for _, constraint := range column.Constraints {
			if constraint == COLUMN_CONSTRAINT_UNIQUE {
				unique = column.Name
			}
		}
	}
	if unique == "" {
		return nil
	}
	uniqueValues := make(map[string]bool)
	for _, row := range t.rows {
		val, exists := row[unique]
		if !exists {
			continue
		}
		uniqueValues[fmt.Sprint(val)] = true
	}
	if _, exists := uniqueValues[fmt.Sprint(row[unique])]; exists {
		return fmt.Errorf("unique value already exists")
	}
	return nil
}

// handle auto increment
func (t *Table) autoIncrement() error {
	var autoIncrement string
	for _, column := range t.columns {
		for _, constraint := range column.Constraints {
			if constraint == COLUMN_CONSTRAINT_AUTO_INCREMENT {
				autoIncrement = column.Name
			}
		}
	}
	for _, row := range t.rows {
		val, exists := row[autoIncrement]
		if exists {
			row[autoIncrement] = val.(int) + 1
		}
	}
	return fmt.Errorf("auto increment field ")
}

func (t Table) String() string {
	name := "Table " + t.name + "\n"
	columns := "Columns:\n"
	for _, col := range t.columns {
		columns += fmt.Sprintf("%s\n", col.String())
	}
	rows := "Rows:\n"
	for _, row := range t.rows {
		rows += fmt.Sprintf("%s\n", row.String())
	}
	return name + columns + rows
}
