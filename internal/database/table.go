package database

import (
	"fmt"
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

func (t *Table) hasUnique() bool {
	for _, column := range t.Columns {
		if column.HasConstraint(COLUMN_CONSTRAINT_UNIQUE) {
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
