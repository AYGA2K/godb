package database

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"
)

type Database struct {
	Name   string
	Tables map[string]*Table
}

func NewDatabase(name string) *Database {
	return &Database{
		Name:   name,
		Tables: make(map[string]*Table),
	}
}

// Execute processes SQL commands
func (db *Database) Execute(sql string) (string, error) {
	// Basic SQL parsing
	createRegex := regexp.MustCompile(`(?i)^CREATE TABLE (\w+) \((.+)\)$`)
	insertRegex := regexp.MustCompile(`(?i)^INSERT INTO (\w+) \((.+)\) VALUES \((.+)\)$`)
	selectRegex := regexp.MustCompile(`(?i)^SELECT (.+) FROM (\w+)(?: WHERE (.+))?$`)
	deleteRegex := regexp.MustCompile(`(?i)^DELETE FROM (\w+)(?: WHERE (.+))?$`)

	switch {
	case strings.HasPrefix(strings.ToUpper(sql), "CREATE TABLE"):
		matches := createRegex.FindStringSubmatch(sql)
		if len(matches) < 3 {
			return "", fmt.Errorf("invalid CREATE TABLE syntax")
		}
		tableName := matches[1]
		columns := strings.Split(matches[2], ",")
		return db.CreateTable(tableName, columns)
	case strings.HasPrefix(strings.ToUpper(sql), "DELETE FROM"):
		matches := deleteRegex.FindStringSubmatch(sql)
		if len(matches) < 3 {
			return "", fmt.Errorf("invalid DELETE syntax")
		}
		tableName := matches[1]
		var whereClause string
		if len(matches) > 2 {
			whereClause = matches[2]
		}
		return db.Delete(tableName, whereClause)

	case strings.HasPrefix(strings.ToUpper(sql), "INSERT INTO"):
		matches := insertRegex.FindStringSubmatch(sql)
		if len(matches) < 4 {
			return "", fmt.Errorf("invalid INSERT syntax")
		}
		tableName := matches[1]
		columns := strings.Split(matches[2], ",")
		values := strings.Split(matches[3], ",")
		return db.Insert(tableName, columns, values)

	case strings.HasPrefix(strings.ToUpper(sql), "SELECT"):
		matches := selectRegex.FindStringSubmatch(sql)
		if len(matches) < 3 {
			return "", fmt.Errorf("invalid SELECT syntax")
		}
		columns := strings.Split(matches[1], ",")
		tableName := matches[2]
		var whereClause string
		if len(matches) > 3 {
			whereClause = matches[3]
		}
		return db.Select(tableName, columns, whereClause)

	default:
		return "", fmt.Errorf("unsupported SQL command")
	}
}

func (db *Database) CreateTable(name string, columnDefs []string) (string, error) {
	if _, exists := db.Tables[name]; exists {
		return "", fmt.Errorf("table %s already exists", name)
	}

	table := &Table{Name: name}
	for _, def := range columnDefs {
		parts := strings.Split(strings.TrimSpace(def), " ")
		if len(parts) < 2 {
			return "", fmt.Errorf("invalid column definition: %s", def)
		}

		name := strings.TrimSpace(parts[0])
		typeStr := ColumnType(strings.ToUpper(strings.TrimSpace(parts[1])))

		if !isValidColumnType(typeStr) {
			fmt.Println("Invalid column type:", typeStr)
			return "", fmt.Errorf("invalid column type: %s", typeStr)
		}

		var table struct {
			Columns []Column
		}

		table.Columns = append(table.Columns, Column{
			Name: name,
			Type: typeStr,
		})
	}

	db.Tables[name] = table
	return fmt.Sprintf("Table %s created", name), nil
}

// Insert adds a new row to a table
func (db *Database) Insert(tableName string, columns []string, values []string) (string, error) {
	table, exists := db.Tables[tableName]
	if !exists {
		return "", fmt.Errorf("table %s does not exist", tableName)
	}

	if len(columns) != len(values) {
		return "", fmt.Errorf("column count does not match value count")
	}

	row := make(Row)
	for i, col := range columns {
		col = strings.TrimSpace(col)
		val := strings.TrimSpace(values[i])

		// Find column type
		var colType ColumnType
		for _, column := range table.Columns {
			if column.Name == col {
				colType = column.Type
				break
			}
		}

		// Simple type conversion
		switch colType {
		case COLUMN_TYPE_INT:
			var num int64
			_, err := fmt.Sscanf(val, "%d", &num)
			if err != nil {
				return "", fmt.Errorf("invalid integer value for column %s", col)
			}
			row[col] = num
		case COLUMN_TYPE_STRING, COLUMN_TYPE_TEXT:
			row[col] = strings.Trim(val, "'\"")
		case COLUMN_TYPE_FLOAT:
			var num float64
			_, err := fmt.Sscanf(val, "%f", &num)
			if err != nil {
				return "", fmt.Errorf("invalid float value for column %s", col)
			}
			row[col] = num
		case COLUMN_TYPE_BOOL:
			var num bool
			_, err := fmt.Sscanf(val, "%t", &num)
			if err != nil {
				return "", fmt.Errorf("invalid boolean value for column %s", col)
			}
			row[col] = num
		case COLUMN_TYPE_DATE:
			var num time.Time
			_, err := fmt.Sscanf(val, "%s", &num)
			if err != nil {
				return "", fmt.Errorf("invalid date value for column %s", col)
			}
			row[col] = num
		case COLUMN_TYPE_PRIMARY_KEY:
			row[col] = val // TODO: validate primary key
		default:
			row[col] = val
			fmt.Println("Invalid column type:", colType)
			return "", fmt.Errorf("invalid column type: %s", colType)
		}
	}

	table.Rows = append(table.Rows, row)
	return "1 row inserted", nil
}

// Delete removes a row from a table
func (db *Database) Delete(tableName string, whereClause string) (string, error) {
	table, exists := db.Tables[tableName]
	if !exists {
		return "", fmt.Errorf("table %s does not exist", tableName)
	} else if len(table.Rows) == 0 {
		return "", fmt.Errorf("table %s is empty", tableName)
	}
	var results []Row
	for _, row := range table.Rows {
		if whereClause == "" || db.evaluateWhere(row, whereClause) {
			results = append(results, row)
		}
	}
	table.Rows = results
	return fmt.Sprintf("%d rows deleted", len(results)), nil
}

// Select retrieves data from a table
func (db *Database) Select(tableName string, columns []string, whereClause string) (string, error) {
	table, exists := db.Tables[tableName]
	if !exists {
		return "", fmt.Errorf("table %s does not exist", tableName)
	}

	var results []Row
	for _, row := range table.Rows {
		if whereClause == "" || db.evaluateWhere(row, whereClause) {
			resultRow := make(Row)
			for _, col := range columns {
				col = strings.TrimSpace(col)
				if col == "*" {
					for k, v := range row {
						resultRow[k] = v
					}
				} else if val, exists := row[col]; exists {
					resultRow[col] = val
				} else {
					return "", fmt.Errorf("column %s not found", col)
				}
			}
			results = append(results, resultRow)
		}
	}

	jsonData, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		return "", err
	}
	return string(jsonData), nil
}

// evaluateWhere handles simple WHERE clause evaluation
func (db *Database) evaluateWhere(row Row, whereClause string) bool {
	// Very simple equality evaluation for demonstration
	parts := strings.Split(whereClause, "=")
	if len(parts) != 2 {
		return false
	}

	col := strings.TrimSpace(parts[0])
	val := strings.TrimSpace(parts[1])
	val = strings.Trim(val, "'\"")

	rowVal, exists := row[col]
	if !exists {
		return false
	}

	return fmt.Sprint(rowVal) == val
}
