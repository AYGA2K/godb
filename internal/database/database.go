package database

import (
	"encoding/gob"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

func init() {
	gob.Register(Row{})
	gob.Register(Column{})
	gob.Register(Table{})
	gob.Register(Database{})
	gob.Register(time.Time{})
}

type Database struct {
	Name   string
	Tables map[string]*Table
	mu     sync.RWMutex
}

// NewDatabase creates or loads a database
func NewDatabase(name string) (*Database, error) {
	db := &Database{
		Name:   name,
		Tables: make(map[string]*Table),
	}
	// Try to load existing database
	if err := db.loadFromFileGob(); err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	return db, nil
}

func (db *Database) saveToFileGob() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	file, err := os.Create(db.Name + ".gob")
	if err != nil {
		return err
	}
	defer file.Close()
	return gob.NewEncoder(file).Encode(db)
}

func (db *Database) loadFromFileGob() error {
	file, err := os.Open(db.Name + ".gob")
	if err != nil {
		return err
	}
	defer file.Close()

	return gob.NewDecoder(file).Decode(db)
}

// Execute processes SQL commands
func (db *Database) Execute(sql string) (string, error) {
	// Normalize SQL
	sql = strings.TrimSpace(sql)
	if sql == "" {
		return "", fmt.Errorf("empty SQL statement")
	}

	// Basic SQL parsing
	createRegex := regexp.MustCompile(`(?i)^CREATE\s+TABLE\s+(\w+)\s*\((.+)\)\s*$`)
	insertRegex := regexp.MustCompile(`(?i)^INSERT\s+INTO\s+(\w+)\s*(?:\((.+?)\))?\s*VALUES\s*\((.+?)\)\s*$`)
	selectRegex := regexp.MustCompile(`(?i)^SELECT\s+(.+?)\s+FROM\s+(\w+)(?:\s+(JOIN\s+.+?\s+ON\s+.+?))?(?:\s+WHERE\s+(.+?))?(?:\s+ORDER BY\s+(.+?))?(?:\s+LIMIT\s+(\d+))?\s*$`)
	deleteRegex := regexp.MustCompile(`(?i)^DELETE\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+?))?\s*$`)
	updateRegex := regexp.MustCompile(`(?i)^UPDATE\s+(\w+)\s+SET\s+(.+?)\s+WHERE\s+(.+?)\s*$`)
	dropTableRegex := regexp.MustCompile(`(?i)^DROP\s+TABLE\s+(\w+)\s*$`)

	switch {
	case createRegex.MatchString(sql):
		matches := createRegex.FindStringSubmatch(sql)
		return db.CreateTable(matches[1], strings.Split(matches[2], ","))
	case dropTableRegex.MatchString(sql):
		matches := dropTableRegex.FindStringSubmatch(sql)
		return db.DropTable(matches[1])
	case deleteRegex.MatchString(sql):
		matches := deleteRegex.FindStringSubmatch(sql)
		return db.Delete(matches[1], matches[2])
	case insertRegex.MatchString(sql):
		matches := insertRegex.FindStringSubmatch(sql)
		var columns []string
		if matches[2] != "" {
			columns = strings.Split(matches[2], ",")
		}
		values := strings.Split(matches[3], ",")
		return db.Insert(matches[1], columns, values)
	case updateRegex.MatchString(sql):
		matches := updateRegex.FindStringSubmatch(sql)
		return db.Update(matches[1], matches[2], matches[3])
	case selectRegex.MatchString(sql):
		matches := selectRegex.FindStringSubmatch(sql)
		columns := strings.Split(matches[1], ",")
		// NOTE: FindStringSubmatch always returns a slice with len = 1 + number of capture groups.
		// If a capture group doesn't match, its value will be an empty string (""),
		// so accessing matches[3] or matches[4] is safe as long as the regex matched.
		tableName := matches[2]
		joinClause := matches[3]
		whereClause := matches[4]
		orderByClause := matches[5]
		limitClause := matches[6]
		return db.Select(tableName, columns, whereClause, joinClause, orderByClause, limitClause)
	default:
		return "", fmt.Errorf("unsupported SQL command")
	}
}

// CreateTable creates a new table
func (db *Database) CreateTable(name string, columnDefs []string) (string, error) {
	if _, exists := db.Tables[name]; exists {
		return "", fmt.Errorf("table %s already exists", name)
	}

	table := newTable(name)

	for _, def := range columnDefs {
		def = strings.TrimSpace(def)
		column := &Column{}
		if err := column.parseColumnDef(def); err != nil {
			return "", fmt.Errorf("error parsing column definition '%s': %v", def, err)
		}
		if column.ReferenceColumn != "" && column.ReferenceTable != "" {
			if !db.tableExists(column.ReferenceTable) {
				return "", fmt.Errorf("foreign key reference to unknown table '%s' in column '%s'", column.ReferenceTable, column.Name)
			}
		}
		table.addColumn(*column)
	}

	db.Tables[name] = table

	if err := db.saveToFileGob(); err != nil {
		return "", err
	}

	return fmt.Sprintf("Table %s created", name), nil
}

// DropTable removes a table
func (db *Database) DropTable(name string) (string, error) {
	delete(db.Tables, name)
	err := db.saveToFileGob()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("Table %s dropped", name), nil
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
			}
		}
		// Simple type conversion
		convertedVal, err := columnTypeConversion(colType, val)
		if err != nil {
			return "", err
		}
		row[col] = convertedVal
	}

	table.addRow(row)
	err := db.saveToFileGob()
	if err != nil {
		return "", err
	}
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
		if whereClause == "" || !db.evaluateWhere(row, whereClause) {
			results = append(results, row)
		}
	}
	table.Rows = results
	err := db.saveToFileGob()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%d rows deleted", len(results)), nil
}

// Select retrieves data from a table
func (db *Database) Select(tableName string, columns []string, whereClause string, joinClause string, orderByClause string, limitClause string) (string, error) {
	// Get the main table
	mainTable, err := db.getTable(tableName)
	if err != nil {
		return "", fmt.Errorf("table %s does not exist", tableName)
	}

	var results []Row

	if joinClause == "" {
		// Simple SELECT without JOIN
		for _, row := range mainTable.Rows {
			if whereClause == "" || db.evaluateWhere(row, whereClause) {
				resultRow := make(Row)
				for _, col := range columns {
					col = strings.TrimSpace(col)
					if col == "*" {
						maps.Copy(resultRow, row)
					} else if val, exists := row[col]; exists {
						resultRow[col] = val
					} else {
						return "", fmt.Errorf("column %s not found", col)
					}
				}

				if limitClause != "" {
					limit, err := parseLimitClause(limitClause)
					if err != nil {
						return "", err
					}
					if limit > 0 && len(results) >= limit {
						break
					}
				}
				results = append(results, resultRow)

			}
		}
	} else if joinClause != "" {
		// Handle JOIN
		joinTableName, joinCondition, err := parseJoinClause(joinClause)
		if err != nil {
			return "", fmt.Errorf("invalid join clause: %v", err)
		}

		joinTable, err := db.getTable(joinTableName)
		if err != nil {
			return "", fmt.Errorf("join table %s does not exist", joinTableName)
		}

		leftCol, rightCol, err := parseJoinCondition(joinCondition)
		if err != nil {
			return "", fmt.Errorf("invalid join condition: %v", err)
		}

		// Perform the actual join
	outer:
		for _, mainRow := range mainTable.Rows {
			for _, joinRow := range joinTable.Rows {
				if mainRow[leftCol] == joinRow[rightCol] {
					// Combine rows
					combinedRow := make(Row)
					maps.Copy(combinedRow, mainRow)
					maps.Copy(combinedRow, joinRow)

					// Apply WHERE clause if present
					if whereClause == "" || db.evaluateWhere(combinedRow, whereClause) {
						// Select only requested columns
						resultRow := make(Row)
						for _, col := range columns {
							col = strings.TrimSpace(col)
							if col == "*" {
								maps.Copy(resultRow, combinedRow)
							} else if val, exists := combinedRow[col]; exists {
								resultRow[col] = val
							} else {
								// Handle table.column
								if parts := strings.Split(col, "."); len(parts) == 2 {
									tablePrefix := parts[0]
									colName := parts[1]
									if tablePrefix == tableName {
										if val, exists := mainRow[colName]; exists {
											resultRow[col] = val
											continue
										}
									} else if tablePrefix == joinTableName {
										if val, exists := joinRow[colName]; exists {
											resultRow[col] = val
											continue
										}
									}
								}
								return "", fmt.Errorf("column %s not found", col)
							}
						}
						if limitClause != "" {
							limit, err := parseLimitClause(limitClause)
							if err != nil {
								return "", err
							}
							if limit > 0 && len(results) >= limit {
								break outer
							}
						}
						results = append(results, resultRow)
					}
				}
			}
		}
	}
	if len(results) == 0 {
		return "", fmt.Errorf("no results found")
	}
	if orderByClause != "" {
		orderByCol, orderByDir, err := parseOrderByClause(orderByClause)
		if err != nil {
			return "", err
		}
		table, err := db.getTable(tableName)
		if err != nil {
			return "", err
		}
		if !table.columnExists(orderByCol) {
			return "", fmt.Errorf("column %s does not exist", orderByCol)
		}
		col, err := table.GetColumn(orderByCol)
		if err != nil {
			return "", err
		}
		results = sortRows(results, col, orderByDir)
	}

	jsonData, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		return "", fmt.Errorf("failed to marshal results: %v", err)
	}
	return string(jsonData), nil
}

// evaluateWhere handles simple WHERE clause evaluation
func (db *Database) evaluateWhere(row Row, whereClause string) bool {
	if whereClause == "" {
		return true
	}

	// Check for multi-character operators (<=, >=, !=, =) first
	operators := []string{"<=", ">=", "!=", "=", "<", ">", "LIKE"}
	var op string
	var parts []string

	// Find which operator is being used
	for _, operator := range operators {
		if strings.Contains(whereClause, operator) {
			op = operator
			parts = strings.SplitN(whereClause, operator, 2)
			break
		}
	}

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

	// Convert both values to string for comparison
	rowStr := fmt.Sprint(rowVal)
	valStr := val

	switch op {
	case "=":
		return rowStr == valStr
	case "!=":
		return rowStr != valStr
	case "<":
		return compareValues(rowVal, valStr) < 0
	case ">":
		return compareValues(rowVal, valStr) > 0
	case "<=":
		return compareValues(rowVal, valStr) <= 0
	case ">=":
		return compareValues(rowVal, valStr) >= 0
	case "LIKE":
		return strings.Contains(rowStr, valStr)
	default:
		return false
	}
}

// Helper function to compare values with proper type handling
func compareValues(rowVal interface{}, valStr string) int {
	// Try to convert both to numbers first
	if rowNum, valNum, err := convertToNumbers(rowVal, valStr); err == nil {
		if rowNum == valNum {
			return 0
		} else if rowNum < valNum {
			return -1
		} else {
			return 1
		}
	}

	// Fall back to string comparison
	rowStr := fmt.Sprint(rowVal)
	if rowStr == valStr {
		return 0
	} else if rowStr < valStr {
		return -1
	} else {
		return 1
	}
}

// Helper function to convert values to numbers if possible
func convertToNumbers(rowVal interface{}, valStr string) (float64, float64, error) {
	var rowNum, valNum float64
	var err error

	// Convert row value
	switch v := rowVal.(type) {
	case int, int8, int16, int32, int64:
		rowNum = float64(reflect.ValueOf(v).Int())
	case uint, uint8, uint16, uint32, uint64:
		rowNum = float64(reflect.ValueOf(v).Uint())
	case float32:
		rowNum = float64(v)
	case float64:
		rowNum = v
	default:
		return 0, 0, fmt.Errorf("not a number")
	}

	// Convert comparison value
	valNum, err = strconv.ParseFloat(valStr, 64)
	if err != nil {
		return 0, 0, err
	}

	return rowNum, valNum, nil
}

func parseOrderByClause(orderByClause string) (string, string, error) {
	if orderByClause == "" {
		return "", "", fmt.Errorf("empty order by clause")
	}

	parts := strings.Fields(strings.TrimSpace(orderByClause))
	if len(parts) == 0 {
		return "", "", fmt.Errorf("invalid order by clause")
	}

	col := parts[0]
	direction := "ASC" // Default direction

	if len(parts) > 1 {
		upperDir := strings.ToUpper(parts[1])
		if upperDir == "ASC" || upperDir == "DESC" {
			direction = upperDir
		} else {
			return "", "", fmt.Errorf("invalid order by direction")
		}
	}

	return col, direction, nil
}

func parseLimitClause(limitClause string) (int, error) {
	if limitClause != "" {
		limit, err := strconv.Atoi(limitClause)
		if err != nil {
			return 0, fmt.Errorf("invalid limit clause: %v", err)
		}
		return limit, nil
	}
	return 0, nil
}

// Helper functions for join processing
func parseJoinClause(joinClause string) (string, string, error) {
	// Expected format: "JOIN table ON condition"
	parts := strings.SplitN(joinClause, "ON", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("invalid join syntax")
	}

	joinPart := strings.TrimSpace(parts[0])
	joinTable := strings.TrimSpace(strings.TrimPrefix(joinPart, "JOIN"))
	if joinTable == "" {
		return "", "", fmt.Errorf("missing join table name")
	}

	return joinTable, strings.TrimSpace(parts[1]), nil
}

func parseJoinCondition(condition string) (string, string, error) {
	// Expected format: "table1.column = table2.column"
	parts := strings.Split(condition, "=")
	if len(parts) != 2 {
		return "", "", fmt.Errorf("invalid join condition")
	}

	left := strings.TrimSpace(parts[0])
	right := strings.TrimSpace(parts[1])

	// Extract column names
	leftParts := strings.Split(left, ".")
	if len(leftParts) != 2 {
		return "", "", fmt.Errorf("invalid left side of join condition")
	}
	rightParts := strings.Split(right, ".")
	if len(rightParts) != 2 {
		return "", "", fmt.Errorf("invalid right side of join condition")
	}

	return leftParts[1], rightParts[1], nil
}

// Update updates rows in a table
func (db *Database) Update(tableName string, setClause string, whereClause string) (string, error) {
	table, exists := db.Tables[tableName]
	if !exists {
		return "", fmt.Errorf("table %s does not exist", tableName)
	}
	if len(table.Rows) == 0 {
		return "", fmt.Errorf("table %s is empty", tableName)
	}
	var rowCount int
	var updatedIndices []int
	for i, row := range table.Rows {
		if db.evaluateWhere(row, whereClause) {
			updatedIndices = append(updatedIndices, i)
			rowCount++
		}
	}
	if rowCount == 0 {
		return "", fmt.Errorf("no rows found")
	}
	setParts := strings.SplitSeq(setClause, ",")
	for setPart := range setParts {
		parts := strings.Split(setPart, "=")
		if len(parts) != 2 {
			return "", fmt.Errorf("invalid set clause: %s", setPart)
		}
		col := strings.TrimSpace(parts[0])
		val := strings.TrimSpace(parts[1])
		// find column type
		var colType ColumnType
		for _, column := range table.Columns {
			if column.Name == col {
				colType = column.Type
				break
			}
		}
		if !isValidColumnType(colType) {
			return "", fmt.Errorf("invalid column type: %s", colType)
		}

		// simple type conversion
		convertedVal, err := columnTypeConversion(colType, val)
		if err != nil {
			return "", err
		}
		for _, i := range updatedIndices {
			table.Rows[i][col] = convertedVal
		}
	}
	err := db.saveToFileGob()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%d rows updated", rowCount), nil
}

// columnTypeConversion converts a string value to the appropriate type
func columnTypeConversion(colType ColumnType, val string) (any, error) {
	switch colType {
	case COLUMN_TYPE_INT:
		var num int64
		_, err := fmt.Sscanf(val, "%d", &num)
		if err != nil {
			return nil, fmt.Errorf("invalid integer value for column type %s", colType)
		}
		return num, nil
	case COLUMN_TYPE_VARCHAR:
		return strings.Trim(val, "'\""), nil
	case COLUMN_TYPE_DOUBLE:
		var num float64
		_, err := fmt.Sscanf(val, "%f", &num)
		if err != nil {
			return nil, fmt.Errorf("invalid double value for column type %s", colType)
		}
		return num, nil
	case COLUMN_TYPE_FLOAT:
		var num float32
		_, err := fmt.Sscanf(val, "%f", &num)
		if err != nil {
			return nil, fmt.Errorf("invalid float value for column type %s", colType)
		}
		return num, nil
	case COLUMN_TYPE_BOOL:
		var boolean bool
		_, err := fmt.Sscanf(val, "%t", &boolean)
		if err != nil {
			return nil, fmt.Errorf("invalid boolean value for column type %s", colType)
		}
		return boolean, nil
	case COLUMN_TYPE_DATE:
		const layout = "2006-01-02"
		val = strings.Trim(val, "'\"")
		parsed_Date, err := time.Parse(layout, val)
		if err != nil {
			return nil, fmt.Errorf("invalid date value for column type %s", colType)
		}
		formatted_Date := parsed_Date.Format(layout)
		return formatted_Date, nil
	default:
		return val, nil
	}
}

func (db *Database) String() string {
	tables := "Tables:\n"
	for _, table := range db.Tables {
		tables += fmt.Sprintf("%s\n", table)
	}
	return tables
}

// AllTables returns all tables in the database
func (db *Database) AllTables() (map[string]*Table, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.Tables, nil
}

// tableExists checks if a table exists
func (db *Database) tableExists(name string) bool {
	db.mu.RLock()
	defer db.mu.RUnlock()
	_, exists := db.Tables[name]
	return exists
}

// getTable retrieves a table by name
func (db *Database) getTable(name string) (*Table, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	table, exists := db.Tables[name]
	if !exists {
		return nil, fmt.Errorf("table %s does not exist", name)
	}
	return table, nil
}
