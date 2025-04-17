package database

import (
	"encoding/gob"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"regexp"
	"strings"
	"time"
)

type Database struct {
	Name   string
	Tables map[string]*Table
}

func NewDatabase(name string) (*Database, error) {
	db, err := loadFromFileGob(name)
	if err == nil {
		return db, nil
	}

	// Create new database if loading fails
	db = &Database{
		Name:   name,
		Tables: make(map[string]*Table),
	}

	if err := db.saveToFileGob(); err != nil {
		return nil, err
	}

	return db, nil
}

func (db *Database) saveToFileGob() error {
	file, err := os.Create(db.Name + ".gob")
	if err != nil {
		return err
	}
	defer file.Close()
	encoder := gob.NewEncoder(file)
	return encoder.Encode(db)
}

func loadFromFileGob(name string) (*Database, error) {
	file, err := os.Open(name + ".gob")
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var db Database
	decoder := gob.NewDecoder(file)
	if err := decoder.Decode(&db); err != nil {
		return nil, err
	}
	return &db, nil
}

// Execute processes SQL commands
func (db *Database) Execute(sql string) (string, error) {
	// Basic SQL parsing
	createRegex := regexp.MustCompile(`(?i)^CREATE TABLE (\w+) \((.+)\)$`)
	insertRegex := regexp.MustCompile(`(?i)^INSERT INTO (\w+) \((.+)\) VALUES \((.+)\)$`)
	selectRegex := regexp.MustCompile(`(?i)^SELECT (.+) FROM (\w+)(?: WHERE (.+))?$`)
	deleteRegex := regexp.MustCompile(`(?i)^DELETE FROM (\w+)(?: WHERE (.+))?$`)
	updateRegex := regexp.MustCompile(`(?i)^UPDATE (\w+) SET (.+) WHERE (.+)$`)
	dropTableRegex := regexp.MustCompile(`(?i)^DROP TABLE (\w+)$`)

	switch {
	case strings.HasPrefix(strings.ToUpper(sql), "CREATE TABLE"):
		matches := createRegex.FindStringSubmatch(sql)
		if len(matches) < 3 {
			return "", fmt.Errorf("invalid CREATE TABLE syntax")
		}
		tableName := matches[1]
		columns := strings.Split(matches[2], ",")
		return db.CreateTable(tableName, columns)
	case strings.HasPrefix(strings.ToUpper(sql), "DROP TABLE"):
		matches := dropTableRegex.FindStringSubmatch(sql)
		if len(matches) < 2 {
			return "", fmt.Errorf("invalid DROP TABLE syntax")
		}
		tableName := matches[1]
		return db.DropTable(tableName)
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

	case strings.HasPrefix(strings.ToUpper(sql), "UPDATE"):
		matches := updateRegex.FindStringSubmatch(sql)
		if len(matches) < 4 {
			return "", fmt.Errorf("invalid UPDATE syntax")
		}
		tableName := matches[1]
		setClause := matches[2]
		whereClause := matches[3]
		return db.Update(tableName, setClause, whereClause)
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
	database, err := loadFromFileGob(db.Name)
	if err != nil {
		return "", err
	}
	if _, exists := database.Tables[name]; exists {
		return "", fmt.Errorf("table %s already exists", name)
	}

	table := newTable(name)

	for _, def := range columnDefs {
		if column, err := parseColumnDef(def); err != nil {
			return "", err
		} else {
			table.addColumn(column)
		}
	}

	database.Tables[name] = table
	err = database.saveToFileGob()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("Table %s created", name), nil
}

func (db *Database) DropTable(name string) (string, error) {
	database, err := loadFromFileGob(db.Name)
	if err != nil {
		return "", err
	}
	delete(database.Tables, name)
	err = database.saveToFileGob()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("Table %s dropped", name), nil
}

// Insert adds a new row to a table
func (db *Database) Insert(tableName string, columns []string, values []string) (string, error) {
	database, err := loadFromFileGob(db.Name)
	if err != nil {
		return "", err
	}
	table, exists := database.Tables[tableName]
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
	err = database.saveToFileGob()
	if err != nil {
		return "", err
	}
	return "1 row inserted", nil
}

// Delete removes a row from a table
func (db *Database) Delete(tableName string, whereClause string) (string, error) {
	database, err := loadFromFileGob(db.Name)
	if err != nil {
		return "", err
	}
	table, exists := database.Tables[tableName]
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
	err = database.saveToFileGob()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%d rows deleted", len(results)), nil
}

// Select retrieves data from a table
func (db *Database) Select(tableName string, columns []string, whereClause string) (string, error) {
	database, err := loadFromFileGob(db.Name)
	if err != nil {
		return "", err
	}
	table, exists := database.Tables[tableName]
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
					maps.Copy(resultRow, row)
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

func (db *Database) Update(tableName string, setClause string, whereClause string) (string, error) {
	database, err := loadFromFileGob(db.Name)
	if err != nil {
		return "", err
	}
	table, exists := database.Tables[tableName]
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
	err = database.saveToFileGob()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%d rows updated", rowCount), nil
}

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
			return nil, fmt.Errorf("invalid integer value for column type %s", colType)
		}
		return num, nil
	case COLUMN_TYPE_FLOAT:
		var num float32
		_, err := fmt.Sscanf(val, "%f", &num)
		if err != nil {
			return nil, fmt.Errorf("invalid integer value for column type %s", colType)
		}
		return num, nil
	case COLUMN_TYPE_BOOL:
		var boolean bool
		_, err := fmt.Sscanf(val, "%t", &boolean)
		if err != nil {
			return nil, fmt.Errorf("invalid integer value for column type %s", colType)
		}
		return boolean, nil
	case COLUMN_TYPE_DATE:
		const layout = "2006-01-02"
		parsed_Date, err := time.Parse(layout, val)
		if err != nil {
			return nil, fmt.Errorf("invalid integer value for column type %s", colType)
		}
		return parsed_Date, nil
	default:
		return val, nil
	}
}

func (db *Database) String() string {
	tables := "Tables:\n"
	for _, table := range db.Tables {
		tables += fmt.Sprintf("%s\n", table.String())
	}
	return tables
}

func (db *Database) AllTables() (map[string]*Table, error) {
	database, err := loadFromFileGob(db.Name)
	if err != nil {
		return nil, err
	}
	return database.Tables, nil
}
