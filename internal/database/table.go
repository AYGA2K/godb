package database

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

// Row represents a table row (record)
type Row map[string]interface{}

// Table represents a database table
type Table struct {
	Name    string
	Columns []Column
	Rows    []Row
}
