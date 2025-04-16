package database

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

func isValidColumnConstraint(c ColumnConstraint) bool {
	switch c {
	case COLUMN_CONSTRAINT_NULL,
		COLUMN_CONSTRAINT_NOT_NULL,
		COLUMN_CONSTRAINT_AUTO_INCREMENT,
		COLUMN_CONSTRAINT_FOREIGN_KEY,
		COLUMN_CONSTRAINT_PRIMARY_KEY,
		COLUMN_CONSTRAINT_UNIQUE:
		return true
	default:
		return false
	}
}
