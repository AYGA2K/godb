package database_test

import (
	"os"
	"strings"
	"testing"

	"github.com/AYGA2K/db/internal/database"
)

func cleanupTestDB() {
	os.Remove("testdb.gob")
}

func TestCreateTable(t *testing.T) {
	defer cleanupTestDB()

	db, err := database.NewDatabase("testdb")
	if err != nil {
		t.Fatal(err)
	}
	res, err := db.Execute("CREATE TABLE users (id INT, name VARCHAR)")
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if res != "Table users created" {
		t.Errorf("Unexpected result: %s", res)
	}
}

func TestInsertAndSelect(t *testing.T) {
	defer cleanupTestDB()

	db, err := database.NewDatabase("testdb")
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Execute("CREATE TABLE users (id INT, name VARCHAR)")
	if err != nil {
		t.Fatal(err)
	}
	res, err := db.Execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")
	if err != nil {
		t.Fatalf("Insert error: %v", err)
	}
	if res != "1 row inserted" {
		t.Errorf("Unexpected insert result: %s", res)
	}

	selectRes, err := db.Execute("SELECT * FROM users")
	if err != nil {
		t.Fatalf("Select error: %v", err)
	}
	if !strings.Contains(selectRes, `"name": "Alice"`) || !strings.Contains(selectRes, `"id": 1`) {
		t.Errorf("Unexpected select result: %s", selectRes)
	}
}

func TestWhereClause(t *testing.T) {
	defer cleanupTestDB()

	db, err := database.NewDatabase("testdb")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = db.Execute("CREATE TABLE users (id INT, name VARCHAR)")
	_, _ = db.Execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")
	_, _ = db.Execute("INSERT INTO users (id, name) VALUES (2, 'Bob')")

	res, err := db.Execute("SELECT name FROM users WHERE id = 2")
	if err != nil {
		t.Fatalf("Select with where error: %v", err)
	}
	if !strings.Contains(res, `"name": "Bob"`) || strings.Contains(res, `"name": "Alice"`) {
		t.Errorf("Expected result to contain Bob but not Alice, got: %s", res)
	}
}

func TestDelete(t *testing.T) {
	defer cleanupTestDB()

	db, err := database.NewDatabase("testdb")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = db.Execute("CREATE TABLE users (id INT, name VARCHAR)")
	_, _ = db.Execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")
	_, _ = db.Execute("INSERT INTO users (id, name) VALUES (2, 'Bob')")

	res, err := db.Execute("DELETE FROM users WHERE id = 1")
	if err != nil {
		t.Fatalf("Delete error: %v", err)
	}
	if res != "1 rows deleted" {
		t.Errorf("Unexpected delete result: %s", res)
	}
	selectRes, err := db.Execute("SELECT * FROM users")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(selectRes, `"id": 1`) || !strings.Contains(selectRes, `"id": 2`) {
		t.Errorf("Expected Alice to be deleted and Bob to remain, got: %s", selectRes)
	}
}

func TestUpdate(t *testing.T) {
	defer cleanupTestDB()

	db, err := database.NewDatabase("testdb")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = db.Execute("CREATE TABLE users (id INT, name VARCHAR)")
	_, _ = db.Execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")
	_, _ = db.Execute("INSERT INTO users (id, name) VALUES (2, 'Bob')")

	res, err := db.Execute("UPDATE users SET name = 'Charlie' WHERE id = 1")
	if err != nil {
		t.Fatalf("Update error: %v", err)
	}
	if res != "1 rows updated" {
		t.Errorf("Unexpected update result: %s", res)
	}
	selectRes, err := db.Execute("SELECT * FROM users")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(selectRes, `"name": "Charlie"`) ||
		!strings.Contains(selectRes, `"id": 1`) ||
		!strings.Contains(selectRes, `"name": "Bob"`) {
		t.Errorf("Expected Charlie for id 1 and Bob to remain unchanged, got: %s", selectRes)
	}
}

func TestDropTable(t *testing.T) {
	defer cleanupTestDB()

	db, err := database.NewDatabase("testdb")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = db.Execute("CREATE TABLE users (id INT, name VARCHAR)")
	_, _ = db.Execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")
	_, _ = db.Execute("INSERT INTO users (id, name) VALUES (2, 'Bob')")

	res, err := db.Execute("DROP TABLE users")
	if err != nil {
		t.Fatalf("Drop table error: %v", err)
	}
	if res != "Table users dropped" {
		t.Errorf("Unexpected drop table result: %s", res)
	}
	_, exists := db.Tables["users"]
	if exists {
		t.Errorf("Table users still exists")
	}
}

func TestColumnTypeParsing(t *testing.T) {
	defer cleanupTestDB()
	db, err := database.NewDatabase("testdb")
	if err != nil {
		t.Fatal(err)
	}

	// Define test cases
	tests := []struct {
		tableName   string
		createStmt  string
		expectExist bool
		colType     database.ColumnType
		constraints []database.ColumnConstraint
	}{
		{
			"test_int",
			"CREATE TABLE test_int (col INT)",
			true,
			database.COLUMN_TYPE_INT,
			nil,
		},
		{
			"test_double",
			"CREATE TABLE test_double (col DOUBLE)",
			true,
			database.COLUMN_TYPE_DOUBLE,
			nil,
		},
		{
			"test_float",
			"CREATE TABLE test_float (col FLOAT)",
			true,
			database.COLUMN_TYPE_FLOAT,
			nil,
		},
		{
			"test_varchar",
			"CREATE TABLE test_varchar (col VARCHAR)",
			true,
			database.COLUMN_TYPE_VARCHAR,
			nil,
		},
		{
			"test_bool",
			"CREATE TABLE test_bool (col BOOL)",
			true,
			database.COLUMN_TYPE_BOOL,
			nil,
		},
		{
			"test_date",
			"CREATE TABLE test_date (col DATE)",
			true,
			database.COLUMN_TYPE_DATE,
			nil,
		},
		{
			"test_enum",
			"CREATE TABLE test_enum (col ENUM)",
			true,
			database.COLUMN_TYPE_ENUM,
			nil,
		},
		{
			"test_invalid_type",
			"CREATE TABLE test_invalid_type (col INVALID_TYPE)",
			false,
			"",
			nil,
		},

		{
			"test_null",
			"CREATE TABLE test_null (col INT NULL)",
			true,
			database.COLUMN_TYPE_INT,
			[]database.ColumnConstraint{database.COLUMN_CONSTRAINT_NULL},
		},

		{
			"test_not_null",
			"CREATE TABLE test_not_null (col INT NOT NULL)",
			true,
			database.COLUMN_TYPE_INT,
			[]database.ColumnConstraint{database.COLUMN_CONSTRAINT_NOT_NULL},
		},

		// {
		// 	"test_foreign_key",
		// 	"CREATE TABLE test_foreign_key (col INT FOREIGN KEY)",
		// 	true,
		// 	database.COLUMN_TYPE_INT,
		// 	[]database.ColumnConstraint{database.COLUMN_CONSTRAINT_FOREIGN_KEY},
		// },

		{
			"test_unique_index",
			"CREATE TABLE test_unique_index (col INT UNIQUE)",
			true,
			database.COLUMN_TYPE_INT,
			[]database.ColumnConstraint{database.COLUMN_CONSTRAINT_UNIQUE},
		},

		{
			"test_primary_key_index",
			"CREATE TABLE test_primary_key_index (col INT PRIMARY KEY)",
			true,
			database.COLUMN_TYPE_INT,
			[]database.ColumnConstraint{database.COLUMN_CONSTRAINT_PRIMARY_KEY},
		},

		{
			"test_auto_increment_index",
			"CREATE TABLE test_auto_increment_index (col INT AUTO_INCREMENT)",
			true,

			database.COLUMN_TYPE_INT,

			[]database.ColumnConstraint{database.COLUMN_CONSTRAINT_AUTO_INCREMENT},
		},

		// {
		// 	"test_foreign_key_index",
		// 	"CREATE TABLE test_foreign_key_index (col INT FOREIGN KEY)",
		// 	true,
		// 	database.COLUMN_TYPE_INT,
		// 	[]database.ColumnConstraint{database.COLUMN_CONSTRAINT_FOREIGN_KEY},
		// },
	}

	// Create tables
	for _, test := range tests {
		_, _ = db.Execute(test.createStmt)
	}

	tables, err := db.AllTables()
	if err != nil {
		t.Errorf("Expected no error, got: %s", err)
	}

	// Verify each test case
	for _, test := range tests {
		t.Run(test.tableName, func(t *testing.T) {
			table, exists := tables[test.tableName]
			if exists != test.expectExist {
				t.Errorf("Expected table %s existence to be %v, got %v", test.tableName, test.expectExist, exists)
				return
			}

			if !test.expectExist {
				return
			}

			if len(table.GetColumns()) == 0 {
				t.Errorf("Expected at least one column, got none")
				return
			}

			column := table.GetColumns()[0]
			if column.Name != "col" {
				t.Errorf("Expected column name to be 'col', got: %s", column.Name)
			}
			if column.Type != test.colType {
				t.Errorf("Expected column type to be %s, got: %s", test.colType, column.Type)
			}
			if len(column.Constraints) != len(test.constraints) {
				t.Errorf("Expected %d constraints, got: %v", len(test.constraints), column.Constraints)
			} else {
				for i, constraint := range test.constraints {
					if column.Constraints[i] != constraint {
						t.Errorf("Expected constraint %d to be %s, got: %s", i, constraint, column.Constraints[i])
					}
				}
			}
		})
	}
}

func TestPrimaryKeyAutoIncrement(t *testing.T) {
	defer cleanupTestDB()

	db, err := database.NewDatabase("testdb")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = db.Execute("CREATE TABLE users (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR)")
	_, _ = db.Execute("INSERT INTO users (name) VALUES ( 'Alice')")
	_, _ = db.Execute("INSERT INTO users (name) VALUES ( 'Bob')")

	res, err := db.Execute("SELECT * FROM users WHERE id = 2")
	if err != nil {
		t.Fatalf("Select with where error: %v", err)
	}
	if !strings.Contains(res, `"id": 2`) {
		t.Errorf("Expected result to contain id 2, got: %s", res)
	}
}
