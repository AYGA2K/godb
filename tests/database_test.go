package database_test

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/AYGA2K/db/internal/database"
)

func cleanupTestDB(name string) {
	os.Remove(name + ".gob")
}

func TestCreateTable(t *testing.T) {
	defer cleanupTestDB("testdb")

	db, err := database.NewDatabase("testdb")
	if err != nil {
		t.Fatal(err)
	}
	res, err := db.Execute("CREATE TABLE users (id INT, name VARCHAR ,birthdate DATE)")
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if res != "Table users created" {
		t.Errorf("Unexpected result: %s", res)
	}
}

func TestInsertAndSelect(t *testing.T) {
	defer cleanupTestDB("testdb")

	db, err := database.NewDatabase("testdb")
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Execute("CREATE TABLE users (id INT, name VARCHAR, birthdate DATE)")
	if err != nil {
		t.Fatal(err)
	}
	res, err := db.Execute("INSERT INTO users (id, name, birthdate) VALUES (1, 'Alice','1990-01-01')")
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
	if !strings.Contains(selectRes, `"name": "Alice"`) || !strings.Contains(selectRes, `"id": 1`) || !strings.Contains(selectRes, `"birthdate": "1990-01-01"`) {
		t.Errorf("Unexpected select result: %s", selectRes)
	}
}

func TestWhereClause(t *testing.T) {
	defer cleanupTestDB("testdb")

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
	defer cleanupTestDB("testdb")

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
	defer cleanupTestDB("testdb")

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
	defer cleanupTestDB("testdb")

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
	defer cleanupTestDB("testdb")
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
	defer cleanupTestDB("testdb")

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

func TestForeignKey(t *testing.T) {
	defer cleanupTestDB("testdb")
	db, err := database.NewDatabase("testdb")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = db.Execute("CREATE TABLE users (id INT, name VARCHAR)")
	_, _ = db.Execute("CREATE TABLE posts (id INT, user_id INT FOREIGN KEY REFERENCES users(id) , title VARCHAR)")
	_, _ = db.Execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")
	_, _ = db.Execute("INSERT INTO posts (id, user_id, title) VALUES (1, 1, 'Hello')")

	res, err := db.Execute("SELECT * FROM posts where user_id = 1")
	if err != nil {
		t.Fatalf("Select with where error: %v", err)
	}
	if !strings.Contains(res, `"user_id": 1`) {
		t.Errorf("Expected result to contain user_id 1, got: %s", res)
	}
}

func TestSelectJoin(t *testing.T) {
	defer cleanupTestDB("testdb")
	db, err := database.NewDatabase("testdb")
	if err != nil {
		t.Fatal(err)
	}

	_, _ = db.Execute("CREATE TABLE users (id INT, name VARCHAR)")
	_, _ = db.Execute("CREATE TABLE posts (id INT, user_id INT FOREIGN KEY REFERENCES users(id), title VARCHAR)")
	_, _ = db.Execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")
	_, _ = db.Execute("INSERT INTO users (id, name) VALUES (2, 'Bob')")
	_, _ = db.Execute("INSERT INTO posts (id, user_id, title) VALUES (1, 1, 'Hello')")
	_, _ = db.Execute("INSERT INTO posts (id, user_id, title) VALUES (2, 2, 'World')")

	res, err := db.Execute("SELECT posts.title, users.name FROM posts JOIN users ON posts.user_id = users.id")
	if err != nil {
		t.Fatalf("Select with join error: %v", err)
	}

	if !strings.Contains(res, `"posts.title": "Hello"`) || !strings.Contains(res, `"users.name": "Alice"`) {
		t.Errorf("Expected result to contain post 'Hello' by 'Alice', got: %s", res)
	}
	if !strings.Contains(res, `"posts.title": "World"`) || !strings.Contains(res, `"users.name": "Bob"`) {
		t.Errorf("Expected result to contain post 'World' by 'Bob', got: %s", res)
	}
}

func TestSelectLimit(t *testing.T) {
	defer cleanupTestDB("testdb")
	db, err := database.NewDatabase("testdb")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = db.Execute("CREATE TABLE users (id INT, name VARCHAR)")
	_, _ = db.Execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")
	_, _ = db.Execute("INSERT INTO users (id, name) VALUES (2, 'Bob')")
	_, _ = db.Execute("INSERT INTO users (id, name) VALUES (3, 'Charlie')")
	_, _ = db.Execute("INSERT INTO users (id, name) VALUES (4, 'David')")

	res, err := db.Execute(fmt.Sprintf("SELECT * FROM users LIMIT %d", 3))
	if err != nil {
		t.Fatalf("Select with limit error: %v", err)
	}
	if !strings.Contains(res, `"id": 1`) || !strings.Contains(res, `"id": 2`) || !strings.Contains(res, `"id": 3`) {
		t.Errorf("Expected result to contain id 1, 2, 3, got: %s", res)
	}
	if strings.Contains(res, `"id": 4`) {
		t.Errorf("Expected result to not contain id 4, got: %s", res)
	}
}

func TestSelectOrderBy(t *testing.T) {
	defer cleanupTestDB("testdb")
	db, err := database.NewDatabase("testdb")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = db.Execute("CREATE TABLE users (id INT, name VARCHAR)")
	_, _ = db.Execute("INSERT INTO users (id, name) VALUES (1, 'Charlie')")
	_, _ = db.Execute("INSERT INTO users (id, name) VALUES (2, 'Alice')")
	_, _ = db.Execute("INSERT INTO users (id, name) VALUES (3, 'David')")
	_, _ = db.Execute("INSERT INTO users (id, name) VALUES (4, 'Bob')")

	res, err := db.Execute("SELECT * FROM users ORDER BY name")
	if err != nil {
		t.Fatalf("Select with order by error: %v", err)
	}

	// Check that results are in alphabetical order by name
	expectedOrder := []string{"Alice", "Bob", "Charlie", "David"}
	var results []map[string]interface{}
	if err := json.Unmarshal([]byte(res), &results); err != nil {
		t.Fatalf("Failed to unmarshal results: %v", err)
	}

	if len(results) != len(expectedOrder) {
		t.Fatalf("Expected %d results, got %d", len(expectedOrder), len(results))
	}

	for i, row := range results {

		name, ok := row["name"].(string)
		if !ok {
			t.Errorf("Name field is not a string in row %d", i)
			continue
		}
		if name != expectedOrder[i] {
			t.Errorf("Expected name at position %d to be %s, got %s", i, expectedOrder[i], name)
		}
	}
}

func TestComparisonOperators(t *testing.T) {
	defer cleanupTestDB("testdb")

	db, err := database.NewDatabase("testdb")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = db.Execute("CREATE TABLE users (id INT, name VARCHAR, age INT, birthdate DATE)")
	_, _ = db.Execute("INSERT INTO users (id, name, age, birthdate) VALUES (1, 'Alice', 25,'2000-01-01')")
	_, _ = db.Execute("INSERT INTO users (id, name, age,birthdate) VALUES (2, 'Bob', 30,'1995-03-20')")
	_, _ = db.Execute("INSERT INTO users (id, name, age,birthdate) VALUES (3, 'Charlie', 35,'1990-03-12')")
	_, _ = db.Execute("INSERT INTO users (id, name, age,birthdate) VALUES (4, 'David', 40,'1985-02-03')")

	tests := []struct {
		name     string
		query    string
		expected []int // expected number of results
	}{
		{"Less than", "SELECT * FROM users WHERE age < 30", []int{1}},
		{"Greater than", "SELECT * FROM users WHERE age > 30", []int{3, 4}},
		{"Less than or equal", "SELECT * FROM users WHERE age <= 30", []int{1, 2}},
		{"Greater than or equal", "SELECT * FROM users WHERE age >= 30", []int{2, 3, 4}},
		{"Not equal", "SELECT * FROM users WHERE age != 30", []int{1, 3, 4}},
		{"String less than", "SELECT * FROM users WHERE name < 'Bob'", []int{1}},
		{"String greater than", "SELECT * FROM users WHERE name > 'Bob'", []int{3, 4}},
		{"Strign Like", "SELECT * FROM users WHERE name LIKE 'li'", []int{1, 3}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := db.Execute(tt.query)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}

			var results []map[string]interface{}
			if err := json.Unmarshal([]byte(res), &results); err != nil {
				t.Fatalf("Failed to unmarshal results: %v", err)
			}

			if len(results) != len(tt.expected) {
				t.Errorf("Expected %d results, got %d", tt.expected, len(results))
			}
		})
	}
}

func TestConcurrentInserts(t *testing.T) {
	defer cleanupTestDB("testdbconcurrent")
	db, err := database.NewDatabase("testdbconcurrent")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = db.Execute("CREATE TABLE users (id INT, name VARCHAR)")

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			name := fmt.Sprintf("User%d", i)
			_, err := db.Execute(fmt.Sprintf("INSERT INTO users (id, name) VALUES (%d, '%s')", i, name))
			if err != nil {
				t.Errorf("Insert failed: %v", err)
			}
		}(i)
	}
	wg.Wait()

	res, err := db.Execute("SELECT * FROM users")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		if !strings.Contains(res, fmt.Sprintf(`"name": "User%d"`, i)) {
			t.Errorf("Expected User%d in result, got: %s", i, res)
		}
	}
}
