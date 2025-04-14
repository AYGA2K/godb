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
