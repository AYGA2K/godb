# Go Database

A simple database with basic SQL support.

## Supported SQL Syntax

### Table Operations

```sql
-- Create table 
CREATE TABLE users (id INT, name VARCHAR)

-- Drop table
DROP TABLE users
```

### Data Manipulation

```sql
-- Insert data
INSERT INTO users (id, name) VALUES (1, 'Alice')

-- Update data
UPDATE users SET name = 'Charlie' WHERE id = 1

-- Delete data
DELETE FROM users WHERE id = 1
```

### Querying Data

```sql
-- Basic select
SELECT * FROM users

-- Select specific columns
SELECT name FROM users

-- Select with WHERE
SELECT name FROM users WHERE id = 2

-- Select with JOIN
SELECT posts.title, users.name 
FROM posts 
JOIN users ON posts.user_id = users.id

-- Select with LIMIT
SELECT * FROM users LIMIT 3

-- Select with ORDER BY
SELECT * FROM users ORDER BY name
```

## Data Types

- `INT`
- `VARCHAR`
- `DOUBLE`
- `FLOAT`
- `BOOL`
- `DATE`
- `ENUM`

## Constraints

- `PRIMARY KEY`
- `FOREIGN KEY`
- `AUTO_INCREMENT`
- `NULL`
- `NOT NULL`
- `UNIQUE`
