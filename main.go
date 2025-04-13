package main

import (
	"fmt"
	"log"
	"strings"

	"github.com/AYGA2K/db/internal/database"
	"github.com/chzyer/readline"
)

func main() {
	fmt.Println("Simple SQL Database in Go")
	db, err := database.NewDatabase("testdb")
	if err != nil {
		log.Fatal(err)
	}

	rl, err := readline.NewEx(&readline.Config{
		Prompt:          "sql> ",
		HistoryFile:     "/tmp/sql_history.tmp", // Stores history between sessions
		InterruptPrompt: "^C",
		EOFPrompt:       "exit",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer rl.Close()

	for {
		sql, err := rl.Readline()
		if err != nil { // Handles Ctrl+C or Ctrl+D
			break
		}

		sql = strings.TrimSpace(sql)
		if sql == "" {
			continue
		}
		if sql == "exit" {
			break
		}

		result, err := db.Execute(sql)
		if err != nil {
			fmt.Println("Error:", err)
		} else {
			fmt.Println(result)
		}
	}
}
