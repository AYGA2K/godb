package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/AYGA2K/db/internal/database"
)

func main() {
	fmt.Println("Simple SQL Database in Go")
	db := database.NewDatabase("testdb")

	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("sql> ")
		sql, _ := reader.ReadString('\n')
		sql = strings.TrimSpace(sql)
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
