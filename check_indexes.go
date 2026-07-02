package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	_ "modernc.org/sqlite"
)

func main() {
	dbPath := "otel-logs.db"
	if len(os.Args) > 1 {
		dbPath = os.Args[1]
	}

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT name, tbl_name FROM sqlite_master WHERE type='index' ORDER BY tbl_name, name")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	fmt.Println("Indexes in database:")
	for rows.Next() {
		var name, table string
		if err := rows.Scan(&name, &table); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("  %s.%s\n", table, name)
	}
}
