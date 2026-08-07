// Command db_density_report reports SQLite page and table/index density.
package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"

	_ "github.com/mattn/go-sqlite3"
)

type objectSize struct {
	name  string
	kind  string
	bytes int64
}

func main() {
	flag.Parse()
	if flag.NArg() != 1 {
		fmt.Fprintf(os.Stderr, "usage: db_density_report DATABASE\n")
		os.Exit(2)
	}
	if err := report(flag.Arg(0)); err != nil {
		log.Fatal(err)
	}
}

func report(path string) error {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer func() { _ = db.Close() }()

	if err := db.Ping(); err != nil {
		return fmt.Errorf("ping database: %w", err)
	}

	pageCount, err := pragmaInt64(db, "page_count")
	if err != nil {
		return err
	}
	pageSize, err := pragmaInt64(db, "page_size")
	if err != nil {
		return err
	}
	freelist, err := pragmaInt64(db, "freelist_count")
	if err != nil {
		return err
	}

	eventCount, err := tableCount(db, "log_event")
	if err != nil {
		return err
	}
	attrCount, err := tableCount(db, "log_attr")
	if err != nil {
		return err
	}

	fileBytes := int64(0)
	if info, statErr := os.Stat(path); statErr == nil {
		fileBytes = info.Size()
	}

	fmt.Printf("database: %s\n", path)
	fmt.Printf("file_bytes: %d\n", fileBytes)
	fmt.Printf("page_count: %d\n", pageCount)
	fmt.Printf("page_size: %d\n", pageSize)
	fmt.Printf("freelist_count: %d\n", freelist)
	fmt.Printf("logical_bytes: %d\n", pageCount*pageSize)
	fmt.Printf("log_event_count: %d\n", eventCount)
	fmt.Printf("log_attr_count: %d\n", attrCount)
	if eventCount > 0 {
		fmt.Printf("logical_bytes_per_event: %.2f\n", float64(pageCount*pageSize)/float64(eventCount))
	}

	objects, err := dbstatObjects(db)
	if err != nil {
		// dbstat is a diagnostic virtual table and is not available in every
		// SQLite build. The page-level report remains useful without it.
		fmt.Printf("dbstat: unavailable (%v)\n", err)
		return nil
	}
	fmt.Println("dbstat_bytes:")
	for _, object := range objects {
		fmt.Printf("  %s\t%s\t%d\n", object.kind, object.name, object.bytes)
	}
	return nil
}

func pragmaInt64(db *sql.DB, name string) (int64, error) {
	var value int64
	if err := db.QueryRow("PRAGMA " + name).Scan(&value); err != nil {
		return 0, fmt.Errorf("read pragma %s: %w", name, err)
	}
	return value, nil
}

func tableCount(db *sql.DB, table string) (int64, error) {
	var exists int
	if err := db.QueryRow(
		"SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name=?)",
		table,
	).Scan(&exists); err != nil {
		return 0, fmt.Errorf("check table %s: %w", table, err)
	}
	if exists == 0 {
		return 0, nil
	}

	var count int64
	if err := db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); err != nil {
		return 0, fmt.Errorf("count table %s: %w", table, err)
	}
	return count, nil
}

func dbstatObjects(db *sql.DB) ([]objectSize, error) {
	rows, err := db.Query(`
		SELECT name, MAX(CASE WHEN name IN (
			SELECT name FROM sqlite_master WHERE type='index'
		) THEN 'index' ELSE 'table' END), SUM(pgsize)
		FROM dbstat
		GROUP BY name`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var objects []objectSize
	for rows.Next() {
		var object objectSize
		if err := rows.Scan(&object.name, &object.kind, &object.bytes); err != nil {
			return nil, err
		}
		objects = append(objects, object)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.Slice(objects, func(i, j int) bool {
		if objects[i].kind == objects[j].kind {
			return objects[i].name < objects[j].name
		}
		return objects[i].kind < objects[j].kind
	})
	return objects, nil
}
