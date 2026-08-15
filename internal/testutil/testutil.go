// Package testutil provides small helpers shared across test packages.
package testutil

import (
	"database/sql"
	"testing"
)

// AssertTableCount asserts that COUNT(*) on the given table equals want.
// The table name is a fixed test constant; dynamic input never reaches the
// SQL string.
func AssertTableCount(t *testing.T, db *sql.DB, table string, want int) {
	t.Helper()
	var got int
	if err := db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&got); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	if got != want {
		t.Errorf("count(%s) = %d, want %d", table, got, want)
	}
}
