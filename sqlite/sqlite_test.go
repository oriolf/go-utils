package sqlite

import (
	"embed"
	"testing"
)

//go:embed migrations
var migrationFiles embed.FS

func TestInit(t *testing.T) {
	db, err := Init("db.db", migrationFiles)
	if err != nil {
		t.Errorf("Could not init: %s", err)
	}
	defer db.Close()
}
