package sqlite

import (
	"database/sql"
	"embed"
	"fmt"
	"sort"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

func Init(filename string, migrationFiles embed.FS) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", filename)
	if err != nil {
		return nil, fmt.Errorf("could not open db: %w", err)
	}

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS migrations (
    	id   INTEGER NOT NULL PRIMARY KEY,
		name TEXT NOT NULL,
		time TIMESTAMP NOT NULL
	);`)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("could not create migrations table: %w", err)
	}

	const migrationsFolder = "migrations"
	files, err := migrationFiles.ReadDir(migrationsFolder)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("could not read migration files: %w", err)
	}

	sort.Slice(files, func(i, j int) bool { return files[i].Name() < files[j].Name() })
	for _, file := range files {
		b, err := migrationFiles.ReadFile(migrationsFolder + "/" + file.Name())
		if err != nil {
			db.Close()
			return nil, fmt.Errorf("could not read migration file %s: %w", file.Name(), err)
		}

		err = migrateFile(db, file.Name(), string(b))
		if err != nil {
			db.Close()
			return nil, fmt.Errorf("could not execute migration file %s: %w", file.Name(), err)
		}
	}

	return db, nil
}

func migrateFile(db *sql.DB, filename, contents string) error {
	return transaction(db, func(tx *sql.Tx) error {
		var count int
		err := db.QueryRow("SELECT COUNT(1) FROM migrations WHERE name=?;", filename).Scan(&count)
		if err != nil {
			return fmt.Errorf("could not check if previous migration existed: %w", err)
		}

		if count == 0 {
			if _, err := db.Exec(contents); err != nil {
				return fmt.Errorf("could not execute migration: %w", err)
			}

			if _, err := db.Exec("INSERT INTO migrations (name, time) VALUES (?, ?);", filename, time.Now().Unix()); err != nil {
				return fmt.Errorf("could not set migration as executed: %w", err)
			}
		}

		return nil
	})
}

func transaction(db *sql.DB, f func(*sql.Tx) error) error {
	tx, err := db.Begin()
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("could not begin transaction: %w", err)
	}

	if err := f(tx); err != nil {
		tx.Rollback()
		return fmt.Errorf("could not execute function: %w", err)
	}

	if err := tx.Commit(); err != nil {
		tx.Rollback()
		return fmt.Errorf("could not commit transaction: %w", err)
	}

	return nil
}
