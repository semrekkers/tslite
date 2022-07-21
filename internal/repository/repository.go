package repository

import (
	"database/sql"

	_ "embed"
)

//go:embed initialize.sql
var initializeSQL string

func MigrateDatabase(db *sql.DB) error {
	_, err := db.Exec(initializeSQL)
	return err
}
