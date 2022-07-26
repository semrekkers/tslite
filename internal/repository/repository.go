package repository

import (
	"database/sql"
	"path"

	"embed"
)

var (
	//go:embed migrations/*.sql
	sqlMigrations    embed.FS
	sqlMigrationsDir = "migrations"
)

func MigrateDatabase(db *sql.DB) {
	items, err := sqlMigrations.ReadDir(sqlMigrationsDir)
	if err != nil {
		panic(err)
	}
	for _, item := range items {
		contents, err := sqlMigrations.ReadFile(path.Join(sqlMigrationsDir, item.Name()))
		if err != nil {
			panic(err)
		}
		_, err = db.Exec(string(contents))
		if err != nil {
			panic(err)
		}
	}
}
