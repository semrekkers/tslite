package repository

import (
	"database/sql"
	"path"
	"time"

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
	if _, err = db.Exec("ANALYZE"); err != nil {
		panic(err)
	}
}

type ValueSample struct {
	Timestamp time.Time `json:"ts"`
	Value     float64   `json:"value"`
}

type TimeFrame struct {
	Start, End time.Time
	Interval   int64
}

func GetValueSamples(db *sql.DB, tf TimeFrame) ([]ValueSample, error) {
	const query = `SELECT ts, value FROM value_all WHERE ts BETWEEN ? AND ? ORDER BY ts`

	startTs := tf.Start.Unix()
	rows, err := db.Query(query, startTs, tf.End.Unix())
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var (
		results = make([]ValueSample, 0, 32)
		nextTs  = startTs
		ts      int64
		value   float64
	)
	for rows.Next() {
		if err = rows.Scan(&ts, &value); err != nil {
			return nil, err
		}
		if ts < nextTs {
			continue // skip this sample, aligning with the Interval
		}
		results = append(results, ValueSample{
			Timestamp: time.Unix(ts, 0),
			Value:     value,
		})
		nextTs += tf.Interval
	}

	return results, nil
}
