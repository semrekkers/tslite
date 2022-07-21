package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"net/url"
	"os"

	"go.uber.org/zap"

	"github.com/semrekkers/go-sqlite3"
	"github.com/semrekkers/tslite/internal/repository"
)

const defaultDB = "test.db?_journal=wal&_sync=normal&_timeout=8000&_fk=on"

func main() {
	var (
		flagDB = flag.String("db", defaultDB, "Database `file` name")
	)
	flag.Parse()

	logger, err := zap.NewDevelopment()
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error creating logger:", err)
		os.Exit(1)
	}
	zap.ReplaceGlobals(logger)
	log := logger.Sugar()

	sql.Register("tslite", &sqlite3.SQLiteDriver{
		Extensions:  []string{"./tslite/tslite"},
		ConnectHook: initConn,
	})

	dbDSN, err := url.Parse(*flagDB)
	if err != nil {
		log.Fatal(err)
	}

	if err = setupDB(dbDSN.Path); err != nil {
		log.Fatal(err)
	}

	db, err := sql.Open("tslite", *flagDB)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	useDB(db, log)
}

func useDB(db *sql.DB, log *zap.SugaredLogger) {
	if err := repository.MigrateDatabase(db); err != nil {
		log.Error(err)
	}
}

func initConn(conn *sqlite3.SQLiteConn) error {
	_, err := conn.Exec(`PRAGMA mmap_size = 268435456`, nil)
	if err != nil {
		return err
	}
	_, err = conn.Exec(`PRAGMA wal_autocheckpoint = 10000`, nil)
	return err
}

func setupDB(fileName string) error {
	_, err := os.Stat(fileName)
	if err == nil {
		// File exists, no need for setup.
		return nil
	} else if !os.IsNotExist(err) {
		// Other error.
		return err
	}
	db, err := sql.Open("sqlite3", fileName)
	if err != nil {
		return err
	}
	defer db.Close()
	conn, err := db.Conn(context.Background())
	if err != nil {
		return err
	}
	defer conn.Close()
	_, err = conn.ExecContext(context.Background(), `PRAGMA page_size = 65536`)
	if err != nil {
		return err
	}
	_, err = conn.ExecContext(context.Background(), `VACUUM`)
	return err
}
