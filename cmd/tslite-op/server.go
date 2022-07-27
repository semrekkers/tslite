package main

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"time"

	"github.com/semrekkers/tslite/internal/repository"

	"github.com/go-chi/chi/v5"
	"go.uber.org/zap"
)

func setupHTTPServer(db *sql.DB, log *zap.SugaredLogger) {
	const httpAddr = ":8080"

	r := chi.NewRouter()

	r.Route("/api", func(r chi.Router) {
		r.Get("/samples", func(w http.ResponseWriter, r *http.Request) {
			var (
				start, _    = time.Parse(time.RFC3339, r.URL.Query().Get("start"))
				end, _      = time.Parse(time.RFC3339, r.URL.Query().Get("end"))
				interval, _ = time.ParseDuration(r.URL.Query().Get("interval"))
			)
			if start.IsZero() {
				start = time.Now()
			}
			if end.IsZero() {
				end = start.Add(1 * time.Hour)
			}
			log.Infow("Timeframe", "start", start, "end", end, "interval", interval)

			results, err := repository.GetValueSamples(db, repository.TimeFrame{
				Start:    start,
				End:      end,
				Interval: interval.Milliseconds() / 1000,
			})
			if err != nil {
				log.Errorw("Get samples error", err)
				serveError(w, http.StatusInternalServerError, err)
				return
			}

			serveJSON(w, http.StatusOK, results)
		})
	})

	log.Infow("HTTP server listening", "addr", httpAddr)
	err := http.ListenAndServe(httpAddr, r)
	if err != nil {
		log.Errorw("HTTP server", err)
	}
}

func serveJSON(w http.ResponseWriter, statusCode int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(v)
}

func serveError(w http.ResponseWriter, statusCode int, err error) {
	serveJSON(w, statusCode, struct {
		Message string `json:"message"`
	}{
		Message: err.Error(),
	})
}
