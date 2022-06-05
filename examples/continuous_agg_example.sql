-- Usage:
-- cat continuous_agg_example.sql | ./sqlite3 --cmd '.load ./tslite.so' --cmd 'PRAGMA page_size=32768; PRAGMA journal_mode=off; PRAGMA synchronous=off' --echo


-- First create the "reality" sampled per second
CREATE TABLE timeseries_1s AS
WITH gen AS (
	SELECT
		value AS ts,
		abs(random() % 10000) / 100000.0 AS delta
	FROM generate_series(unixepoch('2019-01-01'), unixepoch('2022-01-01'), interval('1s'))
)
SELECT
	ts,
	sum(delta) OVER (ORDER BY ts) AS value
FROM gen;

CREATE UNIQUE INDEX idx_timeseries_1s_ts ON timeseries_1s(ts DESC);

-- Create aggregation step for 1 minute
CREATE TABLE timeseries_1m AS
SELECT
	ts_1m AS ts,
	value
FROM (
	SELECT
		time_bucket(interval('1m'), ts) AS ts_1m,
		avg(value) AS value
	FROM timeseries_1s
	WHERE ts < unixepoch((SELECT max(ts) FROM timeseries_1s), 'unixepoch', '-1 month')
	GROUP BY ts_1m
);

CREATE UNIQUE INDEX idx_timeseries_1m_ts ON timeseries_1m(ts DESC);

DELETE FROM timeseries_1s WHERE ts < unixepoch((SELECT max(ts) FROM timeseries_1s), 'unixepoch', '-1 month');

-- Create aggregation step for 15 minutes
CREATE TABLE timeseries_15m AS
SELECT
	ts_15m AS ts,
	value
FROM (
	SELECT
		time_bucket(interval('15m'), ts) AS ts_15m,
		avg(value) AS value
	FROM timeseries_1m
	WHERE ts < unixepoch((SELECT max(ts) FROM timeseries_1s), 'unixepoch', '-2 months')
	GROUP BY ts_15m
);

CREATE UNIQUE INDEX idx_timeseries_15m_ts ON timeseries_15m(ts DESC);

DELETE FROM timeseries_1m WHERE ts < unixepoch((SELECT max(ts) FROM timeseries_1s), 'unixepoch', '-2 months');

-- Create aggregation step for 30 minutes
CREATE TABLE timeseries_30m AS
SELECT
	ts_30m AS ts,
	value
FROM (
	SELECT
		time_bucket(interval('30m'), ts) AS ts_30m,
		avg(value) AS value
	FROM timeseries_15m
	WHERE ts < unixepoch((SELECT max(ts) FROM timeseries_1s), 'unixepoch', '-3 months')
	GROUP BY ts_30m
);

CREATE UNIQUE INDEX idx_timeseries_30m_ts ON timeseries_30m(ts DESC);

DELETE FROM timeseries_15m WHERE ts < unixepoch((SELECT max(ts) FROM timeseries_1s), 'unixepoch', '-3 months');

-- Create aggregation step for 1 hour
CREATE TABLE timeseries_1h AS
SELECT
	ts_1h AS ts,
	value
FROM (
	SELECT
		time_bucket(interval('1h'), ts) AS ts_1h,
		avg(value) AS value
	FROM timeseries_30m
	WHERE ts < unixepoch((SELECT max(ts) FROM timeseries_1s), 'unixepoch', '-4 months')
	GROUP BY ts_1h
);

CREATE UNIQUE INDEX idx_timeseries_1h_ts ON timeseries_1h(ts DESC);

DELETE FROM timeseries_30m WHERE ts < unixepoch((SELECT max(ts) FROM timeseries_1s), 'unixepoch', '-4 months');

-- Create aggregation step for 1 day
CREATE TABLE timeseries_1d AS
SELECT
	ts_1d AS ts,
	value
FROM (
	SELECT
		time_bucket(interval('1d'), ts) AS ts_1d,
		avg(value) AS value
	FROM timeseries_1h
	WHERE ts < unixepoch((SELECT max(ts) FROM timeseries_1s), 'unixepoch', '-10 months')
	GROUP BY ts_1d
);

CREATE UNIQUE INDEX idx_timeseries_1d_ts ON timeseries_1d(ts DESC);

DELETE FROM timeseries_1h WHERE ts < unixepoch((SELECT max(ts) FROM timeseries_1s), 'unixepoch', '-10 months');

-- Create aggregation step for 4 weeks
CREATE TABLE timeseries_4w AS
SELECT
	ts_4w AS ts,
	value
FROM (
	SELECT
		time_bucket(interval('28d'), ts) AS ts_4w,
		avg(value) AS value
	FROM timeseries_1d
	WHERE ts < unixepoch((SELECT max(ts) FROM timeseries_1s), 'unixepoch', '-2 years')
	GROUP BY ts_4w
);

CREATE UNIQUE INDEX idx_timeseries_4w_ts ON timeseries_4w(ts DESC);

DELETE FROM timeseries_1d WHERE ts < unixepoch((SELECT max(ts) FROM timeseries_1s), 'unixepoch', '-2 years');

-- Create timeseries overall view
CREATE VIEW timeseries AS
	SELECT * FROM timeseries_1s
	UNION ALL
	SELECT * FROM timeseries_1m
	UNION ALL
	SELECT * FROM timeseries_15m
	UNION ALL
	SELECT * FROM timeseries_30m
	UNION ALL
	SELECT * FROM timeseries_1h
	UNION ALL
	SELECT * FROM timeseries_1d
	UNION ALL
	SELECT * FROM timeseries_4w;
