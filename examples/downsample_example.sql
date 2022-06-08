-- Creating the aggregate tables with rates of 1 second, 1 minute, 5 minutes, 15 minutes, 1 hour and 1 day.
CREATE TABLE samples_1s  (ts integer PRIMARY KEY, value real NOT NULL);
CREATE TABLE samples_1m  (ts integer PRIMARY KEY, value real NOT NULL);
CREATE TABLE samples_5m  (ts integer PRIMARY KEY, value real NOT NULL);
CREATE TABLE samples_15m (ts integer PRIMARY KEY, value real NOT NULL);
CREATE TABLE samples_1h  (ts integer PRIMARY KEY, value real NOT NULL);
CREATE TABLE samples_1d  (ts integer PRIMARY KEY, value real NOT NULL);


-- Create the complete view
CREATE VIEW samples AS
	SELECT ts, value, '1s' AS resolution FROM samples_1s
	UNION
	SELECT ts, value, '1m' AS resolution FROM samples_1m
	UNION
	SELECT ts, value, '5m' AS resolution FROM samples_5m
	UNION
	SELECT ts, value, '15m' AS resolution FROM samples_15m
	UNION
	SELECT ts, value, '1h' AS resolution FROM samples_1h
	UNION
	SELECT ts, value, '1d' AS resolution FROM samples_1d;


-- Fill the table with the highest rate.
INSERT INTO samples_1s
	WITH gen AS (
		SELECT
			value AS ts,
			abs(random() % 10000) / 100000.0 AS delta
		FROM generate_series(unixepoch('2016-01-01'), unixepoch('2017-01-01'), interval('1s'))
	)
	SELECT
		ts,
		sum(delta) OVER (ORDER BY ts) AS value
	FROM gen;


-- Aggregate and downsample in waterfall flow.
BEGIN;

INSERT INTO samples_1m
	SELECT
		time_bucket(interval('1m'), ts) AS time_bucket,
		avg(value) AS value
	FROM samples_1s
	WHERE ts < unixepoch((SELECT max(ts) FROM samples_1s), 'unixepoch', '-1 month')
	GROUP BY time_bucket;

DELETE FROM samples_1s
	WHERE ts < unixepoch((SELECT max(ts) FROM samples_1s), 'unixepoch', '-1 month');

COMMIT;


BEGIN;

INSERT INTO samples_5m
	SELECT
		time_bucket(interval('5m'), ts) AS time_bucket,
		avg(value) AS value
	FROM samples_1m
	WHERE ts < unixepoch((SELECT max(ts) FROM samples_1m), 'unixepoch', '-3 month')
	GROUP BY time_bucket;

DELETE FROM samples_1m
	WHERE ts < unixepoch((SELECT max(ts) FROM samples_1m), 'unixepoch', '-3 month');

COMMIT;


BEGIN;

INSERT INTO samples_15m
	SELECT
		time_bucket(interval('15m'), ts) AS time_bucket,
		avg(value) AS value
	FROM samples_5m
	WHERE ts < unixepoch((SELECT max(ts) FROM samples_5m), 'unixepoch', '-3 month')
	GROUP BY time_bucket;

DELETE FROM samples_5m
	WHERE ts < unixepoch((SELECT max(ts) FROM samples_5m), 'unixepoch', '-3 month');

COMMIT;


BEGIN;

INSERT INTO samples_1h
	SELECT
		time_bucket(interval('1h'), ts) AS time_bucket,
		avg(value) AS value
	FROM samples_15m
	WHERE ts < unixepoch((SELECT max(ts) FROM samples_15m), 'unixepoch', '-1 year')
	GROUP BY time_bucket;

DELETE FROM samples_15m
	WHERE ts < unixepoch((SELECT max(ts) FROM samples_15m), 'unixepoch', '-1 year');

COMMIT;


BEGIN;

INSERT INTO samples_1d
	SELECT
		time_bucket(interval('1d'), ts) AS time_bucket,
		avg(value) AS value
	FROM samples_1h
	WHERE ts < unixepoch((SELECT max(ts) FROM samples_1h), 'unixepoch', '-1 year')
	GROUP BY time_bucket;

DELETE FROM samples_1h
	WHERE ts < unixepoch((SELECT max(ts) FROM samples_1h), 'unixepoch', '-1 year');

COMMIT;

-- Create the interpolated view
CREATE VIEW samples_all AS
	WITH dense AS (
		SELECT
			value AS ts
		FROM generate_series(
			(SELECT min(ts) FROM samples),
			(SELECT max(ts) FROM samples),
			interval('1s')
		)
	),
	bound_samples AS (
		SELECT
			dense.ts,
			value
		FROM dense
		LEFT JOIN samples ON dense.ts = samples.ts
	),
	locf AS (
		SELECT
			ts,
			value,
			last_known(value) OVER forward AS last_value,
			last_known(iif(value IS NOT NULL, ts, NULL)) OVER forward AS last_ts,
			last_known(value) OVER backward AS next_value,
			last_known(iif(value IS NOT NULL, ts, NULL)) OVER backward AS next_ts
		FROM bound_samples
		WINDOW forward AS (ORDER BY ts), backward AS (ORDER BY ts DESC)
	)
	SELECT
		ts,
		coalesce(
			value,
			lerp(last_ts, last_value, next_ts, next_value, ts)
		) AS value
	FROM locf;
