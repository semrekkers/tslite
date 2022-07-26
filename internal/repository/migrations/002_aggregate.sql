CREATE TABLE value_aggr_1m (
	ts int PRIMARY KEY NOT NULL,
	value real NOT NULL
);

BEGIN;
INSERT INTO value_aggr_1m
SELECT
	time_bucket(interval('1 minute'), ts) AS one_minute,
	avg(value)
FROM value_samples
WHERE ts < unixepoch((SELECT max(ts) FROM value_samples), 'unixepoch', '-3 months')
GROUP BY one_minute;

DELETE FROM value_samples WHERE ts < unixepoch((SELECT max(ts) FROM value_samples), 'unixepoch', '-3 months');
COMMIT;

CREATE VIEW value_all AS
	SELECT ts, value FROM value_samples
	UNION ALL
	SELECT ts, value FROM value_aggr_1m;

ANALYZE;
