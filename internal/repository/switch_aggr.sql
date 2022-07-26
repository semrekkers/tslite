WITH value_deltas AS (
		SELECT
			ts,
			coalesce(ts - lag(ts) OVER (ORDER BY ts), 0) AS ts_delta,
			value,

		FROM pulse_samples
		ORDER BY ts


WITH deltas AS (
	SELECT
		ts,
		value,
		coalesce(ts - lag(ts) OVER win, 0) AS ts_delta,
		coalesce(abs(value - lag(value) OVER win), 0.0) AS value_delta
	FROM switch_samples
	WINDOW win AS (ORDER BY ts)
	ORDER BY ts
	LIMIT 150
), effect_groups AS (
	SELECT
		count() FILTER (WHERE ts_delta > 5 OR value_delta > 0.5) OVER (ORDER BY ts) AS group_nr,
		ts,
		value
	FROM deltas
), group_bounds AS (
	SELECT
		min(ts) AS ts,
		(max(ts) - min(ts)) + 1 AS duration
	FROM effect_groups
	WHERE value > 0.5
	GROUP BY group_nr
)
SELECT * FROM group_bounds




CREATE TABLE switch_aggr (
	ts int PRIMARY KEY NOT NULL,
	duration int NOT NULL
);




INSERT INTO switch_aggr
	WITH deltas AS (
		SELECT
			ts,
			value,
			coalesce(ts - lag(ts) OVER win, 0) AS ts_delta,
			coalesce(abs(value - lag(value) OVER win), 0.0) AS value_delta
		FROM switch_samples
		WINDOW win AS (ORDER BY ts)
		ORDER BY ts
	), effect_groups AS (
		SELECT
			count() FILTER (WHERE ts_delta > 5 OR value_delta > 0.5) OVER (ORDER BY ts) AS group_nr,
			ts,
			value
		FROM deltas
	)
	SELECT
		min(ts) AS ts,
		(max(ts) - min(ts)) + 1 AS duration
	FROM effect_groups
	WHERE value > 0.5
	GROUP BY group_nr




WITH deltas AS (
	SELECT
		ts,
		value,
		abs(value - lag(value, 1, 0.0) OVER (ORDER BY ts)) AS value_delta
	FROM switch_samples
	ORDER BY ts
	LIMIT 150
), periods AS (
	SELECT
		count() FILTER (WHERE value_delta > 0.5 AND value > 0.5) OVER (ORDER BY ts) AS period_nr,
		ts,
		value,
		value_delta
	FROM deltas
), aggr AS (
	SELECT
		min(ts) AS ts,
		1 + max(ts) - min(ts) AS duration
	FROM periods
	WHERE value > 0.0
	GROUP BY period_nr
)
SELECT * FROM aggr;





CREATE TABLE switch_aggr (
	ts int PRIMARY KEY NOT NULL,
	duration int NOT NULL
);

INSERT INTO switch_aggr
	WITH deltas AS (
		SELECT
			ts,
			value,
			abs(value - lag(value, 1, 0.0) OVER (ORDER BY ts)) AS value_delta
		FROM switch_samples
		ORDER BY ts
	), periods AS (
		SELECT
			count() FILTER (WHERE value_delta > 0.5 AND value > 0.5) OVER (ORDER BY ts) AS period_nr,
			ts,
			value,
			value_delta
		FROM deltas
	)
	SELECT
		min(ts) AS ts,
		1 + max(ts) - min(ts) AS duration
	FROM periods
	WHERE value > 0.0
	GROUP BY period_nr;


WITH deltas AS (
	SELECT
		ts,
		ts - lag(ts) OVER (ORDER BY ts) AS ts_delta
	FROM switch_aggr
	ORDER BY ts
), ntiles AS (
	SELECT
		ts_delta,
		ntile(100) OVER (ORDER BY ts_delta) AS ntile
	FROM deltas
)
SELECT
	ntile,
	min(ts_delta) AS min,
	max(ts_delta) AS max,
	count(ts_delta) AS count
FROM ntiles
GROUP BY ntile
ORDER BY ntile



WITH deltas AS (
	SELECT
		ts,
		ts - lag(ts, 1, 0) OVER (ORDER BY ts) AS ts_delta
	FROM switch_aggr
	ORDER BY ts
)

WITH ends AS (
	SELECT
		ts + duration - 1 AS ts
	FROM switch_aggr
	ORDER BY ts
), bounds AS (
	SELECT
		ts
	FROM switch_aggr
	UNION
	SELECT
		ts
	FROM ends
), deltas AS (
	SELECT
		ts,
		ts - lag(ts) OVER (ORDER BY ts) AS ts_delta
	FROM bounds
	ORDER BY ts
)
SELECT * FROM deltas LIMIT 100


WITH bounds AS (
	SELECT
		ts AS start_ts,
		ts + duration - 1 AS end_ts
	FROM switch_aggr
	ORDER BY start_ts
), deltas AS (
	SELECT
		start_ts,
		end_ts,
		start_ts - lag(end_ts) OVER (ORDER BY start_ts) - 1 AS off_duration
	FROM bounds
)
SELECT
	off_duration,
	count(off_duration)
FROM deltas
WHERE off_duration IS NOT NULL
GROUP BY off_duration
ORDER BY off_duration
