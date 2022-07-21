BEGIN;
INSERT INTO stock_aggr_1m
SELECT
	time_bucket(interval('1 minute'), ts) AS one_minute,
	avg(value)
FROM stock_samples
WHERE ts < unixepoch((SELECT max(ts) FROM stock_samples), 'unixepoch', '-3 months')
GROUP BY one_minute;

DELETE FROM stock_samples WHERE ts < unixepoch((SELECT max(ts) FROM stock_samples), 'unixepoch', '-3 months');
COMMIT;


BEGIN;
INSERT INTO pulse_aggr
	WITH deltas AS (
		SELECT
			ts,
			coalesce(ts - lag(ts) OVER (ORDER BY ts), 0) AS ts_delta,
			value
		FROM pulse_samples
		ORDER BY ts
	), pulse_groups AS (
		SELECT
			count() FILTER (WHERE ts_delta > 5) OVER (ORDER BY ts) AS group_num,
			ts,
			ts_delta,
			value
		FROM deltas
	), pulse_peaks AS (
		SELECT
			group_num,
			ts,
			max(value) AS value
		FROM pulse_groups
		GROUP BY group_num
	), pulse_timing AS (
		SELECT
			group_num,
			min(ts) AS t0,
			max(ts) - min(ts) AS d2
		FROM pulse_groups
		GROUP BY group_num
	)
	SELECT
		t0,
		pulse_peaks.ts - t0 AS d1,
		pulse_peaks.value AS v1,
		d2
	FROM pulse_timing
	INNER JOIN pulse_peaks ON pulse_timing.group_num = pulse_peaks.group_num
	WHERE t0 < unixepoch((SELECT max(ts) FROM pulse_samples), 'unixepoch', '-7 days');

DELETE FROM pulse_samples WHERE ts <= (SELECT max(t0) + d2 FROM pulse_aggr);
COMMIT;
