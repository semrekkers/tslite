WITH deltas AS (
	SELECT
		ts,
		ts - lag(ts) OVER (ORDER BY ts) AS ts_delta,
		value
	FROM pulse_samples
), groups AS (
	SELECT
		count() FILTER (WHERE ts_delta <= 5) OVER (ORDER BY ts),
		ts,
		value
	FROM deltas
)
SELECT * FROM groups
LIMIT 100

SELECT
	ts,
	lag(ts) OVER (ORDER BY ts DESC) AS ts_delta,
	value
FROM pulse_samples
ORDER BY ts DESC


SELECT
	ts,
	lag(ts) OVER (ORDER BY ts DESC) - ts AS ts_delta,
	value
FROM (
SELECT
	ts,
	value
FROM pulse_samples
ORDER BY ts DESC
LIMIT 1000
)


SELECT
	ts,
	ts - lag(ts) OVER (ORDER BY ts) AS ts_delta,
	value
FROM pulse_samples
ORDER BY ts DESC
LIMIT 100





WITH deltas AS (
	SELECT
		ts,
		coalesce(lag(ts) OVER (ORDER BY ts DESC) - ts, 0) AS ts_delta,
		value
	FROM pulse_samples
	ORDER BY ts DESC
	LIMIT 120
), pulse_groups AS (
	SELECT
		count() FILTER (WHERE ts_delta > 5) OVER (ORDER BY ts DESC) AS group_num,
		ts,
		ts_delta,
		value
	FROM deltas
)
SELECT
	*
FROM pulse_groups;



WITH deltas AS (
	SELECT
		ts,
		coalesce(ts - lag(ts) OVER (ORDER BY ts), 0) AS ts_delta,
		value
	FROM pulse_samples
	ORDER BY ts
	LIMIT 120
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
	0.0 AS v0,
	pulse_peaks.ts - t0 AS d1,
	pulse_peaks.value AS v1,
	d2,
	0.0 AS v2,
	d2 + 1 AS duration
FROM pulse_timing
INNER JOIN pulse_peaks ON pulse_timing.group_num = pulse_peaks.group_num




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
	INNER JOIN pulse_peaks ON pulse_timing.group_num = pulse_peaks.group_num;


WITH samples AS (
	SELECT
		column1 AS t,
		column2 AS value
	FROM (
		VALUES
			(1, 0.1),
			(2, 0.1),
			(3, 0.1),
			(4, 0.1),
			(5, 0.1),
			(6, 0.2),
			(7, 0.2),
			(8, 0.4),
			(9, 0.6),
			(10, 0.9),
			(11, 1.0),
			(12, 1.1),
			(13, 1.0),
			(14, 0.9),
			(15, 1.1),
			(16, 1.0),
			(17, 1.0),
			(18, 1.3),
			(19, 1.0),
			(20, 0.9),
			(21, 0.9),
			(22, 0.9),
			(23, 0.9),
			(24, 0.9),
			(25, 0.9),
			(26, 0.9),
			(27, 0.9),
			(28, 0.9),
			(29, 0.8),
			(30, 1.0),
			(31, 1.1),
			(32, 1.0),
			(33, 1.0),
			(34, 1.0),
			(35, 0.9),
			(36, 0.7),
			(37, 0.3),
			(38, 0.2),
			(39, 0.2),
			(40, 0.1),
			(41, 0.0),
			(42, 0.0)
	)
	ORDER BY t
), median AS (
	SELECT
		t,
		value,
		(

		)
)






SELECT
	column2
FROM samples
ORDER BY column2
LIMIT 1
OFFSET (
	SELECT count(*) FROM samples
) / 2



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



CREATE TABLE mat_pulse_samples (
	ts int PRIMARY KEY NOT NULL,
	group_num int NOT NULL,
	value real NOT NULL
);

INSERT INTO mat_pulse_samples
	WITH deltas AS (
		SELECT
			ts,
			coalesce(ts - lag(ts) OVER (ORDER BY ts), 0) AS ts_delta,
			value
		FROM pulse_samples
		ORDER BY ts
	)
	SELECT
		ts,
		count() FILTER (WHERE ts_delta > 5) OVER (ORDER BY ts) AS group_num,
		value
	FROM deltas;

CREATE INDEX mat_pulse_samples_group_idx ON mat_pulse_samples(group_num, ts);

SELECT
	count() OVER (PARTITION BY group_num)
FROM mat_pulse_samples
LIMIT 100

WITH group_median(group_nr) AS (
	SELECT
		value
	FROM mat_pulse_samples
	WHERE group_num = group_nr
	ORDER BY ts
	LIMIT 1
	OFFSET (
		SELECT count(*) FROM mat_pulse_samples
		WHERE group_num = group_nr
	) / 2
)
SELECT
	ts,
	group_num,
	value,

SELECT
	value
FROM mat_pulse_samples
WHERE group_num = 0
ORDER BY ts
LIMIT 1
OFFSET (
	SELECT count(*) FROM mat_pulse_samples
	WHERE group_num = 0
) / 2


WITH group_median(groupnr) AS (
	SELECT
		value
	FROM mat_pulse_samples
	WHERE group_num = groupnr
	ORDER BY ts
	LIMIT 1
	OFFSET (
		SELECT count(*) FROM mat_pulse_samples
		WHERE group_num = groupnr
	) / 2
)
SELECT
	group_median.value
FROM mat_pulse_samples
INNER JOIN group_median ON group_median.groupnr = mat_pulse_samples.group_num
LIMIT 10



WITH group_median AS (
	SELECT
		group_num,
		count(*) / 2
	FROM mat_pulse_samples
	GROUP BY group_num
)
SELECT * FROM group_median








-- Trapezoid setup

WITH deltas AS (
	SELECT
		ts,
		coalesce(ts - lag(ts) OVER (ORDER BY ts), 0) AS ts_delta,
		value
	FROM pulse_samples
	ORDER BY ts
	LIMIT 150
), pulse_groups AS (
	SELECT
		count() FILTER (WHERE ts_delta > 5) OVER (ORDER BY ts) AS group_num,
		ts,
		ts_delta,
		value
	FROM deltas
)
SELECT * FROM pulse_groups

CREATE TABLE mat_pulse_groups AS
	WITH deltas AS (
		SELECT
			ts,
			coalesce(ts - lag(ts) OVER (ORDER BY ts), 0) AS ts_delta,
			value
		FROM pulse_samples
		ORDER BY ts
	)
	SELECT
		count() FILTER (WHERE ts_delta > 5) OVER (ORDER BY ts) AS group_nr,
		ts,
		ts_delta,
		value
	FROM deltas

CREATE INDEX mat_pulse_groups_group_nr ON mat_pulse_groups(group_nr)

CREATE TABLE pulse_zoids (
	ts int PRIMARY KEY NOT NULL,
	a_offset int NOT NULL,
	a_value real NOT NULL,
	b_offset int NOT NULL,
	b_value real NOT NULL
);





CREATE TABLE pulse_aggr_1m (
	ts int PRIMARY KEY NOT NULL,
	value real NOT NULL
)

INSERT INTO pulse_aggr_1m
	SELECT
		ts,
		value
	FROM pulse_samples
	WHERE
		ts < unixepoch((SELECT max(ts) FROM pulse_samples), 'unixepoch', '-1 month')
		AND value < 0.3
