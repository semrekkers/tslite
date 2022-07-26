INSERT INTO value_samples
	WITH generate AS (
		SELECT	-- Genesis
			unixepoch('2000-01-01') AS ts,
			1000 AS delta
		UNION ALL
		SELECT
			value + (random() % interval('4 seconds')) AS ts,
			(random() % 100) / 1000.0 AS delta
		FROM generate_series(
			unixepoch('2005-01-01'),    -- start
			unixepoch('2017-12-31'),    -- end
			interval('7 seconds'))      -- interval (approximately)
	)
	SELECT
		ts,
		sum(delta) OVER (ORDER BY ts) AS value
	FROM generate
	WHERE true  -- parsing ambiguity
	ON CONFLICT DO NOTHING;

INSERT INTO gauge_samples
	SELECT
		value + (random() % interval('2 seconds')) AS ts,
		(abs(random()) % 250) / 10.0 AS value
	FROM generate_series(
		unixepoch('2005-01-01'),    -- start
		unixepoch('2017-12-31'),    -- end
		interval('20 seconds'))     -- interval (approximately)
	WHERE true
	ON CONFLICT DO NOTHING;

INSERT INTO switch_samples
	WITH generate AS (
		SELECT
			value + (random() % interval('1 minute')) AS ts
		FROM generate_series(
			unixepoch('2005-01-01'),    -- start
			unixepoch('2017-12-31'),    -- end
			interval('40 seconds'))     -- interval (approximately)
	)
	SELECT
		ts + switch.column1 AS ts,
		switch.column2 AS value
	FROM generate
	FULL JOIN (
		VALUES
			(0, 1.0),
			(1, 1.0),
			(2, 1.0),
			(3, 1.0),
			(4, 0.0)
	) switch
	WHERE true
	ON CONFLICT DO NOTHING;

INSERT INTO activity_samples
	WITH generate AS (
		SELECT
			value + (random() % interval('5 seconds')) AS ts
		FROM generate_series(
			unixepoch('2005-01-01'),    -- start
			unixepoch('2017-12-31'),    -- end
			interval('9 seconds'))     -- interval (approximately)
	)
	SELECT
		ts + activity.column1 AS ts,
		1.0 AS value
	FROM generate
	FULL JOIN (
		VALUES
			(0),
			(1),
			(2),
			(3),
			(4)
	) activity
	WHERE true
	ON CONFLICT DO NOTHING;

INSERT INTO pulse_samples
	WITH generate AS (
		SELECT
			value + (random() % interval('20 seconds')) AS ts
		FROM generate_series(
			unixepoch('2005-01-01'),    -- start
			unixepoch('2017-12-31'),    -- end
			interval('40 seconds'))     -- interval (approximately)
	)
	SELECT
		ts + pulse.column1 AS ts,
		coalesce(
			(pulse.column2 + (abs(random()) % 60)) / 10.0,
			pulse.column3
		) AS value
	FROM generate
	FULL JOIN (
		VALUES
			(interval('0s'), NULL, 0.0),
			(interval('1s'), 60, NULL),
			(interval('2s'), 60, NULL),
			(interval('3s'), 0, NULL),
			(interval('4s'), NULL, 0.0)
	) pulse
	WHERE true
	ON CONFLICT DO NOTHING;
