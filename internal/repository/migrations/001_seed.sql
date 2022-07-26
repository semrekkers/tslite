INSERT INTO value_samples
	WITH generate AS (
		SELECT	-- Genesis
			unixepoch('2018-12-01') AS ts,
			1000 AS delta
		UNION ALL
		SELECT
			value + (random() % interval('4 seconds')) AS ts,
			(random() % 100) / 1000.0 AS delta
		FROM generate_series(
			unixepoch('2019-01-01'),    -- start
			unixepoch('2022-07-01'),    -- end
			interval('7 seconds'))      -- interval (approximately)
	)
	SELECT
		ts,
		sum(delta) OVER (ORDER BY ts) AS value
	FROM generate
	WHERE true  -- parsing ambiguity
	ON CONFLICT DO NOTHING;
