/* ---------- Samples ---------- */

CREATE TABLE value_samples (
	ts int PRIMARY KEY NOT NULL,
	value real NOT NULL
);

CREATE TABLE gauge_samples (
	ts int PRIMARY KEY NOT NULL,
	value real NOT NULL
);

CREATE TABLE switch_samples (
	ts int PRIMARY KEY NOT NULL,
	value real NOT NULL
);

CREATE TABLE activity_samples (
	ts int PRIMARY KEY NOT NULL,
	value real NOT NULL
);

/* ---------- Aggregations ---------- */

/*
CREATE TABLE stock_aggr_1m (
	ts int PRIMARY KEY NOT NULL,
	value real NOT NULL
);

CREATE TABLE pulse_aggr (
	t0 int PRIMARY KEY NOT NULL,
	-- v0 is always 0.0
	d1 int NOT NULL,
	v1 real NOT NULL,
	d2 int NOT NULL
	-- v2 is always 0.0
);

CREATE TABLE pulse_aggr_asr (
	t0 int PRIMARY KEY NOT NULL,
	-- v0 is always 0.0
	d1 int NOT NULL,			-- end of attack, start of sustain
	v1 real NOT NULL,			-- sustain value
	d2 int NOT NULL,			-- end of sustain, start of decay
	-- v2 is always equals v1
	d3 int NOT NULL				-- end of decay
	-- v3 is always 0.0
);
*/
