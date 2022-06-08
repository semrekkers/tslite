# tslite

This extension module for SQLite provides extra functionality for timeseries data. 

Four functionalities are yet implemented:

- `interval(text interval)`
  Parses an interval to interval seconds. Example:
  - `1s` to 1 second
  - `1m` to 60 seconds
  - `1h` to 3600 seconds
  - `1d` to 86400 seconds
- `time_bucket(bucket_width int, timestamp int)`
  Puts timestamp in the corresponding bucket denoted by the bucket_width. Example:
  - `time_bucket(interval('15m'), unixepoch('2022-03-04 11:23:43'))` output is _2022-03-04 11:15:00_ in UNIX epoch.
- `lerp(timestamp a, value a, timestamp b, value b, timestamp t)` Calculate the intermediate value at timestamp _T_.
- `last_known(any value)` (window aggregation) Remebers the last known value (that is excluding NULLs)

### Examples

See the `examples` directory.
