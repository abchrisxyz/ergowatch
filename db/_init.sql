/*
	psql input file to load schema's in right order to satisfy dependencies.

	Example:

		psql -U ergo -d ergo --single-transaction -f _init.sql
*/
\i './oracle-pools.sql'
\i './sigmausd.sql'
\i './coingecko.sql'
-- \i './snapshots.sql'
\i './continuous.sql'
\i './sync.sql'
\i './metrics.sql'
