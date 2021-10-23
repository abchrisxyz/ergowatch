/*
	psql input file to load schema's right order to satisfy dependencies.

	Example:

		psql -U ergo -d ergo --single-transaction -f _init.sql
*/
\i './oracle-pools.sql'
\i './sigmausd.sql'
\i './sync.sql'