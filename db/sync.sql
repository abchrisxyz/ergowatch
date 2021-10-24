/*
	Main ergowatch schema.

	Takes care of notifying new blocks and defines top-level sync routine.
*/
drop schema if exists ew cascade;
create schema ew;


create table ew.sync_status (
	last_sync_height integer
);
insert into ew.sync_status (last_sync_height) values (0);


create function ew.notify_new_header() returns trigger as
	$$
	begin
	perform pg_notify('ergowatch', (select height::text from new_table));
	return null;
	end;
	$$ language plpgsql;
	
create trigger notify_node_headers_insert
    after insert on node_headers
    referencing new table as new_table
    for each statement
    execute function ew.notify_new_header();


/*
	Main sync routine.

	Groups calls to other schema's sync routines.
*/
-- drop procedure if exists ew.sync;
create procedure ew.sync(in _height integer) as
	$$
	
	-- Oracle Pools
	call orp.sync(_height);

	-- SigmaUSD (relies on ERG/USD oracle pool)
	call sig.sync(_height);

	-- Age
	call age.sync();

	-- Distribution
	call dis.sync();

	update ew.sync_status
	set last_sync_height = _height;

	$$
	language sql;
