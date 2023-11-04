create schema exchanges;
comment on schema exchanges is 'CEX addresses and balances';

-------------------------------------------------------------------------------
-- Revision
-------------------------------------------------------------------------------
create table exchanges._rev (
	singleton int primary key default 1,
	rev_major integer not null,
	rev_minor integer not null,
	check(singleton = 1)
);
insert into exchanges._rev (rev_major, rev_minor) values (1, 0);
comment on table exchanges._rev is 'Current schema revision';

create table exchanges.main_addresses (
    
);
-- New Coinex main: 9i51m3reWk99iw8WF6PgxbUT6ZFKhzJ1PmD11vEuGu125hRaKAH
