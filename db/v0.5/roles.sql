--  Read only role
create role ew_reader nologin;
grant usage on schema adr, blk, cex, cgo, core, ew, mtr, usp to ew_reader;
grant select on all tables in schema adr, blk, cex, cgo, core, ew, mtr, usp to ew_reader;

-- Prevent read only roles from creating tables in public schema
revoke all on schema public from public;
