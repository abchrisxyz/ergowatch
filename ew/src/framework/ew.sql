-- Internal management schema

create schema ew;

-- Revision of each schema/worker subset
create table ew.revisions (
    schema_name text not null,
    worker_id text not null,
    major integer not null,
    minor integer not null,
    primary key (schema_name, worker_id)
);

-- Last processed block for each schema/worker
create table ew.headers (
    schema_name text not null,
    worker_id text not null,
    height integer not null,
    timestamp bigint not null,
    header_id text not null,
    parent_id text not null,
    primary key (schema_name, worker_id)
);
