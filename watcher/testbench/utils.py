import os
from pathlib import Path
import subprocess
import psycopg as pg
from typing import List
import pytest


def run_watcher(
    cfg_path: Path,
    target="release",
    backtrace=False,
    timeout=15,
    log_file: str = None,
    allow_migrations: bool = False,
) -> subprocess.CompletedProcess:
    exe = str(
        Path(__file__).parent.parent.absolute() / Path(f"target/{target}/watcher")
    )
    args = [exe, "-c", cfg_path]
    args.append("--exit")
    if allow_migrations:
        args.append("-m")

    env = dict(
        os.environ,
        EW_LOG="INFO",
    )
    if backtrace:
        env["RUST_BACKTRACE"] = "full"

    print(args)
    if log_file is None:
        cp = subprocess.run(
            args,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            env=env,
            timeout=timeout,
        )
        print(cp.stdout.decode())
    else:
        with open(log_file, "w") as f:
            cp = subprocess.run(
                args,
                stdout=f,
                stderr=subprocess.STDOUT,
                env=env,
                timeout=timeout,
            )
    return cp


def extract_db_conn_str(db_conn) -> str:
    db_info = db_conn.info
    return f"host={db_info.host} port={db_info.port} dbname={db_info.dbname} user={db_info.user} password={db_info.password}"


def table_has_pk(conn: pg.Connection, schema: str, table: str):
    """Return true if a primary key is set for specified table"""
    # https://wiki.postgresql.org/wiki/Retrieve_primary_key_columns
    with conn.cursor() as cur:
        cur.execute(
            f"""
            select exists(
                select *
                from pg_index i
                join pg_attribute a on a.attrelid = i.indrelid
                    and a.attnum = any(i.indkey)
                where i.indrelid = '{schema}.{table}'::regclass
                and i.indisprimary
            );
        """
        )
        return cur.fetchone()[0]


def assert_pk(conn: pg.Connection, schema: str, table: str, pk_columns: List[str]):
    """Assert primary key is set as specified"""
    # https://wiki.postgresql.org/wiki/Retrieve_primary_key_columns
    with conn.cursor() as cur:
        cur.execute(
            f"""
            select a.attname
                , format_type(a.atttypid, a.atttypmod) as data_type
            from pg_index i
            join pg_attribute a on a.attrelid = i.indrelid
                and a.attnum = any(i.indkey)
            where i.indrelid = '{schema}.{table}'::regclass
            and i.indisprimary;
        """
        )
        assert [r[0] for r in cur.fetchall()] == pk_columns


def assert_fk(conn: pg.Connection, schema: str, table: str, constraint_name: str):
    """Assert foreign key exists"""
    with conn.cursor() as cur:
        cur.execute(
            f"""
            select exists (
                select *
                from information_schema.table_constraints
                where table_schema = '{schema}'
                    and table_name = '{table}'
                    and constraint_type = 'FOREIGN KEY'
                    and constraint_name = '{constraint_name}'
            );
        """
        )
        assert cur.fetchone()[0]


def assert_unique(conn: pg.Connection, schema: str, table: str, columns: List[str]):
    """Assert foreign key exists"""
    constraint_name = f"{table}_unique_{'_'.join(columns)}"
    with conn.cursor() as cur:
        cur.execute(
            f"""
            select exists (
                select *
                from information_schema.table_constraints
                where table_schema = '{schema}'
                    and table_name = '{table}'
                    and constraint_type = 'UNIQUE'
                    and constraint_name = '{constraint_name}'
            );
        """
        )
        assert cur.fetchone()[0]


def assert_column_not_null(conn: pg.Connection, schema: str, table: str, column: str):
    """Assert not null constraint is set on column"""
    with conn.cursor() as cur:
        cur.execute(
            f"""
            select exists (
                select column_name
                from information_schema.columns
                where table_schema = '{schema}'
                    and table_name = '{table}'
                    and column_name = '{column}'
                    and is_nullable = 'NO'
            );
        """
        )
        assert cur.fetchone()[0]


def assert_column_ge(
    conn: pg.Connection, schema: str, table: str, column: str, value: float
):
    """Assert check(column >= x) constraint is set"""
    with conn.cursor() as cur:
        with pytest.raises(pg.errors.CheckViolation):
            cur.execute(f"update {schema}.{table} set {column} = {value - 1};")
        # Roll back to prevent psycopg.errors.InFailedSqlTransaction in subsequent call
        conn.rollback()


def assert_column_le(
    conn: pg.Connection, schema: str, table: str, column: str, value: float
):
    """Assert check(column <= x) constraint is set"""
    with conn.cursor() as cur:
        with pytest.raises(pg.errors.CheckViolation):
            cur.execute(f"update {schema}.{table} set {column} = {value + 1};")
        # Roll back to prevent psycopg.errors.InFailedSqlTransaction in subsequent call
        conn.rollback()


def assert_index(conn: pg.Connection, schema: str, table: str, index_name: str):
    """Assert index exists for table"""
    with conn.cursor() as cur:
        cur.execute(
            f"""
            select exists(
                select
                    t.relname as table_name,
                    i.relname as index_name,
                    a.attname as column_name
                from
                    pg_class t,
                    pg_class i,
                    pg_index ix,
                    pg_attribute a
                where
                    t.oid = ix.indrelid
                    and i.oid = ix.indexrelid
                    and a.attrelid = t.oid
                    and a.attnum = ANY(ix.indkey)
                    and t.relkind = 'r'
                    and ix.indrelid = '{schema}.{table}'::regclass
                    and i.relname = '{index_name}'
                order by
                    t.relname,
                    i.relname
            );
        """
        )
        assert cur.fetchone()[0]
