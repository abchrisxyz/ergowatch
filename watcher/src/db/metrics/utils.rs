use super::heights::Cache as HeightsCache;
use postgres::Transaction;

pub(super) fn refresh_change_summary(
    tx: &mut Transaction,
    hc: &HeightsCache,
    metrics_table: &str,
    columns: &[&str],
) {
    let summary_table = summary_table(metrics_table);

    tx.execute(&format!("truncate table {summary_table};"), &[])
        .unwrap();

    insert_change_summary(tx, &hc, metrics_table, columns);
}

pub(super) fn bootstrap_change_summary(
    tx: &mut Transaction,
    metrics_table: &str,
    columns: &[&str],
) {
    let hc = HeightsCache::load(tx);
    insert_change_summary(tx, &hc, metrics_table, columns);
}

fn summary_table(metrics_table: &str) -> String {
    format!("{metrics_table}_summary")
}

fn insert_change_summary(
    tx: &mut Transaction,
    hc: &HeightsCache,
    metrics_table: &str,
    columns: &[&str],
) {
    let summary_table = summary_table(metrics_table);

    tx.execute(&format!("truncate table {summary_table};"), &[])
        .unwrap();

    for column in columns {
        tx.execute(
            &format!(
                "
                with current as (
                    select {column} as val
                    from {metrics_table}
                    where height = $2
                )
                insert into {summary_table} (
                    label,
                    current,
                    diff_1d,
                    diff_1w,
                    diff_4w,
                    diff_6m,
                    diff_1y
                )
                select $1 as label
                    , cur.val as current
                    , cur.val - (select {column} from {metrics_table} where height = $3) as diff_1d
                    , cur.val - (select {column} from {metrics_table} where height = $4) as diff_1w
                    , cur.val - (select {column} from {metrics_table} where height = $5) as diff_4w
                    , cur.val - (select {column} from {metrics_table} where height = $6) as diff_6m
                    , cur.val - (select {column} from {metrics_table} where height = $7) as diff_1y
                from current cur;"
            ),
            &[&column, &hc.current, &hc.d1, &hc.w1, &hc.w4, &hc.m6, &hc.y1],
        )
        .unwrap();
    }
}
