/// Migration 11
///
/// Add cex text id's
use postgres::Transaction;

pub(super) fn apply(tx: &mut Transaction) -> anyhow::Result<()> {
    let statements = vec![
        "alter table cex.cexs add column text_id text;",
        "update cex.cexs set text_id = 'coinex' where id = 1;",
        "update cex.cexs set text_id = 'gate' where id = 2;",
        "update cex.cexs set text_id = 'kucoin' where id = 3;",
        "update cex.cexs set text_id = 'probit' where id = 4;",
        "alter table cex.cexs add constraint cexs_unique_text_ids unique (text_id);",
        "alter table cex.cexs alter column text_id set not null;",
    ];

    for statement in statements {
        tx.execute(statement, &[])?;
    }

    Ok(())
}
