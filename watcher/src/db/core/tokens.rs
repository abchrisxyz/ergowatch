use super::BlockData;
use crate::parsing::Asset;
use crate::parsing::Output;
use log::warn;
use postgres::Transaction;

struct Token<'a> {
    pub token_id: &'a str,
    pub box_id: &'a str,
    pub emission_amount: i64,
    pub eip4_data: Option<EIP4Data>,
}

struct EIP4Data {
    name: String,
    description: String,
    decimals: i32,
}

pub(super) fn include(tx: &mut Transaction, block: &BlockData) {
    let sql_generic = "
        insert into core.tokens (id, box_id, emission_amount)
        values ($1, $2, $3);";

    let sql_eip4 = "
        insert into core.tokens (id, box_id, emission_amount, name, description, decimals, standard)
        values ($1, $2, $3, $4, $5, $6, 'EIP-004');";

    for tkn in extract_new_tokens(block) {
        match tkn.eip4_data {
            Some(eip4_data) => {
                tx.execute(
                    sql_eip4,
                    &[
                        &tkn.token_id,
                        &tkn.box_id,
                        &tkn.emission_amount,
                        &eip4_data.name,
                        &eip4_data.description,
                        &eip4_data.decimals,
                    ],
                )
                .unwrap();
            }
            None => {
                tx.execute(
                    sql_generic,
                    &[&tkn.token_id, &tkn.box_id, &tkn.emission_amount],
                )
                .unwrap();
            }
        }
    }
}

pub(super) fn set_constraints(tx: &mut Transaction) {
    let statements = vec![
        "alter table core.tokens add primary key (id, box_id);",
        "alter table core.tokens alter column id set not null;",
        "alter table core.tokens alter column box_id set not null;",
        "alter table core.tokens alter column emission_amount set not null;",
        "alter table core.tokens	add foreign key (box_id)
            references core.outputs (box_id) on delete cascade;",
        "alter table core.tokens add check (emission_amount > 0);",
    ];

    for statement in statements {
        tx.execute(statement, &[]).unwrap();
    }
}

// Handle newly minted tokes.
// New tokens have the same id as the first input box of the tx.
// New tokens can be minted into more than one asset record within the same box.
// New tokens can be minted into more than one output box.
// EIP-4 asset standard: https://github.com/ergoplatform/eips/blob/master/eip-0004.md
fn extract_new_tokens<'a>(block: &'a BlockData) -> Vec<Token<'a>> {
    block
        .transactions
        .iter()
        .flat_map(|tx| {
            tx.outputs.iter().flat_map(|op| {
                op.assets
                    .iter()
                    .filter(|a| a.token_id == tx.input_box_ids[0])
                    .map(|a| Asset {
                        token_id: a.token_id,
                        amount: a.amount,
                    })
                    .reduce(|a, b| a + b)
                    .map(|a| Token {
                        token_id: &a.token_id,
                        box_id: &op.box_id,
                        emission_amount: a.amount,
                        eip4_data: EIP4Data::from_output(op),
                    })
            })
        })
        .collect()
}

impl EIP4Data {
    fn from_output<'a, 'b: 'a>(output: &'a Output) -> Option<EIP4Data> {
        if output.r4().is_none() || output.r5().is_none() || output.r6().is_none() {
            return None;
        }
        let r4 = output.r4().as_ref().unwrap();
        let r5 = output.r5().as_ref().unwrap();
        let r6 = output.r6().as_ref().unwrap();

        if r4.stype != "Coll[SByte]" || r5.stype != "Coll[SByte]" || r6.stype != "Coll[SByte]" {
            return None;
        }

        let decimals = match parse_eip4_register(&r6.rendered_value)
            .unwrap()
            .parse::<i32>()
        {
            Ok(i32) => i32,
            Err(error) => {
                warn!(
                    "Invalid integer literal in R6 of possible EIP4 transaction: {}. Box ID: {}",
                    &r6.rendered_value, &output.box_id
                );
                log::warn!("{}", error);
                return None;
            }
        };

        Some(EIP4Data {
            name: parse_eip4_register(&r4.rendered_value).unwrap(),
            description: parse_eip4_register(&r5.rendered_value).unwrap(),
            decimals: decimals,
        })
    }
}

fn parse_eip4_register(base16_str: &str) -> anyhow::Result<String> {
    let bytes = base16::decode(base16_str.as_bytes()).unwrap();
    anyhow::Ok(String::from_utf8(bytes)?)
}

#[cfg(test)]
mod tests {
    use super::extract_new_tokens;
    use crate::parsing::testing::block_600k;
    use crate::parsing::testing::block_minting_tokens;
    use crate::parsing::testing::block_multi_asset_mint;
    use pretty_assertions::assert_eq;

    #[test]
    fn block_without_minting_transactions() -> () {
        let block = block_600k();
        let tokens = extract_new_tokens(&block);
        assert_eq!(tokens.len(), 0);
    }

    #[test]
    fn block_with_miniting_transactions() {
        let block = block_minting_tokens();
        let tokens = extract_new_tokens(&block);
        assert_eq!(tokens.len(), 2);

        // EIP-4
        let tkn = &tokens[0];
        assert_eq!(
            tkn.token_id,
            "34d14f73cc1d5342fb06bc1185bd1335e8119c90b1795117e2874ca6ca8dd2c5"
        );
        assert_eq!(
            tkn.box_id,
            "5410f440002d0f350781463633ff6be869c54149cebeaeb935eb2968918e846b"
        );
        assert_eq!(tkn.emission_amount, 5000);
        assert_eq!(tkn.eip4_data.is_some(), true);
        if let Some(data) = &tkn.eip4_data {
            assert_eq!(data.name, "best");
            assert_eq!(data.description, "test ");
            assert_eq!(data.decimals, 1);
        }

        // Generic
        let tkn = &tokens[1];
        assert_eq!(
            tkn.token_id,
            "3c65b325ebf58f4907d6c085d216e176d105a5093540704baf1f7a2a42ad60f8"
        );
        assert_eq!(
            tkn.box_id,
            "48461e901b2a518d66b8d147a5282119cfc5b065a3ebba6a56b354686686a48c"
        );
        assert_eq!(tkn.emission_amount, 1000);
        assert_eq!(tkn.eip4_data.is_none(), true);
    }

    #[test]
    fn multi_asset_mint() {
        let block = block_multi_asset_mint();
        let tokens = extract_new_tokens(&block);
        assert_eq!(tokens.len(), 1);
        // Emission amount should be total of minting boxes
        assert_eq!(tokens[0].emission_amount, 10 + 10);
    }
}
