use crate::core::node;
use ergotree_ir::chain::ergo_box::ErgoBox;
use ergotree_ir::serialization::SigmaSerializable;
use tracing::warn;

/// Calculate box size in bytes.
///
/// Node boxes are not guaranteed to be valid,
/// see https://github.com/ergoplatform/sigma-rust/issues/587.
pub fn calc_box_size(output: &node::models::Output) -> Option<i32> {
    let json = format!(
        "
        {{
            \"boxId\": \"{}\",
            \"value\": {},
            \"ergoTree\": \"{}\",
            \"assets\": [{}],
            \"creationHeight\": {},
            \"additionalRegisters\": {},
            \"transactionId\": \"{}\",
            \"index\": {}
        }}",
        output.box_id,
        output.value,
        output.ergo_tree,
        format_assets(&output.assets),
        output.creation_height,
        output.additional_registers,
        output.transaction_id,
        output.index
    );
    let ergo_box: Result<ErgoBox, serde_json::error::Error> = serde_json::from_str(&json);
    match ergo_box {
        Ok(eb) => Some(eb.sigma_serialize_bytes().unwrap().len() as i32),
        Err(e) => {
            warn!("Encountered undeserializable box {}", output.box_id);
            warn!("{:?}", e);
            None
        }
    }
}

fn format_assets(assets: &Vec<node::models::Asset>) -> String {
    assets
        .iter()
        .map(|a| {
            format!(
                "{{\"tokenId\": \"{}\", \"amount\": {} }}",
                a.token_id, a.amount
            )
        })
        .collect::<Vec<String>>()
        .join(",")
}
