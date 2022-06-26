use ergo_chain_types::Digest32;
use ergotree_ir::chain::address::Address;
use ergotree_ir::chain::address::AddressEncoder;
use ergotree_ir::chain::address::NetworkPrefix;
use ergotree_ir::chain::ergo_box::ErgoBox;
use ergotree_ir::chain::ergo_box::ErgoBoxCandidate;
use ergotree_ir::chain::tx_id::TxId;
use ergotree_ir::ergo_tree::ErgoTree;
use ergotree_ir::serialization::SigmaSerializable;

use pyo3::prelude::*;

/// The sigpy module
#[pymodule]
fn sigpy(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(base16_to_address, m)?)?;
    m.add_function(wrap_pyfunction!(calc_box_id, m)?)?;
    Ok(())
}

/// Converts a base16 encoded ergo tree string into an address string
#[pyfunction]
fn base16_to_address(base16_str: &str) -> PyResult<String> {
    let tree_bytes = base16::decode(base16_str.as_bytes()).unwrap();
    let tree = ErgoTree::sigma_parse_bytes(&tree_bytes).unwrap();
    let address = address_from_ergo_tree(&tree);
    Ok(address)
}

/// Get the address string from an ErgoTree
fn address_from_ergo_tree(tree: &ErgoTree) -> String {
    let address = Address::recreate_from_ergo_tree(tree).unwrap();
    let encoder = AddressEncoder::new(NetworkPrefix::Mainnet);
    encoder.address_to_str(&address)
}

/// Calculate a box id from a box candidate, tx id and index
#[pyfunction]
fn calc_box_id(serialized_candidate: &str, tx_id: &str, index: u16) -> PyResult<String> {
    let ebc: ErgoBoxCandidate = serde_json::from_str(serialized_candidate).unwrap();
    let eb = ErgoBox::from_box_candidate(&ebc, txid_from_str(tx_id), index).unwrap();
    Ok(String::from(eb.box_id()))
}

/// Convert a string to a TxId
fn txid_from_str(tx_id: &str) -> TxId {
    TxId(Digest32::try_from(String::from(tx_id)).unwrap())
}
