use ergotree_ir::chain::address::Address;
use ergotree_ir::chain::address::AddressEncoder;
use ergotree_ir::chain::address::NetworkPrefix;
use ergotree_ir::ergo_tree::ErgoTree;

pub(super) fn address_from_ergo_tree(tree: &ErgoTree) -> String {
    let address = Address::recreate_from_ergo_tree(tree).unwrap();
    let encoder = AddressEncoder::new(NetworkPrefix::Mainnet);
    encoder.address_to_str(&address)
}

#[cfg(test)]
mod tests {
    use super::address_from_ergo_tree;
    use ergotree_ir::ergo_tree::ErgoTree;
    use ergotree_ir::serialization::SigmaSerializable;
    use pretty_assertions::assert_eq;

    #[test]
    fn check_address_from_ergo_tree() {
        let base16_str = "0008cd03553448c194fdd843c87d080f5e8ed983f5bb2807b13b45a9683bba8c7bfb5ae8";
        let tree_bytes = base16::decode(base16_str.as_bytes()).unwrap();
        let tree = ErgoTree::sigma_parse_bytes(&tree_bytes).unwrap();
        assert_eq!(
            address_from_ergo_tree(&tree),
            "9h7L7sUHZk43VQC3PHtSp5ujAWcZtYmWATBH746wi75C5XHi68b"
        );
    }
}
