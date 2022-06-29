use crate::parsing::BlockData;
use crate::parsing::ExtensionField;
use postgres::Transaction;

struct SystemParameters {
    storage_fee: Option<i32>,        // 1. Storage fee nanoErg/byte
    min_box_value: Option<i32>,      // 2. Minimum box value in nanoErg
    max_block_size: Option<i32>,     // 3. Maximum block size
    max_cost: Option<i32>,           // 4. Maximum computational cost of a block
    token_access_cost: Option<i32>,  // 5. Token access cost
    tx_input_cost: Option<i32>,      // 6. Cost per tx input
    tx_data_input_cost: Option<i32>, // 7. Cost per tx data-input
    tx_output_cost: Option<i32>,     // 8. Cost per tx output
    block_version: Option<i32>,      // 123. Block version
}

pub(super) fn include(tx: &mut Transaction, block: &BlockData) {
    let mut params = SystemParameters {
        storage_fee: None,
        min_box_value: None,
        max_block_size: None,
        max_cost: None,
        token_access_cost: None,
        tx_input_cost: None,
        tx_data_input_cost: None,
        tx_output_cost: None,
        block_version: None,
    };

    for field in &block.extension.fields {
        let key_bytes = base16::decode(field.key.as_bytes()).unwrap();
        match key_bytes[0] {
            //  System parameter
            0 => match key_bytes[1] {
                1 => params.storage_fee = Some(parse_i32_value(field.val)),
                2 => params.min_box_value = Some(parse_i32_value(field.val)),
                3 => params.max_block_size = Some(parse_i32_value(field.val)),
                4 => params.max_cost = Some(parse_i32_value(field.val)),
                5 => params.token_access_cost = Some(parse_i32_value(field.val)),
                6 => params.tx_input_cost = Some(parse_i32_value(field.val)),
                7 => params.tx_data_input_cost = Some(parse_i32_value(field.val)),
                8 => params.tx_output_cost = Some(parse_i32_value(field.val)),
                123 => params.block_version = Some(parse_i32_value(field.val)),
                _ => insert_unhandled_extension_field(tx, block.height, &field),
            },
            // Interlinks vector - skipping
            1 => (),
            // Validation rules - store in case we want to do something with them at some point.
            2 => insert_unhandled_extension_field(tx, block.height, &field),
            // For good measure
            _ => insert_unhandled_extension_field(tx, block.height, &field),
        };
    }

    // Only store params when at least one is defined
    if params.has_data() {
        insert_system_parameters(tx, block.height, params);
    }
}

pub(super) fn rollback(tx: &mut Transaction, block: &BlockData) {
    tx.execute(
        "delete from core.system_parameters where height = $1;",
        &[&block.height],
    )
    .unwrap();
}

pub(super) fn set_constraints(tx: &mut Transaction) {
    tx.execute(
        "alter table core.system_parameters add primary key (height);",
        &[],
    )
    .unwrap();
}

fn parse_i32_value(value: &str) -> i32 {
    let bytes = base16::decode(value.as_bytes()).unwrap();
    i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
}

fn insert_system_parameters(tx: &mut Transaction, height: i32, params: SystemParameters) {
    tx.execute(
        "
        insert into core.system_parameters (
            height,
            storage_fee,
            min_box_value,
            max_block_size,
            max_cost,
            token_access_cost,
            tx_input_cost,
            tx_data_input_cost,
            tx_output_cost,
            block_version
        )
        values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10);",
        &[
            &height,
            &params.storage_fee,
            &params.min_box_value,
            &params.max_block_size,
            &params.max_cost,
            &params.token_access_cost,
            &params.tx_input_cost,
            &params.tx_data_input_cost,
            &params.tx_output_cost,
            &params.block_version,
        ],
    )
    .unwrap();
}

fn insert_unhandled_extension_field(tx: &mut Transaction, height: i32, field: &ExtensionField) {
    tx.execute(
        "
        insert into core.unhandled_extension_fields (height, key, value)
        values ($1, $2, $3);",
        &[&height, &field.key, &field.val],
    )
    .unwrap();
}

impl SystemParameters {
    pub fn has_data(&self) -> bool {
        self.storage_fee.is_some()
            || self.min_box_value.is_some()
            || self.max_block_size.is_some()
            || self.max_cost.is_some()
            || self.token_access_cost.is_some()
            || self.tx_input_cost.is_some()
            || self.tx_data_input_cost.is_some()
            || self.tx_output_cost.is_some()
            || self.block_version.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::SystemParameters;
    /// Return SystemParameters with all fields None
    fn make_empty_system_parameters() -> SystemParameters {
        SystemParameters {
            storage_fee: None,
            min_box_value: None,
            max_block_size: None,
            max_cost: None,
            token_access_cost: None,
            tx_input_cost: None,
            tx_data_input_cost: None,
            tx_output_cost: None,
            block_version: None,
        }
    }

    #[test]
    fn has_data_empty() -> () {
        let p = make_empty_system_parameters();
        assert_eq!(p.has_data(), false);
    }

    #[test]
    fn has_data_storage_fee() -> () {
        let mut p = make_empty_system_parameters();
        p.storage_fee = Some(1);
        assert_eq!(p.has_data(), true);
    }

    #[test]
    fn has_data_min_box_value() {
        let mut p = make_empty_system_parameters();
        p.min_box_value = Some(1);
        assert_eq!(p.has_data(), true);
    }

    #[test]
    fn has_data_max_block_size() {
        let mut p = make_empty_system_parameters();
        p.max_block_size = Some(1);
        assert_eq!(p.has_data(), true);
    }
    #[test]
    fn has_data_max_cost() {
        let mut p = make_empty_system_parameters();
        p.max_cost = Some(1);
        assert_eq!(p.has_data(), true);
    }
    #[test]
    fn has_data_token_access_cost() {
        let mut p = make_empty_system_parameters();
        p.token_access_cost = Some(1);
        assert_eq!(p.has_data(), true);
    }
    #[test]
    fn has_data_tx_input_cost() {
        let mut p = make_empty_system_parameters();
        p.tx_input_cost = Some(1);
        assert_eq!(p.has_data(), true);
    }
    #[test]
    fn has_data_tx_data_input_cost() {
        let mut p = make_empty_system_parameters();
        p.tx_data_input_cost = Some(1);
        assert_eq!(p.has_data(), true);
    }
    #[test]
    fn has_data_tx_output_cost() {
        let mut p = make_empty_system_parameters();
        p.tx_output_cost = Some(1);
        assert_eq!(p.has_data(), true);
    }
    #[test]
    fn has_data_block_version() {
        let mut p = make_empty_system_parameters();
        p.block_version = Some(1);
        assert_eq!(p.has_data(), true);
    }
}
