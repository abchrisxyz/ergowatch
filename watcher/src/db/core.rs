//! # core
//!
//! Process blocks into core tables data.

pub mod genesis;

mod additional_registers;
mod assets;
mod data_inputs;
mod headers;
mod inputs;
mod outputs;
mod tokens;
mod transactions;

use super::Transaction;
use crate::parsing::BlockData;

pub(super) fn include_block(tx: &mut Transaction, block: &BlockData) -> anyhow::Result<()> {
    headers::include(tx, block);
    transactions::include(tx, block);
    outputs::include(tx, block);
    inputs::include(tx, block);
    data_inputs::include(tx, block);
    additional_registers::include(tx, block);
    tokens::include(tx, block);
    assets::include(tx, block);
    Ok(())
}

pub(super) fn rollback_block(tx: &mut Transaction, block: &BlockData) -> anyhow::Result<()> {
    assets::rollback(tx, block);
    tokens::rollback(tx, block);
    additional_registers::rollback(tx, block);
    data_inputs::rollback(tx, block);
    inputs::rollback(tx, block);
    outputs::rollback(tx, block);
    transactions::rollback(tx, block);
    headers::rollback(tx, block);

    Ok(())
}

pub(super) fn set_constraints(tx: &mut Transaction) {
    headers::set_constraints(tx);
    transactions::set_constraints(tx);
    outputs::set_constraints(tx);
    inputs::set_constraints(tx);
    data_inputs::set_constraints(tx);
    additional_registers::set_constraints(tx);
    tokens::set_constraints(tx);
    assets::set_constraints(tx);
}
