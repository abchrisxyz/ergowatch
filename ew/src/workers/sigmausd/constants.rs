use crate::core::types::AddressID;
use crate::core::types::Height;

pub const NETWORK_FEE_ADDRESS_ID: AddressID = 240;

/// SigmaUSD V2 contract was created at height 453064.
pub const CONTRACT_CREATION_HEIGHT: Height = 453064;

/// SigmaUSD V2 contract address id
pub const CONTRACT_ADDRESS_ID: AddressID = 154228;

/// NFT following the bank box.
pub const BANK_NFT: &'static str =
    "7d672d1def471720ca5782fd6473e47e796d9ac0c138d9911346f118b2f6d9d9";

/// SigUSD token id
pub const SC_TOKEN_ID: &'static str =
    "03faf2cb329f2e90d6d23b58d91bbb6c046aa143261cc21f52fbe2824bfcbf04";

/// SigRSV token id
pub const RC_TOKEN_ID: &'static str =
    "003bd19d0187117f130b62e1bcab0939929ff5c7709f843c5c4dd158949285d0";

/// Initial SC supply in bank (in cents)
pub const SC_SUPPLY: i64 = 10000000000001;

/// Initial RC supply in bank
pub const RC_SUPPLY: i64 = 10000000000001;

/// Default protocol price for reserve coin
pub const DEFAULT_RSV_PRICE: i64 = 1000000; // TODO

/// ERG/USD oracle pool (v1) NFT
pub const ORACLE_NFT: &'static str =
    "011d3364de07e5a26f0c4eef0852cddb387039a921b7154ef3cab22c6eda887f";

/// Oracle epoch preparation address id
pub const ORACLE_EPOCH_PREP_ADDRESS_ID: AddressID = 153537;
