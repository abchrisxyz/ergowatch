use crate::core::types::AddressID;
use crate::core::types::AssetID;
use crate::core::types::Height;

pub const NETWORK_FEE_ADDRESS_ID: AddressID = AddressID(240_3);

/// SigmaUSD V2 contract was created at height 453064.
pub const CONTRACT_CREATION_HEIGHT: Height = 453064;

/// SigmaUSD V2 contract address id
pub const CONTRACT_ADDRESS_ID: AddressID = AddressID(154228_3);

/// NFT tracking the bank box.
///
/// Token ID: 7d672d1def471720ca5782fd6473e47e796d9ac0c138d9911346f118b2f6d9d9
pub const BANK_NFT: AssetID = 973;

/// SigUSD asset id
///
/// Token ID: 03faf2cb329f2e90d6d23b58d91bbb6c046aa143261cc21f52fbe2824bfcbf04
pub const SC_ASSET_ID: AssetID = 995;

/// SigRSV asset id
///
/// 003bd19d0187117f130b62e1bcab0939929ff5c7709f843c5c4dd158949285d0
pub const RC_ASSET_ID: AssetID = 993;

/// Initial SC supply in bank (in cents)
pub const SC_SUPPLY: i64 = 10000000000001;

/// Initial RC supply in bank
pub const RC_SUPPLY: i64 = 10000000000001;

/// Default protocol price for reserve coin
pub const DEFAULT_RSV_PRICE: i64 = 1000000; // 0.001 ERG

/// ERG/USD oracle pool (v1) NFT
///
/// Token ID: 011d3364de07e5a26f0c4eef0852cddb387039a921b7154ef3cab22c6eda887f
pub const ORACLE_NFT: AssetID = 932;
