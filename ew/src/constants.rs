use crate::core::types::Height;
use crate::core::types::Timestamp;

pub const GENESIS_TIMESTAMP: Timestamp = 1561978800000;
pub const ZERO_HEADER: &str = "0000000000000000000000000000000000000000000000000000000000000000";

/// Number of blocks in a voting epoch
pub const VOTING_EPOCH_LENGTH: Height = 1024;

pub mod address_ids {
    use crate::core::types::AddressID;

    /// Emission contract
    ///
    /// 2Z4YBkDsDvQj8BX7xiySFewjitqp2ge9c99jfes2whbtKitZTxdBYqbrVZUvZvKv6aqn9by4kp3LE1c26LCyosFnVnm6b6U1JYvWpYmL2ZnixJbXLjWAWuBThV1D6dLpqZJYQHYDznJCk49g5TUiS4q8khpag2aNmHwREV7JSsypHdHLgJT7MGaw51aJfNubyzSKxZ4AJXFS27EfXwyCLzW1K6GVqwkJtCoPvrcLqmqwacAWJPkmh78nke9H4oT88XmSbRt2n9aWZjosiZCafZ4osUDxmZcc5QVEeTWn8drSraY3eFKe8Mu9MSCcVU
    pub const EMISSION: AddressID = AddressID(1_3);

    /// EF treasury contract
    ///
    /// 4L1ktFSzm3SH1UioDuUf5hyaraHird4D2dEACwQ1qHGjSKtA6KaNvSzRCZXZGf9jkfNAEC1SrYaZmCuvb2BKiXk5zW9xuvrXFT7FdNe2KqbymiZvo5UQLAm5jQY8ZBRhTZ4AFtZa1UF5nd4aofwPiL7YkJuyiL5hDHMZL1ZnyL746tHmRYMjAhCgE7d698dRhkdSeVy
    pub const TREASURY: AddressID = AddressID(3_3);

    /// Fees contract
    ///
    /// 2iHkR7CWvD1R4j1yZg5bkeDRQavjAaVPeTDFGGLZduHyfWMuYpmhHocX8GJoaieTx78FntzJbCBVL6rf96ocJoZdmWBL2fci7NqWgAirppPQmZ7fN9V6z13Ay6brPriBKYqLp1bT2Fk4FkFLCfdPpe
    pub const FEES: AddressID = AddressID(240_3);

    /// EIP-27 Re-emission contract
    ///
    /// 22WkKcVUvboYCZJe1urbmvBL3j67LKb5KEAvFhJXqA6ubYvHpSCvbvwvEY3xzUr7QvxpEtqjzMAPMsVdZh1VGWmZphvKoJdVzL1ayhsMftTtEFoA3YYdq3zKeeYXavVrrPUmK3fRXJ2HWEbZexewtBWcgAnHBw5tKvYFy9dEUi645gE2fYMUvVBtbvMExE9mjZ2W9goWkqu1VtThAsMZWZWjHxDjX116HpeQKu9b9neEUBj4kE5sX8QXaV6ZeReXxYHFJFg2rmaTknSPMxHXA8NpQKgzryBwLssp5EJ1QTqn5R6xuvGgFCEUZicCEo8qk8UNbE7e2d4WqW5qzpQPzJkKoPa5UtJEPYDWNhaCKmCpzdSc77
    pub const REEMISSION: AddressID = AddressID(596523_3);

    /// EIP-27 Pay-to-reemission contract
    ///
    /// 6KxusedL87PBibr1t1f4ggzAyTAmWEPqSpqXbkdoybNwHVw5Nb7cUESBmQw5XK8TyvbQiueyqkR9XMNaUgpWx3jT54p
    pub const PAY_TO_REEMISSION: AddressID = AddressID(599350_3);

    /// All (re)emission related contracts.
    pub const EMISSION_CONTRACTS: [AddressID; 3] = [EMISSION, REEMISSION, PAY_TO_REEMISSION];
}

pub mod settings {
    use crate::core::types::Height;
    /// Maximum number of blocks that can be rolled back.
    ///
    /// If exceeded, some workers will have to be resynced from scratch.
    pub const ROLLBACK_HORIZON: Height = 20;
}
