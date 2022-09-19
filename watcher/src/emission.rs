const FIXED_RATE: i64 = 75000000000; // 75 ERG
const FIXED_RATE_PERIOD: i64 = 525600;
const EPOCH_LENGTH: i64 = 64800;
const INITIAL_TREASURY_REWARD: i64 = 7500000000; // 7.5 ERG
const ONE_EPOCH_REDUCTION: i64 = 3000000000; // 3 ERG

// EIP-27
const REEMISSION_ACTIVATION_HEIGHT: i64 = 777217;
const REEMISSION_START_HEIGHT: i64 = 2080800;
const BASIC_CHARGE_AMOUNT: i64 = 12000000000; // 12 ERG
const THREE_ERG: i64 = 3000000000; // 3 ERG

/// Coinbase emission at height
pub fn emission_at_height(height: i32) -> i64 {
    let h = height as i64;
    if h < 1 {
        0i64
    } else if h < REEMISSION_START_HEIGHT {
        pre_eip27_emission_at_height(height as i64) - reemission_at_height(h)
    } else {
        //TODO: handle end of reemission
        THREE_ERG
    }
}

/// Miner reward at given `height`
pub fn miner_reward_at_height(height: i32) -> i64 {
    let h = height as i64;
    if h < 1 {
        0i64
    } else if h < REEMISSION_START_HEIGHT {
        pre_eip27_miner_reward_at_height(height as i64) - reemission_at_height(h)
    } else {
        //TODO: handle end of reemission
        THREE_ERG
    }
}

/// Coinbase emission at height, before EIP-27
fn pre_eip27_emission_at_height(h: i64) -> i64 {
    if h < FIXED_RATE_PERIOD {
        FIXED_RATE
    } else {
        let epoch = 1 + (h - FIXED_RATE_PERIOD) / EPOCH_LENGTH;
        std::cmp::max(FIXED_RATE - ONE_EPOCH_REDUCTION * epoch, 0)
    }
}

/// Miner reward at given `height`
fn pre_eip27_miner_reward_at_height(h: i64) -> i64 {
    if h < FIXED_RATE_PERIOD + 2 * EPOCH_LENGTH {
        FIXED_RATE - INITIAL_TREASURY_REWARD
    } else {
        let epoch = 1i64 + (h - FIXED_RATE_PERIOD) / EPOCH_LENGTH as i64;
        std::cmp::max(FIXED_RATE - ONE_EPOCH_REDUCTION * epoch, 0)
    }
}

/// Nanoerg reserved reserved for later reemission
fn reemission_at_height(h: i64) -> i64 {
    let emission = pre_eip27_emission_at_height(h);
    if h >= REEMISSION_ACTIVATION_HEIGHT && emission >= (BASIC_CHARGE_AMOUNT + THREE_ERG) {
        BASIC_CHARGE_AMOUNT
    } else if h >= REEMISSION_ACTIVATION_HEIGHT && emission > THREE_ERG {
        emission - THREE_ERG
    } else {
        0
    }
}

#[cfg(test)]
mod tests {
    use super::emission_at_height;
    use super::miner_reward_at_height;
    use pretty_assertions::assert_eq;

    #[test]
    fn test_emission_at_height() -> () {
        assert_eq!(emission_at_height(0), 0i64);
        assert_eq!(emission_at_height(1), 75_000_000_000i64);
        assert_eq!(emission_at_height(525599), 75_000_000_000i64);
        assert_eq!(emission_at_height(525600), 72_000_000_000i64);
        assert_eq!(emission_at_height(777216), 63_000_000_000i64);
        assert_eq!(emission_at_height(777217), 51_000_000_000i64);
        assert_eq!(emission_at_height(2080799), 3_000_000_000i64);
        assert_eq!(emission_at_height(2080800), 3_000_000_000i64);
    }

    #[test]
    fn test_rewards_at_height() -> () {
        assert_eq!(miner_reward_at_height(0), 0i64);
        assert_eq!(miner_reward_at_height(1), 67_500_000_000i64);
        assert_eq!(miner_reward_at_height(525599), 67_500_000_000i64);
        assert_eq!(miner_reward_at_height(525600), 67_500_000_000i64);
        assert_eq!(miner_reward_at_height(777216), 63_000_000_000i64);
        assert_eq!(miner_reward_at_height(777217), 51_000_000_000i64);
        assert_eq!(miner_reward_at_height(2080799), 3_000_000_000i64);
        assert_eq!(miner_reward_at_height(2080800), 3_000_000_000i64);
    }
}
