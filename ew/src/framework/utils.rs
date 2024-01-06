use crate::core::types::Height;

#[derive(Debug)]
pub struct BlockRange {
    pub first_height: Height,
    pub last_height: Height,
}

impl BlockRange {
    pub fn new(first_height: Height, last_height: Height) -> Self {
        Self {
            first_height,
            last_height,
        }
    }

    /// Number of blocks in range
    pub fn size(&self) -> i32 {
        self.last_height - self.first_height + 1
    }
}
