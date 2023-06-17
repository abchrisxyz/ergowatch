use base16;

/// Convert node vote string to array of ints.
///
/// Votes are base16 encoded and represent a Scala Byte (i8).
pub fn from_str(votes: &str) -> [i8; 3] {
    let bytes = base16::decode(votes.as_bytes()).unwrap();
    [bytes[0] as i8, bytes[1] as i8, bytes[2] as i8]
}

#[cfg(test)]
mod tests {
    use super::from_str;
    use pretty_assertions::assert_eq;

    #[test]
    fn check_parse_votes() {
        assert_eq!(from_str("080400"), [8i8, 4i8, 0i8]);
        assert_eq!(from_str("01007c"), [1i8, 0i8, 124i8]);
    }
}
