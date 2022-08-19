use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use std::mem::size_of;

const ID_SIZE: usize = size_of::<u32>();
const USERNAME_SIZE_SIZE: usize = size_of::<u64>();
pub const USERNAME_SIZE: usize = 32;
const EMAIL_SIZE_SIZE: usize = size_of::<u64>();
pub const EMAIL_SIZE: usize = 255;
pub const ROW_SIZE: usize =
    ID_SIZE + USERNAME_SIZE_SIZE + USERNAME_SIZE + EMAIL_SIZE_SIZE + EMAIL_SIZE;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Row {
    pub id: u32,
    pub username: String,
    pub email: String,
}

impl Display for Row {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "({}, {}, {})",
            self.id, self.username, self.email
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serialize_row() {
        let expected = Row {
            id: 1234,
            username: String::from("John Doe"),
            email: String::from("johndoe@example.com"),
        };
        let mut row_bytes = [0u8; ROW_SIZE];

        bincode::serialize_into(row_bytes.as_mut_slice(), &expected).unwrap();
        let actual: Row = bincode::deserialize(&row_bytes).unwrap();

        assert_eq!(expected, actual);
    }

    #[test]
    fn serialize_row_max_length() {
        let expected = Row {
            id: u32::MAX,
            username: String::from("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            email: std::iter::repeat("a").take(255).collect(),
        };
        let mut row_bytes = [0u8; ROW_SIZE];

        bincode::serialize_into(row_bytes.as_mut_slice(), &expected).unwrap();
        let actual: Row = bincode::deserialize(&row_bytes).unwrap();

        assert_eq!(expected, actual);
    }
}
