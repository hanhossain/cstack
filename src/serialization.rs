use std::fmt::{Display, Formatter};
use std::mem::size_of;

pub const ID_SIZE: usize = 4;
pub const ID_OFFSET: usize = 0;
pub const USERNAME_SIZE_SIZE: usize = size_of::<u32>();
pub const USERNAME_SIZE_OFFSET: usize = ID_OFFSET + ID_SIZE;
pub const USERNAME_SIZE: usize = 32;
pub const USERNAME_OFFSET: usize = USERNAME_SIZE_OFFSET + USERNAME_SIZE_SIZE;
pub const EMAIL_SIZE_SIZE: usize = size_of::<u32>();
pub const EMAIL_SIZE_OFFSET: usize = USERNAME_OFFSET + USERNAME_SIZE;
pub const EMAIL_SIZE: usize = 255;
pub const EMAIL_OFFSET: usize = EMAIL_SIZE_OFFSET + EMAIL_SIZE_SIZE;
pub const ROW_SIZE: usize =
    ID_SIZE + USERNAME_SIZE_SIZE + USERNAME_SIZE + EMAIL_SIZE_SIZE + EMAIL_SIZE;

#[derive(Debug, PartialEq)]
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

pub fn serialize_row(source: &Row, destination: &mut [u8]) {
    let id_bytes = u32::to_ne_bytes(source.id);
    destination[ID_OFFSET..ID_OFFSET + ID_SIZE].copy_from_slice(&id_bytes);

    // serialize username size and username
    let username_bytes = source.username.as_bytes();
    destination[USERNAME_SIZE_OFFSET..USERNAME_SIZE_OFFSET + USERNAME_SIZE_SIZE]
        .copy_from_slice(&u32::to_ne_bytes(username_bytes.len() as u32));
    destination[USERNAME_OFFSET..USERNAME_OFFSET + username_bytes.len()]
        .copy_from_slice(username_bytes);

    // serialize email size and email
    let email_bytes = source.email.as_bytes();
    destination[EMAIL_SIZE_OFFSET..EMAIL_SIZE_OFFSET + EMAIL_SIZE_SIZE]
        .copy_from_slice(&u32::to_ne_bytes(email_bytes.len() as u32));
    destination[EMAIL_OFFSET..EMAIL_OFFSET + email_bytes.len()].copy_from_slice(email_bytes);
}

pub fn deserialize_row(source: &[u8]) -> Row {
    let mut id_bytes = [0u8; ID_SIZE];
    id_bytes.copy_from_slice(&source[ID_OFFSET..ID_OFFSET + ID_SIZE]);
    let id = u32::from_ne_bytes(id_bytes);

    // deserialize username size and username
    let mut username_size_bytes = [0u8; USERNAME_SIZE_SIZE];
    username_size_bytes
        .copy_from_slice(&source[USERNAME_SIZE_OFFSET..USERNAME_SIZE_OFFSET + USERNAME_SIZE_SIZE]);
    let username_size = u32::from_ne_bytes(username_size_bytes);
    let username =
        std::str::from_utf8(&source[USERNAME_OFFSET..USERNAME_OFFSET + username_size as usize])
            .unwrap()
            .to_string();

    // deserialize email size and email
    let mut email_size_bytes = [0u8; EMAIL_SIZE_SIZE];
    email_size_bytes
        .copy_from_slice(&source[EMAIL_SIZE_OFFSET..EMAIL_SIZE_OFFSET + EMAIL_SIZE_SIZE]);
    let email_size = u32::from_ne_bytes(email_size_bytes);
    let email = std::str::from_utf8(&source[EMAIL_OFFSET..EMAIL_OFFSET + email_size as usize])
        .unwrap()
        .to_string();

    Row {
        id,
        username,
        email,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serialize_and_deserialize_row() {
        let expected = Row {
            id: 1234,
            username: String::from("John Doe"),
            email: String::from("johndoe@example.com"),
        };
        let mut row_bytes = [0u8; ROW_SIZE];
        serialize_row(&expected, &mut row_bytes);
        let actual = deserialize_row(&row_bytes);
        assert_eq!(expected, actual);
    }
}
