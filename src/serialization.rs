use libc::{c_char, c_void, memcpy};
use std::ffi::CStr;

pub const COLUMN_USERNAME_SIZE: usize = 32;
pub const COLUMN_EMAIL_SIZE: usize = 255;
pub const USERNAME_SIZE: usize = COLUMN_USERNAME_SIZE + 1;
pub const EMAIL_SIZE: usize = COLUMN_EMAIL_SIZE + 1;
pub const ID_SIZE: usize = 4;
pub const ID_OFFSET: usize = 0;
pub const USERNAME_OFFSET: usize = ID_OFFSET + ID_SIZE;
pub const EMAIL_OFFSET: usize = USERNAME_OFFSET + USERNAME_SIZE;
pub const ROW_SIZE: usize = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

pub struct Row {
    pub id: u32,
    pub username: [c_char; USERNAME_SIZE],
    pub email: [c_char; EMAIL_SIZE],
}

impl Row {
    pub fn new() -> Row {
        Row {
            id: 0,
            email: [0; EMAIL_SIZE],
            username: [0; USERNAME_SIZE],
        }
    }

    pub unsafe fn print_row(&self) {
        let username = ptr_to_str(&self.username);
        let email = ptr_to_str(&self.email);
        println!("({}, {}, {})", self.id, username, email);
    }
}

pub(crate) unsafe fn serialize_row(source: &Row, destination: *mut c_void) {
    memcpy(
        destination.add(ID_OFFSET),
        &source.id as *const u32 as *const c_void,
        ID_SIZE,
    );

    memcpy(
        destination.add(USERNAME_OFFSET),
        &source.username as *const c_char as *const c_void,
        USERNAME_SIZE,
    );

    memcpy(
        destination.add(EMAIL_OFFSET),
        &source.email as *const c_char as *const c_void,
        EMAIL_SIZE,
    );
}

pub(crate) unsafe fn deserialize_row(source: *const c_void, destination: &mut Row) {
    memcpy(
        &mut destination.id as *mut u32 as *mut c_void,
        source.add(ID_OFFSET),
        ID_SIZE,
    );

    memcpy(
        &mut destination.username as *mut c_char as *mut c_void,
        source.add(USERNAME_OFFSET),
        USERNAME_SIZE,
    );

    memcpy(
        &mut destination.email as *mut c_char as *mut c_void,
        source.add(EMAIL_OFFSET),
        EMAIL_SIZE,
    );
}

unsafe fn ptr_to_str(value: &[c_char]) -> &str {
    CStr::from_ptr(value.as_ptr())
        .to_str()
        .expect("failed to convert from c_char")
}
