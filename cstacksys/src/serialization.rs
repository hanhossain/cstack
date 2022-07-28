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

#[repr(C)]
pub struct Row {
    pub id: u32,
    pub username: [c_char; USERNAME_SIZE],
    pub email: [c_char; EMAIL_SIZE],
}

#[no_mangle]
pub unsafe extern "C" fn serialize_row(source: &Row, destination: *mut c_void) {
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

#[no_mangle]
pub unsafe extern "C" fn deserialize_row(source: *const c_void, destination: &mut Row) {
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

#[no_mangle]
pub unsafe extern "C" fn print_row(row: &Row) {
    let username = ptr_to_str(&row.username);
    let email = ptr_to_str(&row.email);
    println!("({}, {}, {})", row.id, username, email);
}

unsafe fn ptr_to_str(value: &[c_char]) -> &str {
    CStr::from_ptr(value.as_ptr())
        .to_str()
        .expect("failed to convert from c_char")
}
