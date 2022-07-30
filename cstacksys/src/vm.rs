#![allow(non_camel_case_types)]

use crate::repl::InputBuffer;
use crate::serialization::{Row, COLUMN_EMAIL_SIZE, COLUMN_USERNAME_SIZE};
use libc::strcpy;
use std::ffi::{CStr, CString};
use std::str::FromStr;

#[repr(C)]
pub enum StatementType {
    STATEMENT_INSERT,
    STATEMENT_SELECT,
}

#[repr(C)]
pub struct Statement {
    r#type: StatementType,
    row_to_insert: Row, // only used by insert statement
}

#[repr(C)]
pub enum PrepareResult {
    PREPARE_SUCCESS,
    PREPARE_NEGATIVE_ID,
    PREPARE_STRING_TOO_LONG,
    PREPARE_SYNTAX_ERROR,
    PREPARE_UNRECOGNIZED_STATEMENT,
}

#[no_mangle]
pub unsafe extern "C" fn prepare_statement(
    input_buffer: &mut InputBuffer,
    statement: &mut Statement,
) -> PrepareResult {
    let string = CStr::from_ptr(input_buffer.buffer).to_str().unwrap();
    if &string[..6] == "insert" {
        prepare_insert(input_buffer, statement)
    } else if string == "select" {
        statement.r#type = StatementType::STATEMENT_SELECT;
        PrepareResult::PREPARE_SUCCESS
    } else {
        PrepareResult::PREPARE_UNRECOGNIZED_STATEMENT
    }
}

#[allow(temporary_cstring_as_ptr)]
unsafe fn prepare_insert(
    input_buffer: &mut InputBuffer,
    statement: &mut Statement,
) -> PrepareResult {
    statement.r#type = StatementType::STATEMENT_INSERT;
    let mut splitter = CStr::from_ptr(input_buffer.buffer)
        .to_str()
        .unwrap()
        .split(" ");
    let _keyword = splitter.next();
    let id_string = splitter.next();
    let username = splitter.next();
    let email = splitter.next();

    let (id_string, username, email) = match (id_string, username, email) {
        (Some(x), Some(y), Some(z)) => (x, y, z),
        _ => return PrepareResult::PREPARE_SYNTAX_ERROR,
    };

    let id = i32::from_str(id_string).unwrap();
    if id < 0 {
        return PrepareResult::PREPARE_NEGATIVE_ID;
    }

    if username.len() > COLUMN_USERNAME_SIZE || email.len() > COLUMN_EMAIL_SIZE {
        return PrepareResult::PREPARE_STRING_TOO_LONG;
    }

    statement.row_to_insert.id = id as u32;
    strcpy(
        statement.row_to_insert.username.as_mut_ptr(),
        CString::new(username).unwrap().as_ptr(),
    );
    strcpy(
        statement.row_to_insert.email.as_mut_ptr(),
        CString::new(email).unwrap().as_ptr(),
    );

    PrepareResult::PREPARE_SUCCESS
}
