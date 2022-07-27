use crate::callback_printf;
use std::ffi::{CStr, CString};
use std::io::{Stdout, Write};

#[no_mangle]
pub extern "C" fn print_prompt() {
    let value = "db > ";
    let c = CString::new(value).unwrap();
    unsafe {
        callback_printf(c.as_ptr());
    }
}
