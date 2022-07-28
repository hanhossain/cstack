use crate::callback_printf;
use libc::{c_char, exit, EXIT_FAILURE};
use std::ffi::CString;
use std::io::BufRead;
use std::ptr::null_mut;

#[no_mangle]
pub extern "C" fn print_prompt() {
    let value = "db > ";
    let c = CString::new(value).unwrap();
    unsafe {
        callback_printf(c.as_ptr());
    }
}

#[repr(C)]
pub struct InputBuffer {
    buffer: *mut c_char,
    input_length: usize,
}

#[no_mangle]
pub extern "C" fn new_input_buffer() -> *mut InputBuffer {
    let input_buffer = Box::new(InputBuffer {
        buffer: null_mut(),
        input_length: 0,
    });
    Box::into_raw(input_buffer)
}

#[no_mangle]
pub extern "C" fn read_input(input_buffer: &mut InputBuffer) {
    let mut buffer = if input_buffer.buffer.is_null() {
        String::new()
    } else {
        // convert any existing buffer into a String and clear it
        let cstring = unsafe { CString::from_raw(input_buffer.buffer) };
        input_buffer.buffer = null_mut();
        let mut string = cstring.into_string().expect("should have valid utf-8");
        string.clear();
        string
    };

    let mut stdin = std::io::stdin().lock();
    let bytes_read = match stdin.read_line(&mut buffer) {
        Ok(n) => n,
        Err(_) => {
            println!("Error reading input");
            unsafe { exit(EXIT_FAILURE) };
        }
    };

    let cstring = CString::new(buffer.trim_end()).expect("found a null terminated string");
    input_buffer.buffer = cstring.into_raw();
    input_buffer.input_length = bytes_read;
}
