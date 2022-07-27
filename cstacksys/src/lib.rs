use std::ffi::CString;

mod repl;

#[no_mangle]
pub extern "C" fn hello_world() {
    println!("Hello world!");
}

extern "C" {
    fn callback_printf(string: *const libc::c_char);
}
