mod node;
mod repl;

#[no_mangle]
pub extern "C" fn hello_world() {
    println!("Hello world!");
}

#[allow(unused_doc_comments)]
/// cbindgen:ignore
extern "C" {
    fn callback_printf(string: *const libc::c_char);
}
