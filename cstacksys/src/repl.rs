use crate::callback_printf;
use crate::node::{
    get_node_type, internal_node_child, internal_node_key, internal_node_num_keys,
    internal_node_right_child, leaf_node_key, leaf_node_num_cells, NodeType,
    COMMON_NODE_HEADER_SIZE, LEAF_NODE_CELL_SIZE, LEAF_NODE_HEADER_SIZE, LEAF_NODE_MAX_CELLS,
    LEAF_NODE_SPACE_FOR_CELLS,
};
use crate::pager::{get_page, Pager};
use crate::serialization::ROW_SIZE;
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
    pub buffer: *mut c_char,
    pub input_length: usize,
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

pub(crate) fn print_constants() {
    println!("ROW_SIZE: {}", ROW_SIZE);
    println!("COMMON_NODE_HEADER_SIZE: {}", COMMON_NODE_HEADER_SIZE);
    println!("LEAF_NODE_HEADER_SIZE: {}", LEAF_NODE_HEADER_SIZE);
    println!("LEAF_NODE_CELL_SIZE: {}", LEAF_NODE_CELL_SIZE);
    println!("LEAF_NODE_SPACE_FOR_CELLS: {}", LEAF_NODE_SPACE_FOR_CELLS);
    println!("LEAF_NODE_MAX_CELLS: {}", LEAF_NODE_MAX_CELLS);
}

pub(crate) unsafe fn print_tree(pager: &mut Pager, page_num: u32, indentation_level: u32) {
    let node = get_page(pager, page_num as usize);

    match get_node_type(node) {
        NodeType::NODE_LEAF => {
            let num_keys = *leaf_node_num_cells(node);
            indent(indentation_level);
            println!("- leaf (size {})", num_keys);
            for i in 0..num_keys {
                indent(indentation_level + 1);
                println!("- {}", *leaf_node_key(node, i));
            }
        }
        NodeType::NODE_INTERNAL => {
            let num_keys = *internal_node_num_keys(node);
            indent(indentation_level);
            println!("- internal (size {})", num_keys);
            for i in 0..num_keys {
                let child = *internal_node_child(node, i);
                print_tree(pager, child, indentation_level + 1);

                indent(indentation_level + 1);
                println!("- key {}", *internal_node_key(node, i));
            }
            let child = *internal_node_right_child(node);
            print_tree(pager, child, indentation_level + 1);
        }
    }
}

fn indent(level: u32) {
    for _ in 0..level {
        print!("  ");
    }
}
