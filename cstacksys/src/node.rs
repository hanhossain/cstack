#![allow(non_camel_case_types)]
use std::ffi::c_void;
use std::mem::size_of;

// Common Node Header Layout
const NODE_TYPE_OFFSET: usize = 0;
const NODE_TYPE_SIZE: usize = size_of::<u8>();
const IS_ROOT_OFFSET: usize = NODE_TYPE_SIZE;

#[repr(C)]
pub enum NodeType {
    NODE_INTERNAL,
    NODE_LEAF,
}

impl From<u8> for NodeType {
    fn from(value: u8) -> Self {
        match value {
            0 => NodeType::NODE_INTERNAL,
            1 => NodeType::NODE_LEAF,
            _ => unreachable!(),
        }
    }
}

impl From<NodeType> for u8 {
    fn from(node_type: NodeType) -> Self {
        match node_type {
            NodeType::NODE_INTERNAL => 0,
            NodeType::NODE_LEAF => 1,
        }
    }
}

#[no_mangle]
pub extern "C" fn get_node_type(node: *const c_void) -> NodeType {
    let value = unsafe { *(node.add(NODE_TYPE_OFFSET) as *const u8) };
    NodeType::from(value)
}

#[no_mangle]
pub extern "C" fn set_node_type(node: *mut c_void, node_type: NodeType) {
    let value = u8::from(node_type);
    unsafe {
        *(node.add(NODE_TYPE_OFFSET) as *mut u8) = value;
    }
}

#[no_mangle]
pub extern "C" fn is_node_root(node: *const c_void) -> bool {
    let value = unsafe { *(node.add(IS_ROOT_OFFSET) as *const u8) };
    value != 0
}

#[no_mangle]
pub extern "C" fn set_node_root(node: *mut c_void, is_root: bool) {
    let value = if is_root { 1 } else { 0 };
    unsafe {
        *(node.add(IS_ROOT_OFFSET) as *mut u8) = value;
    }
}
