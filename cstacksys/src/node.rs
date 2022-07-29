#![allow(non_camel_case_types)]
use crate::serialization::ROW_SIZE;
use libc::{exit, EXIT_FAILURE};
use std::ffi::c_void;
use std::mem::size_of;

// Common Node Header Layout
const NODE_TYPE_OFFSET: usize = 0;
const NODE_TYPE_SIZE: usize = size_of::<u8>();
const IS_ROOT_SIZE: usize = size_of::<u8>();
const IS_ROOT_OFFSET: usize = NODE_TYPE_SIZE;
const PARENT_POINTER_SIZE: usize = size_of::<u32>();
const PARENT_POINTER_OFFSET: usize = IS_ROOT_OFFSET + IS_ROOT_SIZE;
const COMMON_NODE_HEADER_SIZE: usize = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;

// Internal Node Header Layout
const INTERNAL_NODE_NUM_KEYS_SIZE: usize = size_of::<u32>();
const INTERNAL_NODE_NUM_KEYS_OFFSET: usize = COMMON_NODE_HEADER_SIZE;
const INTERNAL_NODE_RIGHT_CHILD_SIZE: usize = size_of::<u32>();
const INTERNAL_NODE_RIGHT_CHILD_OFFSET: usize =
    INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE;
const INTERNAL_NODE_HEADER_SIZE: usize =
    COMMON_NODE_HEADER_SIZE + INTERNAL_NODE_NUM_KEYS_SIZE + INTERNAL_NODE_RIGHT_CHILD_SIZE;

// Internal Node Body Layout
const INTERNAL_NODE_KEY_SIZE: usize = size_of::<u32>();
const INTERNAL_NODE_CHILD_SIZE: usize = size_of::<u32>();
const INTERNAL_NODE_CELL_SIZE: usize = INTERNAL_NODE_CHILD_SIZE + INTERNAL_NODE_KEY_SIZE;

// Leaf Node Header Layout
const LEAF_NODE_NUM_CELLS_SIZE: usize = size_of::<u32>();
const LEAF_NODE_NUM_CELLS_OFFSET: usize = COMMON_NODE_HEADER_SIZE;
const LEAF_NODE_NEXT_LEAF_SIZE: usize = size_of::<u32>();
const LEAF_NODE_NEXT_LEAF_OFFSET: usize = LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_NUM_CELLS_SIZE;
const LEAF_NODE_HEADER_SIZE: usize =
    COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE + LEAF_NODE_NEXT_LEAF_SIZE;

// Leaf Node Body Layout
const LEAF_NODE_KEY_SIZE: usize = size_of::<u32>();
const LEAF_NODE_VALUE_SIZE: usize = ROW_SIZE;
const LEAF_NODE_CELL_SIZE: usize = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;

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
pub unsafe extern "C" fn get_node_type(node: *const c_void) -> NodeType {
    let value = *(node.add(NODE_TYPE_OFFSET) as *const u8);
    NodeType::from(value)
}

#[no_mangle]
pub unsafe extern "C" fn set_node_type(node: *mut c_void, node_type: NodeType) {
    let value = u8::from(node_type);
    *(node.add(NODE_TYPE_OFFSET) as *mut u8) = value;
}

#[no_mangle]
pub extern "C" fn is_node_root(node: *const c_void) -> bool {
    let value = unsafe { *(node.add(IS_ROOT_OFFSET) as *const u8) };
    value != 0
}

#[no_mangle]
pub unsafe extern "C" fn set_node_root(node: *mut c_void, is_root: bool) {
    let value = if is_root { 1 } else { 0 };
    *(node.add(IS_ROOT_OFFSET) as *mut u8) = value;
}

#[no_mangle]
pub unsafe extern "C" fn node_parent(node: *mut c_void) -> *mut u32 {
    node.add(PARENT_POINTER_OFFSET) as *mut u32
}

#[no_mangle]
pub unsafe extern "C" fn internal_node_num_keys(node: *mut c_void) -> *mut u32 {
    node.add(INTERNAL_NODE_NUM_KEYS_OFFSET) as *mut u32
}

#[no_mangle]
pub unsafe extern "C" fn internal_node_right_child(node: *mut c_void) -> *mut u32 {
    node.add(INTERNAL_NODE_RIGHT_CHILD_OFFSET) as *mut u32
}

#[no_mangle]
pub unsafe extern "C" fn internal_node_cell(node: *mut c_void, cell_num: u32) -> *mut u32 {
    node.add(INTERNAL_NODE_HEADER_SIZE + cell_num as usize * INTERNAL_NODE_CELL_SIZE) as *mut u32
}

#[no_mangle]
pub unsafe extern "C" fn initialize_internal_node(node: *mut c_void) {
    set_node_type(node, NodeType::NODE_INTERNAL);
    set_node_root(node, false);
    *internal_node_num_keys(node) = 0;
}

#[no_mangle]
pub unsafe extern "C" fn internal_node_child(node: *mut c_void, child_num: u32) -> *mut u32 {
    let num_keys = *internal_node_num_keys(node);
    if child_num > num_keys {
        println!("Tried to access child_num {child_num} > num_keys {num_keys}");
        exit(EXIT_FAILURE);
    }

    if child_num == num_keys {
        internal_node_right_child(node)
    } else {
        internal_node_cell(node, child_num)
    }
}

#[no_mangle]
pub unsafe extern "C" fn internal_node_key(node: *mut c_void, key_num: u32) -> *mut u32 {
    internal_node_cell(node, key_num).add(INTERNAL_NODE_CHILD_SIZE)
}

#[no_mangle]
pub unsafe extern "C" fn leaf_node_num_cells(node: *mut c_void) -> *mut u32 {
    node.add(LEAF_NODE_NUM_CELLS_OFFSET) as *mut u32
}

#[no_mangle]
pub unsafe extern "C" fn leaf_node_cell(node: *mut c_void, cell_num: u32) -> *mut c_void {
    node.add(LEAF_NODE_HEADER_SIZE + cell_num as usize * LEAF_NODE_CELL_SIZE)
}

#[no_mangle]
pub unsafe extern "C" fn leaf_node_key(node: *mut c_void, cell_num: u32) -> *mut u32 {
    leaf_node_cell(node, cell_num) as *mut u32
}

#[no_mangle]
pub unsafe extern "C" fn leaf_node_value(node: *mut c_void, cell_num: u32) -> *mut c_void {
    leaf_node_cell(node, cell_num).add(LEAF_NODE_KEY_SIZE)
}

#[no_mangle]
pub unsafe extern "C" fn leaf_node_next_leaf(node: *mut c_void) -> *mut u32 {
    node.add(LEAF_NODE_NEXT_LEAF_OFFSET) as *mut u32
}
