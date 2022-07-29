#![allow(non_camel_case_types)]
use crate::pager::{get_page, get_unused_page_num, PAGE_SIZE};
use crate::serialization::ROW_SIZE;
use crate::table::Table;
use libc::{exit, memcpy, EXIT_FAILURE};
use std::ffi::c_void;
use std::mem::size_of;

// Common Node Header Layout
const NODE_TYPE_OFFSET: usize = 0;
const NODE_TYPE_SIZE: usize = size_of::<u8>();
const IS_ROOT_SIZE: usize = size_of::<u8>();
const IS_ROOT_OFFSET: usize = NODE_TYPE_SIZE;
const PARENT_POINTER_SIZE: usize = size_of::<u32>();
const PARENT_POINTER_OFFSET: usize = IS_ROOT_OFFSET + IS_ROOT_SIZE;
pub(crate) const COMMON_NODE_HEADER_SIZE: usize =
    NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;

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
pub(crate) const LEAF_NODE_HEADER_SIZE: usize =
    COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE + LEAF_NODE_NEXT_LEAF_SIZE;

// Leaf Node Body Layout
const LEAF_NODE_KEY_SIZE: usize = size_of::<u32>();
const LEAF_NODE_VALUE_SIZE: usize = ROW_SIZE;
pub(crate) const LEAF_NODE_CELL_SIZE: usize = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
pub(crate) const LEAF_NODE_SPACE_FOR_CELLS: usize = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
pub(crate) const LEAF_NODE_MAX_CELLS: usize = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;

// Internal Node Body Layout
const INTERNAL_NODE_MAX_CELLS: usize = 3;

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

unsafe fn set_node_type(node: *mut c_void, node_type: NodeType) {
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

pub(crate) unsafe fn internal_node_num_keys(node: *mut c_void) -> *mut u32 {
    node.add(INTERNAL_NODE_NUM_KEYS_OFFSET) as *mut u32
}

pub(crate) unsafe fn internal_node_right_child(node: *mut c_void) -> *mut u32 {
    node.add(INTERNAL_NODE_RIGHT_CHILD_OFFSET) as *mut u32
}

unsafe fn internal_node_cell(node: *mut c_void, cell_num: u32) -> *mut u32 {
    node.add(INTERNAL_NODE_HEADER_SIZE + cell_num as usize * INTERNAL_NODE_CELL_SIZE) as *mut u32
}

unsafe fn initialize_internal_node(node: *mut c_void) {
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

pub(crate) unsafe fn internal_node_key(node: *mut c_void, key_num: u32) -> *mut u32 {
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

#[no_mangle]
pub unsafe extern "C" fn get_node_max_key(node: *mut c_void) -> u32 {
    match get_node_type(node) {
        NodeType::NODE_INTERNAL => *internal_node_key(node, *internal_node_num_keys(node) - 1),
        NodeType::NODE_LEAF => *leaf_node_key(node, *leaf_node_num_cells(node) - 1),
    }
}

#[no_mangle]
pub unsafe extern "C" fn initialize_leaf_node(node: *mut c_void) {
    set_node_type(node, NodeType::NODE_LEAF);
    set_node_root(node, false);
    *leaf_node_num_cells(node) = 0;
    *leaf_node_next_leaf(node) = 0; // 0 represents no sibling
}

/// Returns the index of the child which should contain the given key.
#[no_mangle]
pub unsafe extern "C" fn internal_node_find_child(node: *mut c_void, key: u32) -> u32 {
    let num_keys = *internal_node_num_keys(node);

    // binary search
    let mut min_index = 0;
    let mut max_index = num_keys; // there is one more child than key

    while min_index != max_index {
        let index = (min_index + max_index) / 2;
        let key_to_right = *internal_node_key(node, index);
        if key_to_right >= key {
            max_index = index;
        } else {
            min_index = index + 1;
        }
    }

    min_index
}

// Handle splitting the root.
// Old root copied to new page, becomes the left child.
// Address of right child passed in.
// Re-initialize root page to contain the new root node.
// New root node points to two children.
#[no_mangle]
pub unsafe extern "C" fn create_new_root(table: &mut Table, right_child_page_num: u32) {
    let pager = &mut *table.pager;
    let root = get_page(pager, table.root_page_num as usize);
    let right_child = get_page(pager, right_child_page_num as usize);
    let left_child_page_num = get_unused_page_num(pager);
    let left_child = get_page(pager, left_child_page_num as usize);

    // Left child has data copied from old root
    memcpy(left_child, root, PAGE_SIZE);
    set_node_root(left_child, false);

    // Root node is a new internal node with one key and two children
    initialize_internal_node(root);
    set_node_root(root, true);
    *internal_node_num_keys(root) = 1;
    *internal_node_child(root, 0) = left_child_page_num;
    let left_child_max_key = get_node_max_key(left_child);
    *internal_node_key(root, 0) = left_child_max_key;
    *internal_node_right_child(root) = right_child_page_num;
    *node_parent(left_child) = table.root_page_num;
    *node_parent(right_child) = table.root_page_num;
}

#[no_mangle]
pub unsafe extern "C" fn update_internal_node_key(node: *mut c_void, old_key: u32, new_key: u32) {
    let old_child_index = internal_node_find_child(node, old_key);
    *internal_node_key(node, old_child_index) = new_key;
}

/// Add a child/key pair to parent that corresponds to child.
#[no_mangle]
pub unsafe extern "C" fn internal_node_insert(
    table: &mut Table,
    parent_page_num: u32,
    child_page_num: u32,
) {
    let pager = &mut *table.pager;
    let parent = get_page(pager, parent_page_num as usize);
    let child = get_page(pager, child_page_num as usize);
    let child_max_key = get_node_max_key(child);
    let index = internal_node_find_child(parent, child_max_key);

    let original_num_keys = *internal_node_num_keys(parent);
    *internal_node_num_keys(parent) = original_num_keys + 1;

    if original_num_keys as usize >= INTERNAL_NODE_MAX_CELLS {
        println!("Need to implement splitting internal node");
        exit(EXIT_FAILURE);
    }

    let right_child_page_num = *internal_node_right_child(parent);
    let right_child = get_page(&mut *table.pager, right_child_page_num as usize);

    if child_max_key > get_node_max_key(right_child) {
        // Replace right child
        *internal_node_child(parent, original_num_keys) = right_child_page_num;
        *internal_node_key(parent, original_num_keys) = get_node_max_key(right_child);
        *internal_node_right_child(parent) = child_page_num;
    } else {
        // Make room for the new cell
        for i in ((index + 1)..=original_num_keys).rev() {
            let destination = internal_node_cell(parent, i);
            let source = internal_node_cell(parent, i - 1);
            memcpy(
                destination as *mut c_void,
                source as *mut c_void,
                INTERNAL_NODE_CELL_SIZE,
            );
        }
        *internal_node_child(parent, index) = child_page_num;
        *internal_node_key(parent, index) = child_max_key;
    }
}
