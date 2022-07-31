#![allow(non_camel_case_types)]
use crate::pager::{get_page, get_unused_page_num, PAGE_SIZE};
use crate::serialization::{serialize_row, Row, ROW_SIZE};
use crate::table::{Cursor, Table};
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

const LEAF_NODE_RIGHT_SPLIT_COUNT: usize = (LEAF_NODE_MAX_CELLS + 1) / 2;
const LEAF_NODE_LEFT_SPLIT_COUNT: usize = (LEAF_NODE_MAX_CELLS + 1) - LEAF_NODE_RIGHT_SPLIT_COUNT;

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

pub(crate) unsafe fn get_node_type(node: *const c_void) -> NodeType {
    let value = *(node.add(NODE_TYPE_OFFSET) as *const u8);
    NodeType::from(value)
}

unsafe fn set_node_type(node: *mut c_void, node_type: NodeType) {
    let value = u8::from(node_type);
    *(node.add(NODE_TYPE_OFFSET) as *mut u8) = value;
}

fn is_node_root(node: *const c_void) -> bool {
    let value = unsafe { *(node.add(IS_ROOT_OFFSET) as *const u8) };
    value != 0
}

pub(crate) unsafe fn set_node_root(node: *mut c_void, is_root: bool) {
    let value = if is_root { 1 } else { 0 };
    *(node.add(IS_ROOT_OFFSET) as *mut u8) = value;
}

unsafe fn node_parent(node: *mut c_void) -> *mut u32 {
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

pub(crate) unsafe fn internal_node_child(node: *mut c_void, child_num: u32) -> *mut u32 {
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

pub(crate) unsafe fn leaf_node_num_cells(node: *mut c_void) -> *mut u32 {
    node.add(LEAF_NODE_NUM_CELLS_OFFSET) as *mut u32
}

unsafe fn leaf_node_cell(node: *mut c_void, cell_num: u32) -> *mut c_void {
    node.add(LEAF_NODE_HEADER_SIZE + cell_num as usize * LEAF_NODE_CELL_SIZE)
}

pub(crate) unsafe fn leaf_node_key(node: *mut c_void, cell_num: u32) -> *mut u32 {
    leaf_node_cell(node, cell_num) as *mut u32
}

pub(crate) unsafe fn leaf_node_value(node: *mut c_void, cell_num: u32) -> *mut c_void {
    leaf_node_cell(node, cell_num).add(LEAF_NODE_KEY_SIZE)
}

pub(crate) unsafe fn leaf_node_next_leaf(node: *mut c_void) -> *mut u32 {
    node.add(LEAF_NODE_NEXT_LEAF_OFFSET) as *mut u32
}

unsafe fn get_node_max_key(node: *mut c_void) -> u32 {
    match get_node_type(node) {
        NodeType::NODE_INTERNAL => *internal_node_key(node, *internal_node_num_keys(node) - 1),
        NodeType::NODE_LEAF => *leaf_node_key(node, *leaf_node_num_cells(node) - 1),
    }
}

pub(crate) unsafe fn initialize_leaf_node(node: *mut c_void) {
    set_node_type(node, NodeType::NODE_LEAF);
    set_node_root(node, false);
    *leaf_node_num_cells(node) = 0;
    *leaf_node_next_leaf(node) = 0; // 0 represents no sibling
}

/// Returns the index of the child which should contain the given key.
unsafe fn internal_node_find_child(node: *mut c_void, key: u32) -> u32 {
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
unsafe fn create_new_root(table: &mut Table, right_child_page_num: u32) {
    let pager = &mut table.pager;
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

unsafe fn update_internal_node_key(node: *mut c_void, old_key: u32, new_key: u32) {
    let old_child_index = internal_node_find_child(node, old_key);
    *internal_node_key(node, old_child_index) = new_key;
}

/// Add a child/key pair to parent that corresponds to child.
unsafe fn internal_node_insert(table: &mut Table, parent_page_num: u32, child_page_num: u32) {
    let pager = &mut table.pager;
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
    let right_child = get_page(pager, right_child_page_num as usize);

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

pub(crate) unsafe fn leaf_node_find(table: &mut Table, page_num: u32, key: u32) -> *mut Cursor {
    let node = get_page(&mut table.pager, page_num as usize);
    let num_cells = *leaf_node_num_cells(node);

    let mut cursor = Box::new(Cursor {
        table: table as *mut Table,
        page_num,
        cell_num: 0,
        end_of_table: false,
    });

    // Binary search
    let mut min_index = 0;
    let mut one_past_max_index = num_cells;
    while one_past_max_index != min_index {
        let index = (min_index + one_past_max_index) / 2;
        let key_at_index = *leaf_node_key(node, index);
        if key == key_at_index {
            cursor.cell_num = index;
            return Box::into_raw(cursor);
        } else if key < key_at_index {
            one_past_max_index = index;
        } else {
            min_index = index + 1;
        }
    }

    cursor.cell_num = min_index;
    Box::into_raw(cursor)
}

pub(crate) unsafe fn internal_node_find(table: &mut Table, page_num: u32, key: u32) -> *mut Cursor {
    let node = get_page(&mut table.pager, page_num as usize);
    let child_index = internal_node_find_child(node, key);
    let child_num = *internal_node_child(node, child_index);
    let child = get_page(&mut table.pager, child_num as usize);
    match get_node_type(child) {
        NodeType::NODE_LEAF => leaf_node_find(table, child_num, key),
        NodeType::NODE_INTERNAL => internal_node_find(table, child_num, key),
    }
}

unsafe fn leaf_node_split_and_insert(cursor: &mut Cursor, key: u32, value: &Row) {
    // Create a new node and move half the cells over.
    // Insert the new value in one of the two nodes.
    // Update parent or create a new parent.
    let table = &mut *cursor.table;
    let pager = &mut table.pager;
    let old_node = get_page(pager, cursor.page_num as usize);
    let old_max = get_node_max_key(old_node);
    let new_page_num = get_unused_page_num(pager);
    let new_node = get_page(pager, new_page_num as usize);
    initialize_leaf_node(new_node);
    *node_parent(new_node) = *node_parent(old_node);
    *leaf_node_next_leaf(new_node) = *leaf_node_next_leaf(old_node);
    *leaf_node_next_leaf(old_node) = new_page_num;

    // All existing keys plus new key should be divided
    // evenly between old (left) and new (right) nodes.
    // Starting from the right, move each key to correct position.
    for i in (0..=LEAF_NODE_MAX_CELLS as i32).rev() {
        let destination_node = if i >= LEAF_NODE_LEFT_SPLIT_COUNT as i32 {
            new_node
        } else {
            old_node
        };
        let index_within_node = i % LEAF_NODE_LEFT_SPLIT_COUNT as i32;
        let destination = leaf_node_cell(destination_node, index_within_node as u32);

        if i == cursor.cell_num as i32 {
            serialize_row(
                value,
                leaf_node_value(destination_node, index_within_node as u32),
            );
            *leaf_node_key(destination_node, index_within_node as u32) = key;
        } else if i > cursor.cell_num as i32 {
            memcpy(
                destination,
                leaf_node_cell(old_node, (i - 1) as u32),
                LEAF_NODE_CELL_SIZE,
            );
        } else {
            memcpy(
                destination,
                leaf_node_cell(old_node, i as u32),
                LEAF_NODE_CELL_SIZE,
            );
        }
    }

    // Update cell count on both leaf nodes
    *leaf_node_num_cells(old_node) = LEAF_NODE_LEFT_SPLIT_COUNT as u32;
    *leaf_node_num_cells(new_node) = LEAF_NODE_RIGHT_SPLIT_COUNT as u32;

    if is_node_root(old_node) {
        create_new_root(&mut *cursor.table, new_page_num);
    } else {
        let parent_page_num = *node_parent(old_node);
        let new_max = get_node_max_key(old_node);
        let parent = get_page(&mut (&mut *cursor.table).pager, parent_page_num as usize);
        update_internal_node_key(parent, old_max, new_max);
        internal_node_insert(&mut *cursor.table, parent_page_num, new_page_num);
    }
}

pub(crate) unsafe fn leaf_node_insert(cursor: &mut Cursor, key: u32, value: &Row) {
    let node = get_page(&mut (&mut *cursor.table).pager, cursor.page_num as usize);

    let num_cells = *leaf_node_num_cells(node);
    if num_cells >= LEAF_NODE_MAX_CELLS as u32 {
        // Node full
        leaf_node_split_and_insert(cursor, key, value);
        return;
    }

    if cursor.cell_num < num_cells {
        // Make room for new cell
        for i in (cursor.cell_num + 1..=num_cells).rev() {
            memcpy(
                leaf_node_cell(node, i),
                leaf_node_cell(node, i - 1),
                LEAF_NODE_CELL_SIZE,
            );
        }
    }

    *leaf_node_num_cells(node) += 1;
    *leaf_node_key(node, cursor.cell_num) = key;
    serialize_row(value, leaf_node_value(node, cursor.cell_num));
}
