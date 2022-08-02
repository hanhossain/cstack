use crate::pager::PAGE_SIZE;
use crate::serialization::{serialize_row, Row, ROW_SIZE};
use crate::table::{Cursor, Table};
use libc::{memcpy, EXIT_FAILURE};
use serde::{Deserialize, Serialize};
use std::ffi::c_void;
use std::mem::size_of;
use std::process::exit;

// Common Node Header Layout
pub(crate) const COMMON_NODE_HEADER_SIZE: usize = size_of::<NodeHeader>();

#[derive(Serialize, Deserialize)]
struct NodeHeader {
    r#type: u8,
    is_root: u8,
    parent: u32,
}

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

pub enum NodeType {
    Internal,
    Leaf,
}

impl From<u8> for NodeType {
    fn from(value: u8) -> Self {
        match value {
            0 => NodeType::Internal,
            1 => NodeType::Leaf,
            _ => unreachable!(),
        }
    }
}

impl From<NodeType> for u8 {
    fn from(node_type: NodeType) -> Self {
        match node_type {
            NodeType::Internal => 0,
            NodeType::Leaf => 1,
        }
    }
}

pub struct Node {
    pub buffer: *mut u8,
}

impl Node {
    pub fn new(buffer: *mut u8) -> Node {
        Node { buffer }
    }

    fn get_buffer(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.buffer, PAGE_SIZE) }
    }

    fn get_buffer_mut(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.buffer, PAGE_SIZE) }
    }

    pub fn node_type(&self) -> NodeType {
        let buffer = self.get_buffer();
        let header: NodeHeader = bincode::deserialize(buffer).unwrap();
        NodeType::from(header.r#type)
    }

    fn set_node_type(&mut self, node_type: NodeType) {
        let buffer = self.get_buffer_mut();
        let mut header: NodeHeader = bincode::deserialize(buffer).unwrap();
        header.r#type = u8::from(node_type);
        bincode::serialize_into(buffer, &header).unwrap();
    }

    fn is_root(&self) -> bool {
        let buffer = self.get_buffer();
        let header: NodeHeader = bincode::deserialize(buffer).unwrap();
        header.is_root != 0
    }

    pub fn set_root(&mut self, is_root: bool) {
        let buffer = self.get_buffer_mut();
        let value = if is_root { 1 } else { 0 };
        let mut header: NodeHeader = bincode::deserialize(buffer).unwrap();
        header.is_root = value;
        bincode::serialize_into(buffer, &header).unwrap();
    }

    fn parent(&self) -> u32 {
        let buffer = self.get_buffer();
        let header: NodeHeader = bincode::deserialize(buffer).unwrap();
        header.parent
    }

    fn set_parent(&mut self, parent: u32) {
        let buffer = self.get_buffer_mut();
        let mut header: NodeHeader = bincode::deserialize(buffer).unwrap();
        header.parent = parent;
        bincode::serialize_into(buffer, &header).unwrap();
    }

    unsafe fn get_node_max_key(&self) -> u32 {
        match self.node_type() {
            NodeType::Internal => {
                internal_node_key(self.buffer, internal_node_num_keys(self.buffer) - 1)
            }
            NodeType::Leaf => leaf_node_key(self.buffer, leaf_node_num_cells(self.buffer) - 1),
        }
    }
}

pub unsafe fn internal_node_num_keys(node: *mut u8) -> u32 {
    *(node.add(INTERNAL_NODE_NUM_KEYS_OFFSET) as *mut u32)
}

pub unsafe fn set_internal_node_num_keys(node: *mut u8, num_keys: u32) {
    *(node.add(INTERNAL_NODE_NUM_KEYS_OFFSET) as *mut u32) = num_keys;
}

pub unsafe fn internal_node_right_child(node: *mut u8) -> u32 {
    *(node.add(INTERNAL_NODE_RIGHT_CHILD_OFFSET) as *mut u32)
}

pub unsafe fn set_internal_node_right_child(node: *mut u8, right_child: u32) {
    *(node.add(INTERNAL_NODE_RIGHT_CHILD_OFFSET) as *mut u32) = right_child;
}

unsafe fn internal_node_cell(node: *mut u8, cell_num: u32) -> u32 {
    *(node.add(INTERNAL_NODE_HEADER_SIZE + cell_num as usize * INTERNAL_NODE_CELL_SIZE) as *mut u32)
}

unsafe fn set_internal_node_cell(node: *mut u8, cell_num: u32, cell: u32) {
    *(node.add(INTERNAL_NODE_HEADER_SIZE + cell_num as usize * INTERNAL_NODE_CELL_SIZE)
        as *mut u32) = cell;
}

unsafe fn initialize_internal_node(buffer: *mut u8) {
    let mut node = Node::new(buffer);
    node.set_node_type(NodeType::Internal);
    node.set_root(false);
    set_internal_node_num_keys(buffer, 0);
}

pub unsafe fn internal_node_child(node: *mut u8, child_num: u32) -> u32 {
    let num_keys = internal_node_num_keys(node);
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

pub unsafe fn set_internal_node_child(node: *mut u8, child_num: u32, child: u32) {
    let num_keys = internal_node_num_keys(node);
    if child_num > num_keys {
        println!("Tried to access child_num {child_num} > num_keys {num_keys}");
        exit(EXIT_FAILURE);
    }

    if child_num == num_keys {
        set_internal_node_right_child(node, child)
    } else {
        set_internal_node_cell(node, child_num, child);
    }
}

pub unsafe fn internal_node_key(node: *mut u8, key_num: u32) -> u32 {
    let internal_node_cell = node
        .add(INTERNAL_NODE_HEADER_SIZE + key_num as usize * INTERNAL_NODE_CELL_SIZE)
        as *mut u32;
    *(internal_node_cell.add(INTERNAL_NODE_CHILD_SIZE))
}

unsafe fn set_internal_node_key(node: *mut u8, key_num: u32, key: u32) {
    let internal_node_cell = node
        .add(INTERNAL_NODE_HEADER_SIZE + key_num as usize * INTERNAL_NODE_CELL_SIZE)
        as *mut u32;
    *(internal_node_cell.add(INTERNAL_NODE_CHILD_SIZE)) = key;
}

pub unsafe fn leaf_node_num_cells(node: *mut u8) -> u32 {
    *(node.add(LEAF_NODE_NUM_CELLS_OFFSET) as *mut u32)
}

pub unsafe fn set_leaf_node_num_cells(node: *mut u8, num_cells: u32) {
    *(node.add(LEAF_NODE_NUM_CELLS_OFFSET) as *mut u32) = num_cells;
}

/// Get the pointer to the leaf node cell.
unsafe fn leaf_node_cell(node: *mut u8, cell_num: u32) -> *mut u8 {
    node.add(LEAF_NODE_HEADER_SIZE + cell_num as usize * LEAF_NODE_CELL_SIZE)
}

pub unsafe fn leaf_node_key(node: *mut u8, cell_num: u32) -> u32 {
    *(leaf_node_cell(node, cell_num) as *mut u32)
}

pub unsafe fn set_leaf_node_key(node: *mut u8, cell_num: u32, key: u32) {
    *(leaf_node_cell(node, cell_num) as *mut u32) = key;
}

/// Get the pointer to the leaf node value.
pub unsafe fn leaf_node_value<'a>(node: *mut u8, cell_num: u32) -> *mut u8 {
    leaf_node_cell(node, cell_num).add(LEAF_NODE_KEY_SIZE)
}

pub unsafe fn leaf_node_next_leaf(node: *mut u8) -> u32 {
    *(node.add(LEAF_NODE_NEXT_LEAF_OFFSET) as *mut u32)
}

pub unsafe fn set_leaf_node_next_leaf(node: *mut u8, next_leaf: u32) {
    *(node.add(LEAF_NODE_NEXT_LEAF_OFFSET) as *mut u32) = next_leaf;
}

pub(crate) unsafe fn initialize_leaf_node(buffer: *mut u8) {
    let mut node = Node::new(buffer);
    node.set_node_type(NodeType::Leaf);
    node.set_root(false);
    set_leaf_node_num_cells(buffer, 0);
    set_leaf_node_next_leaf(buffer, 0); // 0 represents no sibling
}

/// Returns the index of the child which should contain the given key.
unsafe fn internal_node_find_child(node: *mut u8, key: u32) -> u32 {
    let num_keys = internal_node_num_keys(node);

    // binary search
    let mut min_index = 0;
    let mut max_index = num_keys; // there is one more child than key

    while min_index != max_index {
        let index = (min_index + max_index) / 2;
        let key_to_right = internal_node_key(node, index);
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
    let mut root = pager.get_page(table.root_page_num as usize);
    let mut right_child = pager.get_page(right_child_page_num as usize);
    let left_child_page_num = pager.get_unused_page_num();
    let mut left_child = pager.get_page(left_child_page_num as usize);

    // Left child has data copied from old root
    memcpy(
        left_child.buffer as *mut c_void,
        root.buffer as *mut c_void,
        PAGE_SIZE,
    );
    left_child.set_root(false);

    // Root node is a new internal node with one key and two children
    initialize_internal_node(root.buffer);
    root.set_root(true);
    set_internal_node_num_keys(root.buffer, 1);
    set_internal_node_child(root.buffer, 0, left_child_page_num);
    let left_child_max_key = left_child.get_node_max_key();
    set_internal_node_key(root.buffer, 0, left_child_max_key);
    set_internal_node_right_child(root.buffer, right_child_page_num);
    left_child.set_parent(table.root_page_num);
    right_child.set_parent(table.root_page_num);
}

unsafe fn update_internal_node_key(node: *mut u8, old_key: u32, new_key: u32) {
    let old_child_index = internal_node_find_child(node, old_key);
    set_internal_node_key(node, old_child_index, new_key);
}

/// Add a child/key pair to parent that corresponds to child.
unsafe fn internal_node_insert(table: &mut Table, parent_page_num: u32, child_page_num: u32) {
    let pager = &mut table.pager;
    let parent = pager.get_page(parent_page_num as usize);
    let child = pager.get_page(child_page_num as usize);
    let child_max_key = child.get_node_max_key();
    let index = internal_node_find_child(parent.buffer, child_max_key);

    let original_num_keys = internal_node_num_keys(parent.buffer);
    set_internal_node_num_keys(parent.buffer, original_num_keys + 1);

    if original_num_keys as usize >= INTERNAL_NODE_MAX_CELLS {
        println!("Need to implement splitting internal node");
        exit(EXIT_FAILURE);
    }

    let right_child_page_num = internal_node_right_child(parent.buffer);
    let right_child = pager.get_page(right_child_page_num as usize);
    if child_max_key > right_child.get_node_max_key() {
        // Replace right child
        set_internal_node_child(parent.buffer, original_num_keys, right_child_page_num);
        set_internal_node_key(
            parent.buffer,
            original_num_keys,
            right_child.get_node_max_key(),
        );
        set_internal_node_right_child(parent.buffer, child_page_num);
    } else {
        // Make room for the new cell
        for i in ((index + 1)..=original_num_keys).rev() {
            let destination = internal_node_cell(parent.buffer, i);
            let source = internal_node_cell(parent.buffer, i - 1);
            memcpy(
                destination as *mut c_void,
                source as *mut c_void,
                INTERNAL_NODE_CELL_SIZE,
            );
        }
        set_internal_node_child(parent.buffer, index, child_page_num);
        set_internal_node_key(parent.buffer, index, child_max_key);
    }
}

pub(crate) unsafe fn leaf_node_find(table: &mut Table, page_num: u32, key: u32) -> Cursor {
    let node = table.pager.get_page(page_num as usize);
    let num_cells = leaf_node_num_cells(node.buffer);

    let mut cursor = Cursor {
        table,
        page_num,
        cell_num: 0,
        end_of_table: false,
    };

    // Binary search
    let mut min_index = 0;
    let mut one_past_max_index = num_cells;
    while one_past_max_index != min_index {
        let index = (min_index + one_past_max_index) / 2;
        let key_at_index = leaf_node_key(node.buffer, index);
        if key == key_at_index {
            cursor.cell_num = index;
            return cursor;
        } else if key < key_at_index {
            one_past_max_index = index;
        } else {
            min_index = index + 1;
        }
    }

    cursor.cell_num = min_index;
    cursor
}

pub(crate) unsafe fn internal_node_find(table: &mut Table, page_num: u32, key: u32) -> Cursor {
    let node = table.pager.get_page(page_num as usize);
    let child_index = internal_node_find_child(node.buffer, key);
    let child_num = internal_node_child(node.buffer, child_index);
    let child = table.pager.get_page(child_num as usize);
    match child.node_type() {
        NodeType::Leaf => leaf_node_find(table, child_num, key),
        NodeType::Internal => internal_node_find(table, child_num, key),
    }
}

unsafe fn leaf_node_split_and_insert(cursor: &mut Cursor, key: u32, value: &Row) {
    // Create a new node and move half the cells over.
    // Insert the new value in one of the two nodes.
    // Update parent or create a new parent.
    let table = &mut *cursor.table;
    let pager = &mut table.pager;
    let old_node = pager.get_page(cursor.page_num as usize);
    let old_max = old_node.get_node_max_key();
    let new_page_num = pager.get_unused_page_num();
    let mut new_node = pager.get_page(new_page_num as usize);
    initialize_leaf_node(new_node.buffer);
    new_node.set_parent(old_node.parent());
    set_leaf_node_next_leaf(new_node.buffer, leaf_node_next_leaf(old_node.buffer));
    set_leaf_node_next_leaf(old_node.buffer, new_page_num);

    // All existing keys plus new key should be divided
    // evenly between old (left) and new (right) nodes.
    // Starting from the right, move each key to correct position.
    for i in (0..=LEAF_NODE_MAX_CELLS as i32).rev() {
        let destination_node = if i >= LEAF_NODE_LEFT_SPLIT_COUNT as i32 {
            new_node.buffer
        } else {
            old_node.buffer
        };
        let index_within_node = i % LEAF_NODE_LEFT_SPLIT_COUNT as i32;
        let destination = leaf_node_cell(destination_node, index_within_node as u32);

        if i == cursor.cell_num as i32 {
            serialize_row(
                value,
                leaf_node_value(destination_node, index_within_node as u32),
            );
            set_leaf_node_key(destination_node, index_within_node as u32, key);
        } else if i > cursor.cell_num as i32 {
            memcpy(
                destination as *mut c_void,
                leaf_node_cell(old_node.buffer, (i - 1) as u32) as *mut c_void,
                LEAF_NODE_CELL_SIZE,
            );
        } else {
            memcpy(
                destination as *mut c_void,
                leaf_node_cell(old_node.buffer, i as u32) as *mut c_void,
                LEAF_NODE_CELL_SIZE,
            );
        }
    }

    // Update cell count on both leaf nodes
    set_leaf_node_num_cells(old_node.buffer, LEAF_NODE_LEFT_SPLIT_COUNT as u32);
    set_leaf_node_num_cells(new_node.buffer, LEAF_NODE_RIGHT_SPLIT_COUNT as u32);

    if old_node.is_root() {
        create_new_root(&mut *cursor.table, new_page_num);
    } else {
        let parent_page_num = old_node.parent();
        let new_max = old_node.get_node_max_key();
        let parent = (&mut *cursor.table)
            .pager
            .get_page(parent_page_num as usize);
        update_internal_node_key(parent.buffer, old_max, new_max);
        internal_node_insert(&mut *cursor.table, parent_page_num, new_page_num);
    }
}

pub(crate) unsafe fn leaf_node_insert(cursor: &mut Cursor, key: u32, value: &Row) {
    let node = (&mut *cursor.table)
        .pager
        .get_page(cursor.page_num as usize);

    let num_cells = leaf_node_num_cells(node.buffer);
    if num_cells >= LEAF_NODE_MAX_CELLS as u32 {
        // Node full
        leaf_node_split_and_insert(cursor, key, value);
        return;
    }

    if cursor.cell_num < num_cells {
        // Make room for new cell
        for i in (cursor.cell_num + 1..=num_cells).rev() {
            memcpy(
                leaf_node_cell(node.buffer, i) as *mut c_void,
                leaf_node_cell(node.buffer, i - 1) as *mut c_void,
                LEAF_NODE_CELL_SIZE,
            );
        }
    }

    set_leaf_node_num_cells(node.buffer, leaf_node_num_cells(node.buffer) + 1);
    set_leaf_node_key(node.buffer, cursor.cell_num, key);
    serialize_row(value, leaf_node_value(node.buffer, cursor.cell_num));
}
