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

pub struct CommonNode {
    pub buffer: *mut u8,
}

impl CommonNode {
    pub fn new(buffer: *mut u8) -> CommonNode {
        CommonNode { buffer }
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

    pub(crate) fn set_parent(&mut self, parent: u32) {
        let buffer = self.get_buffer_mut();
        let mut header: NodeHeader = bincode::deserialize(buffer).unwrap();
        header.parent = parent;
        bincode::serialize_into(buffer, &header).unwrap();
    }

    pub(crate) unsafe fn get_node_max_key(&self) -> u32 {
        match self.node_type() {
            NodeType::Internal => {
                let internal_node = InternalNode::new(self.buffer);
                internal_node.key(InternalNode::new(self.buffer).num_keys() - 1)
            }
            NodeType::Leaf => {
                let leaf_node = LeafNode::new(self.buffer);
                leaf_node.key(leaf_node.num_cells() - 1)
            }
        }
    }
}

pub struct InternalNode {
    pub buffer: *mut u8,
    pub node: CommonNode,
}

impl InternalNode {
    pub fn new(buffer: *mut u8) -> InternalNode {
        InternalNode {
            buffer,
            node: CommonNode::new(buffer),
        }
    }

    // TODO: this should just be part of the new or from method
    pub unsafe fn initialize(&mut self) {
        self.node.set_node_type(NodeType::Internal);
        self.node.set_root(false);
        self.set_num_keys(0);
    }

    pub unsafe fn num_keys(&self) -> u32 {
        *(self.buffer.add(INTERNAL_NODE_NUM_KEYS_OFFSET) as *mut u32)
    }

    pub unsafe fn set_num_keys(&mut self, num_keys: u32) {
        *(self.buffer.add(INTERNAL_NODE_NUM_KEYS_OFFSET) as *mut u32) = num_keys;
    }

    pub unsafe fn right_child(&self) -> u32 {
        *(self.buffer.add(INTERNAL_NODE_RIGHT_CHILD_OFFSET) as *mut u32)
    }

    pub unsafe fn set_right_child(&mut self, right_child: u32) {
        *(self.buffer.add(INTERNAL_NODE_RIGHT_CHILD_OFFSET) as *mut u32) = right_child;
    }

    unsafe fn cell(&self, cell_num: u32) -> u32 {
        *(self
            .buffer
            .add(INTERNAL_NODE_HEADER_SIZE + cell_num as usize * INTERNAL_NODE_CELL_SIZE)
            as *mut u32)
    }

    unsafe fn set_cell(&mut self, cell_num: u32, cell: u32) {
        *(self
            .buffer
            .add(INTERNAL_NODE_HEADER_SIZE + cell_num as usize * INTERNAL_NODE_CELL_SIZE)
            as *mut u32) = cell;
    }

    pub unsafe fn child(&self, child_num: u32) -> u32 {
        let num_keys = self.num_keys();
        if child_num > num_keys {
            println!("Tried to access child_num {child_num} > num_keys {num_keys}");
            exit(EXIT_FAILURE);
        }

        if child_num == num_keys {
            self.right_child()
        } else {
            self.cell(child_num)
        }
    }

    pub unsafe fn set_child(&mut self, child_num: u32, child: u32) {
        let num_keys = self.num_keys();
        if child_num > num_keys {
            println!("Tried to access child_num {child_num} > num_keys {num_keys}");
            exit(EXIT_FAILURE);
        }

        if child_num == num_keys {
            self.set_right_child(child);
        } else {
            self.set_cell(child_num, child);
        }
    }

    pub unsafe fn key(&self, key_num: u32) -> u32 {
        let internal_node_cell = self
            .buffer
            .add(INTERNAL_NODE_HEADER_SIZE + key_num as usize * INTERNAL_NODE_CELL_SIZE)
            as *mut u32;
        *(internal_node_cell.add(INTERNAL_NODE_CHILD_SIZE))
    }

    pub unsafe fn set_key(&mut self, key_num: u32, key: u32) {
        let internal_node_cell = self
            .buffer
            .add(INTERNAL_NODE_HEADER_SIZE + key_num as usize * INTERNAL_NODE_CELL_SIZE)
            as *mut u32;
        *(internal_node_cell.add(INTERNAL_NODE_CHILD_SIZE)) = key;
    }

    /// Returns the index of the child which should contain the given key.
    unsafe fn find_child(&self, key: u32) -> u32 {
        let num_keys = self.num_keys();

        // binary search
        let mut min_index = 0;
        let mut max_index = num_keys; // there is one more child than key

        while min_index != max_index {
            let index = (min_index + max_index) / 2;
            let key_to_right = self.key(index);
            if key_to_right >= key {
                max_index = index;
            } else {
                min_index = index + 1;
            }
        }

        min_index
    }

    unsafe fn update_key(&mut self, old_key: u32, new_key: u32) {
        let old_child_index = self.find_child(old_key);
        self.set_key(old_child_index, new_key);
    }
}

pub struct LeafNode {
    pub buffer: *mut u8,
    pub node: CommonNode,
}

impl LeafNode {
    pub fn new(buffer: *mut u8) -> LeafNode {
        LeafNode {
            buffer,
            node: CommonNode::new(buffer),
        }
    }

    pub unsafe fn num_cells(&self) -> u32 {
        *(self.buffer.add(LEAF_NODE_NUM_CELLS_OFFSET) as *mut u32)
    }

    pub unsafe fn set_num_cells(&mut self, num_cells: u32) {
        *(self.buffer.add(LEAF_NODE_NUM_CELLS_OFFSET) as *mut u32) = num_cells;
    }

    /// Get the pointer to the leaf node cell.
    unsafe fn cell(&self, cell_num: u32) -> *mut u8 {
        self.buffer
            .add(LEAF_NODE_HEADER_SIZE + cell_num as usize * LEAF_NODE_CELL_SIZE)
    }

    pub unsafe fn key(&self, cell_num: u32) -> u32 {
        *(self.cell(cell_num) as *mut u32)
    }

    pub unsafe fn set_key(&mut self, cell_num: u32, key: u32) {
        *(self.cell(cell_num) as *mut u32) = key;
    }

    /// Get the pointer to the leaf node value.
    pub unsafe fn value(&mut self, cell_num: u32) -> *mut u8 {
        self.cell(cell_num).add(LEAF_NODE_KEY_SIZE)
    }

    pub unsafe fn next_leaf(&self) -> u32 {
        *(self.buffer.add(LEAF_NODE_NEXT_LEAF_OFFSET) as *mut u32)
    }

    pub unsafe fn set_next_leaf(&mut self, next_leaf: u32) {
        *(self.buffer.add(LEAF_NODE_NEXT_LEAF_OFFSET) as *mut u32) = next_leaf;
    }

    pub unsafe fn initialize(&mut self) {
        self.node.set_node_type(NodeType::Leaf);
        self.node.set_root(false);
        self.set_num_cells(0);
        self.set_next_leaf(0); // 0 represents no sibling
    }
}

/// Add a child/key pair to parent that corresponds to child.
unsafe fn internal_node_insert(table: &mut Table, parent_page_num: u32, child_page_num: u32) {
    let pager = &mut table.pager;
    let parent = pager.get_page(parent_page_num as usize);
    let child = pager.get_page(child_page_num as usize);
    let child_max_key = child.get_node_max_key();
    let mut parent_internal_node = InternalNode::new(parent.buffer);

    let index = parent_internal_node.find_child(child_max_key);
    let original_num_keys = parent_internal_node.num_keys();
    parent_internal_node.set_num_keys(original_num_keys + 1);

    if original_num_keys as usize >= INTERNAL_NODE_MAX_CELLS {
        println!("Need to implement splitting internal node");
        exit(EXIT_FAILURE);
    }

    let right_child_page_num = parent_internal_node.right_child();
    let right_child = pager.get_page(right_child_page_num as usize);
    if child_max_key > right_child.get_node_max_key() {
        // Replace right child
        parent_internal_node.set_child(original_num_keys, right_child_page_num);
        parent_internal_node.set_key(original_num_keys, right_child.get_node_max_key());
        parent_internal_node.set_right_child(child_page_num);
    } else {
        // Make room for the new cell
        for i in ((index + 1)..=original_num_keys).rev() {
            let destination = parent_internal_node.cell(i);
            let source = parent_internal_node.cell(i - 1);
            memcpy(
                destination as *mut c_void,
                source as *mut c_void,
                INTERNAL_NODE_CELL_SIZE,
            );
        }
        parent_internal_node.set_child(index, child_page_num);
        parent_internal_node.set_key(index, child_max_key);
    }
}

pub(crate) unsafe fn leaf_node_find(table: &mut Table, page_num: u32, key: u32) -> Cursor {
    let node = table.pager.get_page(page_num as usize);
    let leaf_node = LeafNode::new(node.buffer);
    let num_cells = leaf_node.num_cells();

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
        let key_at_index = leaf_node.key(index);
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
    let internal_node = InternalNode::new(node.buffer);
    let child_index = internal_node.find_child(key);
    let child_num = internal_node.child(child_index);
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
    let mut old_leaf_node = LeafNode::new(old_node.buffer);
    let old_max = old_node.get_node_max_key();
    let new_page_num = pager.get_unused_page_num();
    let mut new_node = pager.get_page(new_page_num as usize);
    let mut new_leaf_node = LeafNode::new(new_node.buffer);

    new_leaf_node.initialize();
    new_node.set_parent(old_node.parent());
    new_leaf_node.set_next_leaf(old_leaf_node.next_leaf());
    old_leaf_node.set_next_leaf(new_page_num);

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
        let mut destination_leaf_node = LeafNode::new(destination_node);
        let destination = destination_leaf_node.cell(index_within_node as u32);
        let old_leaf_node = LeafNode::new(old_node.buffer);

        if i == cursor.cell_num as i32 {
            serialize_row(value, destination_leaf_node.value(index_within_node as u32));
            destination_leaf_node.set_key(index_within_node as u32, key);
        } else if i > cursor.cell_num as i32 {
            memcpy(
                destination as *mut c_void,
                old_leaf_node.cell((i - 1) as u32) as *mut c_void,
                LEAF_NODE_CELL_SIZE,
            );
        } else {
            memcpy(
                destination as *mut c_void,
                old_leaf_node.cell(i as u32) as *mut c_void,
                LEAF_NODE_CELL_SIZE,
            );
        }
    }

    // Update cell count on both leaf nodes
    LeafNode::new(old_node.buffer).set_num_cells(LEAF_NODE_LEFT_SPLIT_COUNT as u32);
    new_leaf_node.set_num_cells(LEAF_NODE_RIGHT_SPLIT_COUNT as u32);

    if old_node.is_root() {
        (&mut *cursor.table).create_new_root(new_page_num);
    } else {
        let parent_page_num = old_node.parent();
        let new_max = old_node.get_node_max_key();
        let parent = (&mut *cursor.table)
            .pager
            .get_page(parent_page_num as usize);
        InternalNode::new(parent.buffer).update_key(old_max, new_max);
        internal_node_insert(&mut *cursor.table, parent_page_num, new_page_num);
    }
}

pub(crate) unsafe fn leaf_node_insert(cursor: &mut Cursor, key: u32, value: &Row) {
    let node = (&mut *cursor.table)
        .pager
        .get_page(cursor.page_num as usize);

    let mut leaf_node = LeafNode::new(node.buffer);
    let num_cells = leaf_node.num_cells();
    if num_cells >= LEAF_NODE_MAX_CELLS as u32 {
        // Node full
        leaf_node_split_and_insert(cursor, key, value);
        return;
    }

    if cursor.cell_num < num_cells {
        // Make room for new cell
        for i in (cursor.cell_num + 1..=num_cells).rev() {
            memcpy(
                leaf_node.cell(i) as *mut c_void,
                leaf_node.cell(i - 1) as *mut c_void,
                LEAF_NODE_CELL_SIZE,
            );
        }
    }

    leaf_node.set_num_cells(leaf_node.num_cells() + 1);
    leaf_node.set_key(cursor.cell_num, key);
    serialize_row(value, leaf_node.value(cursor.cell_num));
}
