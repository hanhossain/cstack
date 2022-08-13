use crate::pager::PAGE_SIZE;
use crate::serialization::{serialize_row, Row, ROW_SIZE};
use crate::storage::Storage;
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
//
// | common header | num keys | right child |
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
//
// | common header | num cells | next leaf |
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

#[derive(Debug)]
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

pub enum Node {
    Internal(InternalNode),
    Leaf(LeafNode),
}

impl Node {
    /// Gets the max key in the node.
    pub(crate) fn get_max_key(&self) -> u32 {
        match self {
            Node::Internal(node) => node.get_max_key(),
            Node::Leaf(node) => node.get_max_key(),
        }
    }

    pub fn unwrap_internal(self) -> InternalNode {
        match self {
            Node::Internal(node) => node,
            Node::Leaf(_) => panic!("Attempted to unwrap leaf node as internal node"),
        }
    }

    pub fn unwrap_leaf(self) -> LeafNode {
        match self {
            Node::Internal(_) => panic!("Attempted to unwrap internal node as leaf node"),
            Node::Leaf(node) => node,
        }
    }

    pub fn buffer(&self) -> &[u8] {
        match self {
            Node::Internal(node) => node.node.get_buffer(),
            Node::Leaf(node) => node.node.get_buffer(),
        }
    }

    pub fn buffer_mut(&mut self) -> &mut [u8] {
        match self {
            Node::Internal(node) => node.node.get_buffer_mut(),
            Node::Leaf(node) => node.node.get_buffer_mut(),
        }
    }

    #[allow(dead_code)]
    pub fn buffer_mut_ptr(&mut self) -> *mut u8 {
        match self {
            Node::Internal(node) => node.node.buffer,
            Node::Leaf(node) => node.node.buffer,
        }
    }

    #[allow(dead_code)]
    pub fn buffer_ptr(&self) -> *const u8 {
        match self {
            Node::Internal(node) => node.node.buffer,
            Node::Leaf(node) => node.node.buffer,
        }
    }

    pub fn set_root(&mut self, is_root: bool) {
        match self {
            Node::Internal(node) => node.node.set_root(is_root),
            Node::Leaf(node) => node.node.set_root(is_root),
        }
    }

    pub fn set_parent(&mut self, parent: u32) {
        match self {
            Node::Internal(node) => node.node.set_parent(parent),
            Node::Leaf(node) => node.node.set_parent(parent),
        }
    }
}

impl From<CommonNode> for Node {
    fn from(node: CommonNode) -> Self {
        match node.node_type() {
            NodeType::Internal => Node::Internal(InternalNode::from(node)),
            NodeType::Leaf => Node::Leaf(LeafNode::from(node)),
        }
    }
}

pub struct CommonNode {
    pub buffer: *mut u8,
}

impl CommonNode {
    /// Creates a CommonNode.
    pub fn new(buffer: *mut u8) -> CommonNode {
        CommonNode { buffer }
    }

    /// Gets the buffer as a slice.
    fn get_buffer(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.buffer, PAGE_SIZE) }
    }

    /// Gets the buffer as a mut slice.
    fn get_buffer_mut(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.buffer, PAGE_SIZE) }
    }

    /// Gets the node type.
    pub fn node_type(&self) -> NodeType {
        let buffer = self.get_buffer();
        let header: NodeHeader = bincode::deserialize(buffer).unwrap();
        NodeType::from(header.r#type)
    }

    /// Sets the node type.
    fn set_node_type(&mut self, node_type: NodeType) {
        let buffer = self.get_buffer_mut();
        let mut header: NodeHeader = bincode::deserialize(buffer).unwrap();
        header.r#type = u8::from(node_type);
        bincode::serialize_into(buffer, &header).unwrap();
    }

    /// Gets whether this node is the root.
    fn is_root(&self) -> bool {
        let buffer = self.get_buffer();
        let header: NodeHeader = bincode::deserialize(buffer).unwrap();
        header.is_root != 0
    }

    /// Sets whether this node is the root.
    pub fn set_root(&mut self, is_root: bool) {
        let buffer = self.get_buffer_mut();
        let value = if is_root { 1 } else { 0 };
        let mut header: NodeHeader = bincode::deserialize(buffer).unwrap();
        header.is_root = value;
        bincode::serialize_into(buffer, &header).unwrap();
    }

    /// Gets the location for the parent node.
    fn parent(&self) -> u32 {
        let buffer = self.get_buffer();
        let header: NodeHeader = bincode::deserialize(buffer).unwrap();
        header.parent
    }

    /// Sets the location for the parent node.
    pub(crate) fn set_parent(&mut self, parent: u32) {
        let buffer = self.get_buffer_mut();
        let mut header: NodeHeader = bincode::deserialize(buffer).unwrap();
        header.parent = parent;
        bincode::serialize_into(buffer, &header).unwrap();
    }
}

pub struct InternalNode {
    pub node: CommonNode,
}

impl From<CommonNode> for InternalNode {
    fn from(node: CommonNode) -> Self {
        InternalNode { node }
    }
}

impl InternalNode {
    // TODO: this should just be part of the new or from method
    /// Initializes the internal node.
    pub fn initialize(&mut self) {
        self.node.set_node_type(NodeType::Internal);
        self.node.set_root(false);
        self.set_num_keys(0);
    }

    /// Gets the number of keys in the node.
    pub fn num_keys(&self) -> u32 {
        unsafe { *(self.node.buffer.add(INTERNAL_NODE_NUM_KEYS_OFFSET) as *mut u32) }
    }

    /// Sets the number of keys in the node;
    pub fn set_num_keys(&mut self, num_keys: u32) {
        unsafe {
            *(self.node.buffer.add(INTERNAL_NODE_NUM_KEYS_OFFSET) as *mut u32) = num_keys;
        }
    }

    /// Gets the location of the right child.
    pub fn right_child(&self) -> u32 {
        unsafe { *(self.node.buffer.add(INTERNAL_NODE_RIGHT_CHILD_OFFSET) as *mut u32) }
    }

    /// Sets the location of the right child.
    pub fn set_right_child(&mut self, right_child: u32) {
        unsafe {
            *(self.node.buffer.add(INTERNAL_NODE_RIGHT_CHILD_OFFSET) as *mut u32) = right_child;
        }
    }

    /// Gets the location of the specific node cell.
    fn cell(&self, cell_num: u32) -> u32 {
        unsafe {
            *(self
                .node
                .buffer
                .add(INTERNAL_NODE_HEADER_SIZE + cell_num as usize * INTERNAL_NODE_CELL_SIZE)
                as *mut u32)
        }
    }

    /// Sets the location of the specific node cell.
    fn set_cell(&mut self, cell_num: u32, cell: u32) {
        unsafe {
            *(self
                .node
                .buffer
                .add(INTERNAL_NODE_HEADER_SIZE + cell_num as usize * INTERNAL_NODE_CELL_SIZE)
                as *mut u32) = cell;
        }
    }

    /// Gets the location of the specific child.
    pub fn child(&self, child_num: u32) -> u32 {
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

    /// Sets the location of the specific child.
    pub fn set_child(&mut self, child_num: u32, child: u32) {
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

    pub fn key(&self, key_num: u32) -> u32 {
        unsafe {
            let internal_node_cell = self
                .node
                .buffer
                .add(INTERNAL_NODE_HEADER_SIZE + key_num as usize * INTERNAL_NODE_CELL_SIZE)
                as *mut u32;
            *(internal_node_cell.add(INTERNAL_NODE_CHILD_SIZE))
        }
    }

    pub fn set_key(&mut self, key_num: u32, key: u32) {
        unsafe {
            let internal_node_cell = self
                .node
                .buffer
                .add(INTERNAL_NODE_HEADER_SIZE + key_num as usize * INTERNAL_NODE_CELL_SIZE)
                as *mut u32;
            *(internal_node_cell.add(INTERNAL_NODE_CHILD_SIZE)) = key;
        }
    }

    /// Returns the index of the child which should contain the given key.
    fn find_child(&self, key: u32) -> u32 {
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

    fn update_key(&mut self, old_key: u32, new_key: u32) {
        let old_child_index = self.find_child(old_key);
        self.set_key(old_child_index, new_key);
    }

    /// Gets the max key in the node.
    pub fn get_max_key(&self) -> u32 {
        self.key(self.num_keys() - 1)
    }

    pub fn find<T: Storage>(&self, table: &mut Table<T>, key: u32) -> Cursor<T> {
        let child_index = self.find_child(key);
        let child_num = self.child(child_index);
        let child = table.pager.page(child_num as usize);
        match child {
            Node::Leaf(leaf) => leaf.find(table, key),
            Node::Internal(internal) => internal.find(table, key),
        }
    }

    /// Add a child/key pair to node.
    fn insert<T: Storage>(&mut self, table: &mut Table<T>, child_page_num: u32) {
        let pager = &mut table.pager;
        let child = pager.page(child_page_num as usize);
        let child_max_key = child.get_max_key();

        let index = self.find_child(child_max_key);
        let original_num_keys = self.num_keys();
        self.set_num_keys(original_num_keys + 1);

        if original_num_keys as usize >= INTERNAL_NODE_MAX_CELLS {
            panic!("Need to implement splitting internal node");
        }

        let right_child_page_num = self.right_child();
        let right_child = pager.page(right_child_page_num as usize);
        if child_max_key > right_child.get_max_key() {
            // Replace right child
            self.set_child(original_num_keys, right_child_page_num);
            self.set_key(original_num_keys, right_child.get_max_key());
            self.set_right_child(child_page_num);
        } else {
            // Make room for the new cell
            for i in ((index + 1)..=original_num_keys).rev() {
                let destination = self.cell(i);
                let source = self.cell(i - 1);
                unsafe {
                    memcpy(
                        destination as *mut c_void,
                        source as *mut c_void,
                        INTERNAL_NODE_CELL_SIZE,
                    );
                }
            }
            self.set_child(index, child_page_num);
            self.set_key(index, child_max_key);
        }
    }
}

pub struct LeafNode {
    pub node: CommonNode,
}

impl From<CommonNode> for LeafNode {
    fn from(node: CommonNode) -> Self {
        LeafNode { node }
    }
}

impl LeafNode {
    /// Get the number of cells currently occupied in the node.
    pub fn num_cells(&self) -> u32 {
        unsafe { *(self.node.buffer.add(LEAF_NODE_NUM_CELLS_OFFSET) as *mut u32) }
    }

    /// Set the number of cells currently occupied in the node.
    pub fn set_num_cells(&mut self, num_cells: u32) {
        unsafe {
            *(self.node.buffer.add(LEAF_NODE_NUM_CELLS_OFFSET) as *mut u32) = num_cells;
        }
    }

    /// Get the pointer to the leaf node cell.
    fn cell(&self, cell_num: u32) -> *mut u8 {
        unsafe {
            self.node
                .buffer
                .add(LEAF_NODE_HEADER_SIZE + cell_num as usize * LEAF_NODE_CELL_SIZE)
        }
    }

    pub fn key(&self, cell_num: u32) -> u32 {
        unsafe { *(self.cell(cell_num) as *mut u32) }
    }

    pub fn set_key(&mut self, cell_num: u32, key: u32) {
        unsafe {
            *(self.cell(cell_num) as *mut u32) = key;
        }
    }

    /// Get the pointer to the leaf node value.
    pub fn value(&mut self, cell_num: u32) -> *mut u8 {
        unsafe { self.cell(cell_num).add(LEAF_NODE_KEY_SIZE) }
    }

    /// Gets the location of the next leaf.
    pub fn next_leaf(&self) -> u32 {
        unsafe { *(self.node.buffer.add(LEAF_NODE_NEXT_LEAF_OFFSET) as *mut u32) }
    }

    /// Sets the location of the next leaf.
    pub fn set_next_leaf(&mut self, next_leaf: u32) {
        unsafe {
            *(self.node.buffer.add(LEAF_NODE_NEXT_LEAF_OFFSET) as *mut u32) = next_leaf;
        }
    }

    pub fn initialize(&mut self) {
        self.node.set_node_type(NodeType::Leaf);
        self.node.set_root(false);
        self.set_num_cells(0);
        self.set_next_leaf(0); // 0 represents no sibling
    }

    /// Gets the max key in the node.
    pub fn get_max_key(&self) -> u32 {
        self.key(self.num_cells() - 1)
    }

    pub fn find<T>(self, table: &mut Table<T>, key: u32) -> Cursor<T> {
        let num_cells = self.num_cells();

        // Binary search
        let mut min_index = 0;
        let mut one_past_max_index = num_cells;
        while one_past_max_index != min_index {
            let index = (min_index + one_past_max_index) / 2;
            let key_at_index = self.key(index);
            if key == key_at_index {
                return Cursor {
                    table,
                    cell_num: index,
                    end_of_table: false,
                    node: self,
                };
            } else if key < key_at_index {
                one_past_max_index = index;
            } else {
                min_index = index + 1;
            }
        }

        Cursor {
            table,
            cell_num: min_index,
            end_of_table: false,
            node: self,
        }
    }
}

fn leaf_node_split_and_insert<T: Storage>(cursor: Cursor<T>, key: u32, value: &Row) {
    // Create a new node and move half the cells over.
    // Insert the new value in one of the two nodes.
    // Update parent or create a new parent.
    let table = unsafe { &mut *cursor.table };
    let pager = &mut table.pager;
    let mut old_node = cursor.node;
    let old_max = old_node.get_max_key();
    let new_page_num = pager.get_unused_page_num();
    let mut new_node = pager.new_leaf_page(new_page_num as usize);
    new_node.node.set_parent(old_node.node.parent());
    new_node.set_next_leaf(old_node.next_leaf());
    old_node.set_next_leaf(new_page_num);

    // All existing keys plus new key should be divided
    // evenly between old (left) and new (right) nodes.
    // Starting from the right, move each key to correct position.
    for i in (0..=LEAF_NODE_MAX_CELLS as i32).rev() {
        let destination_node = if i >= LEAF_NODE_LEFT_SPLIT_COUNT as i32 {
            &mut new_node
        } else {
            &mut old_node
        };
        let index_within_node = i % LEAF_NODE_LEFT_SPLIT_COUNT as i32;
        let destination = destination_node.cell(index_within_node as u32);

        unsafe {
            if i == cursor.cell_num as i32 {
                serialize_row(value, destination_node.value(index_within_node as u32));
                destination_node.set_key(index_within_node as u32, key);
            } else if i > cursor.cell_num as i32 {
                memcpy(
                    destination as *mut c_void,
                    old_node.cell((i - 1) as u32) as *mut c_void,
                    LEAF_NODE_CELL_SIZE,
                );
            } else {
                memcpy(
                    destination as *mut c_void,
                    old_node.cell(i as u32) as *mut c_void,
                    LEAF_NODE_CELL_SIZE,
                );
            }
        }
    }

    // Update cell count on both leaf nodes
    old_node.set_num_cells(LEAF_NODE_LEFT_SPLIT_COUNT as u32);
    new_node.set_num_cells(LEAF_NODE_RIGHT_SPLIT_COUNT as u32);

    unsafe {
        if old_node.node.is_root() {
            (&mut *cursor.table).create_new_root(new_page_num);
        } else {
            let parent_page_num = old_node.node.parent();
            let new_max = old_node.get_max_key();
            let mut parent = (&mut *cursor.table)
                .pager
                .page(parent_page_num as usize)
                .unwrap_internal();
            parent.update_key(old_max, new_max);
            parent.insert(&mut *cursor.table, new_page_num);
        }
    }
}

pub(crate) fn leaf_node_insert<T: Storage>(mut cursor: Cursor<T>, key: u32, value: &Row) {
    let num_cells = cursor.node.num_cells();
    if num_cells >= LEAF_NODE_MAX_CELLS as u32 {
        // Node full
        leaf_node_split_and_insert(cursor, key, value);
        return;
    }

    if cursor.cell_num < num_cells {
        // Make room for new cell
        for i in (cursor.cell_num + 1..=num_cells).rev() {
            unsafe {
                memcpy(
                    cursor.node.cell(i) as *mut c_void,
                    cursor.node.cell(i - 1) as *mut c_void,
                    LEAF_NODE_CELL_SIZE,
                );
            }
        }
    }

    cursor.node.set_num_cells(cursor.node.num_cells() + 1);
    cursor.node.set_key(cursor.cell_num, key);
    unsafe {
        serialize_row(value, cursor.node.value(cursor.cell_num));
    }
}
