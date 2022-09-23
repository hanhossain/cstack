use crate::node::common;
use crate::node::common::CommonNode;
use crate::node::{Node, NodeType};
use crate::storage::Storage;
use crate::table::Cursor;
use crate::Table;
use libc::{memcpy, EXIT_FAILURE};
use std::ffi::c_void;
use std::mem::size_of;
use std::process::exit;

// Internal Node Header Layout
//
// | common header | num keys | right child |
const NUM_KEYS_SIZE: usize = size_of::<u32>();
const NUM_KEYS_OFFSET: usize = common::HEADER_SIZE;
const RIGHT_CHILD_SIZE: usize = size_of::<u32>();
const RIGHT_CHILD_OFFSET: usize = NUM_KEYS_OFFSET + NUM_KEYS_SIZE;
const HEADER_SIZE: usize = common::HEADER_SIZE + NUM_KEYS_SIZE + RIGHT_CHILD_SIZE;

// Internal Node Body Layout
const KEY_SIZE: usize = size_of::<u32>();
const CHILD_SIZE: usize = size_of::<u32>();
const CELL_SIZE: usize = CHILD_SIZE + KEY_SIZE;

// Internal Node Body Layout
const MAX_CELLS: u32 = 3;

#[derive(Debug)]
pub struct InternalNode {
    pub node: CommonNode,
}

impl From<CommonNode> for InternalNode {
    fn from(node: CommonNode) -> Self {
        InternalNode { node }
    }
}

impl InternalNode {
    /// Initializes a `CommonNode` as an `InternalNode`.
    pub fn new(mut node: CommonNode) -> Self {
        node.set_node_type(NodeType::Internal);
        node.set_root(false);
        let mut internal = InternalNode { node };
        internal.set_num_keys(0);
        internal
    }

    /// Gets the number of keys in the node.
    pub fn num_keys(&self) -> u32 {
        unsafe { *(self.node.buffer.add(NUM_KEYS_OFFSET) as *mut u32) }
    }

    /// Sets the number of keys in the node;
    pub fn set_num_keys(&mut self, num_keys: u32) {
        unsafe {
            *(self.node.buffer.add(NUM_KEYS_OFFSET) as *mut u32) = num_keys;
        }
    }

    /// Gets the location of the right child.
    pub fn right_child(&self) -> u32 {
        unsafe { *(self.node.buffer.add(RIGHT_CHILD_OFFSET) as *mut u32) }
    }

    /// Sets the location of the right child.
    pub fn set_right_child(&mut self, right_child: u32) {
        unsafe {
            *(self.node.buffer.add(RIGHT_CHILD_OFFSET) as *mut u32) = right_child;
        }
    }

    /// Gets the location of the specific node cell.
    fn cell(&self, cell_num: u32) -> u32 {
        unsafe {
            *(self
                .node
                .buffer
                .add(HEADER_SIZE + cell_num as usize * CELL_SIZE) as *mut u32)
        }
    }

    /// Sets the location of the specific node cell.
    fn set_cell(&mut self, cell_num: u32, cell: u32) {
        unsafe {
            *(self
                .node
                .buffer
                .add(HEADER_SIZE + cell_num as usize * CELL_SIZE) as *mut u32) = cell;
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
            let internal_node_cell =
                self.node
                    .buffer
                    .add(HEADER_SIZE + key_num as usize * CELL_SIZE) as *mut u32;
            *(internal_node_cell.add(CHILD_SIZE))
        }
    }

    pub fn set_key(&mut self, key_num: u32, key: u32) {
        unsafe {
            let internal_node_cell =
                self.node
                    .buffer
                    .add(HEADER_SIZE + key_num as usize * CELL_SIZE) as *mut u32;
            *(internal_node_cell.add(CHILD_SIZE)) = key;
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

    pub fn update_key(&mut self, old_key: u32, new_key: u32) {
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
        let child = table.pager.page(child_num);
        match child {
            Node::Leaf(leaf) => leaf.find(table, key),
            Node::Internal(internal) => internal.find(table, key),
        }
    }

    /// Add a child/key pair to node.
    pub fn insert<T: Storage>(&mut self, table: &mut Table<T>, child_page_num: u32) {
        let pager = &mut table.pager;
        let child = pager.page(child_page_num);
        let child_max_key = child.get_max_key();

        let index = self.find_child(child_max_key);
        let original_num_keys = self.num_keys();
        self.set_num_keys(original_num_keys + 1);

        if original_num_keys >= MAX_CELLS {
            panic!("Need to implement splitting internal node");
        }

        let right_child_page_num = self.right_child();
        let right_child = pager.page(right_child_page_num);
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
                    memcpy(destination as *mut c_void, source as *mut c_void, CELL_SIZE);
                }
            }
            self.set_child(index, child_page_num);
            self.set_key(index, child_max_key);
        }
    }
}
