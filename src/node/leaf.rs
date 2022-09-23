use crate::node::common;
use crate::node::common::CommonNode;
use crate::node::NodeType;
use crate::pager::PAGE_SIZE;
use crate::serialization::ROW_SIZE;
use crate::storage::Storage;
use crate::table::Cursor;
use crate::{Row, Table};
use libc::memcpy;
use std::ffi::c_void;
use std::mem::size_of;

// Leaf Node Header Layout
//
// | common header | num cells | next leaf |
const NUM_CELLS_SIZE: usize = size_of::<u32>();
const NUM_CELLS_OFFSET: usize = common::HEADER_SIZE;
const NEXT_LEAF_SIZE: usize = size_of::<u32>();
const NEXT_LEAF_OFFSET: usize = NUM_CELLS_OFFSET + NUM_CELLS_SIZE;
pub(crate) const HEADER_SIZE: usize = common::HEADER_SIZE + NUM_CELLS_SIZE + NEXT_LEAF_SIZE;

// Leaf Node Body Layout
const KEY_SIZE: usize = size_of::<u32>();
const VALUE_SIZE: usize = ROW_SIZE;
pub(crate) const CELL_SIZE: usize = KEY_SIZE + VALUE_SIZE;
pub(crate) const SPACE_FOR_CELLS: usize = PAGE_SIZE - HEADER_SIZE;
pub(crate) const MAX_CELLS: usize = SPACE_FOR_CELLS / CELL_SIZE;

const RIGHT_SPLIT_COUNT: usize = (MAX_CELLS + 1) / 2;
const LEFT_SPLIT_COUNT: usize = (MAX_CELLS + 1) - RIGHT_SPLIT_COUNT;

#[derive(Debug)]
pub struct LeafNode {
    pub node: CommonNode,
}

impl From<CommonNode> for LeafNode {
    fn from(node: CommonNode) -> Self {
        LeafNode { node }
    }
}

impl LeafNode {
    /// Initialize a `CommonNode` as a `LeafNode`
    pub fn new(mut node: CommonNode) -> Self {
        node.set_node_type(NodeType::Leaf);
        node.set_root(false);
        let mut leaf = LeafNode { node };
        leaf.set_num_cells(0);
        leaf.set_next_leaf(0); // 0 represents no siblings
        leaf
    }

    /// Get the number of cells currently occupied in the node.
    pub fn num_cells(&self) -> u32 {
        unsafe { *(self.node.buffer.add(NUM_CELLS_OFFSET) as *mut u32) }
    }

    /// Set the number of cells currently occupied in the node.
    pub fn set_num_cells(&mut self, num_cells: u32) {
        unsafe {
            *(self.node.buffer.add(NUM_CELLS_OFFSET) as *mut u32) = num_cells;
        }
    }

    /// Get the pointer to the leaf node cell.
    fn cell(&self, cell_num: u32) -> *mut u8 {
        unsafe {
            self.node
                .buffer
                .add(HEADER_SIZE + cell_num as usize * CELL_SIZE)
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

    /// Get a mutable slice to the leaf node value.
    pub fn value_mut(&mut self, cell_num: u32) -> &mut [u8] {
        unsafe {
            let ptr = self.cell(cell_num).add(KEY_SIZE);
            std::slice::from_raw_parts_mut(ptr, ROW_SIZE)
        }
    }

    /// Get a slice to the leaf node value
    pub fn value(&self, cell_num: u32) -> &[u8] {
        unsafe {
            let ptr = self.cell(cell_num).add(KEY_SIZE);
            std::slice::from_raw_parts(ptr, ROW_SIZE)
        }
    }

    /// Gets the location of the next leaf.
    pub fn next_leaf(&self) -> u32 {
        unsafe { *(self.node.buffer.add(NEXT_LEAF_OFFSET) as *mut u32) }
    }

    /// Sets the location of the next leaf.
    pub fn set_next_leaf(&mut self, next_leaf: u32) {
        unsafe {
            *(self.node.buffer.add(NEXT_LEAF_OFFSET) as *mut u32) = next_leaf;
        }
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
    let mut new_node = pager.new_leaf_page(new_page_num);
    new_node.node.set_parent(old_node.node.parent());
    new_node.set_next_leaf(old_node.next_leaf());
    old_node.set_next_leaf(new_page_num);

    // All existing keys plus new key should be divided
    // evenly between old (left) and new (right) nodes.
    // Starting from the right, move each key to correct position.
    for i in (0..=MAX_CELLS as i32).rev() {
        let destination_node = if i >= LEFT_SPLIT_COUNT as i32 {
            &mut new_node
        } else {
            &mut old_node
        };
        let index_within_node = i % LEFT_SPLIT_COUNT as i32;
        let destination = destination_node.cell(index_within_node as u32);

        unsafe {
            if i == cursor.cell_num as i32 {
                bincode::serialize_into(
                    destination_node.value_mut(index_within_node as u32),
                    value,
                )
                .unwrap();
                destination_node.set_key(index_within_node as u32, key);
            } else if i > cursor.cell_num as i32 {
                memcpy(
                    destination as *mut c_void,
                    old_node.cell((i - 1) as u32) as *mut c_void,
                    CELL_SIZE,
                );
            } else {
                memcpy(
                    destination as *mut c_void,
                    old_node.cell(i as u32) as *mut c_void,
                    CELL_SIZE,
                );
            }
        }
    }

    // Update cell count on both leaf nodes
    old_node.set_num_cells(LEFT_SPLIT_COUNT as u32);
    new_node.set_num_cells(RIGHT_SPLIT_COUNT as u32);

    unsafe {
        if old_node.node.is_root() {
            (&mut *cursor.table).create_new_root(new_page_num);
        } else {
            let parent_page_num = old_node.node.parent();
            let new_max = old_node.get_max_key();
            let mut parent = (&mut *cursor.table)
                .pager
                .page(parent_page_num)
                .unwrap_internal();
            parent.update_key(old_max, new_max);
            parent.insert(&mut *cursor.table, new_page_num);
        }
    }
}

pub(crate) fn leaf_node_insert<T: Storage>(mut cursor: Cursor<T>, key: u32, value: &Row) {
    let num_cells = cursor.node.num_cells();
    if num_cells >= MAX_CELLS as u32 {
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
                    CELL_SIZE,
                );
            }
        }
    }

    cursor.node.set_num_cells(cursor.node.num_cells() + 1);
    cursor.node.set_key(cursor.cell_num, key);
    bincode::serialize_into(cursor.node.value_mut(cursor.cell_num), value).unwrap();
}
