use crate::node::{internal_node_find, leaf_node_find, InternalNode, LeafNode, Node, NodeType};
use crate::pager::{Pager, PAGE_SIZE, TABLE_MAX_PAGES};
use libc::{c_void, close, memcpy, EXIT_FAILURE};
use std::process::exit;
use std::ptr::null_mut;

pub struct Table {
    pub pager: Pager,
    pub root_page_num: u32,
}

impl Table {
    /// Return the position of the given key.
    /// If the key is not present, return the position
    /// where it should be inserted.
    pub unsafe fn find(&mut self, key: u32) -> Cursor {
        let root_page_num = self.root_page_num;
        let root_node = self.pager.get_page(root_page_num as usize);

        match root_node.node_type() {
            NodeType::Internal => internal_node_find(self, root_page_num, key),
            NodeType::Leaf => leaf_node_find(self, root_page_num, key),
        }
    }

    pub unsafe fn start(&mut self) -> Cursor {
        let mut cursor = self.find(0);
        let page_num = cursor.page_num as usize;
        let node = self.pager.get_page(page_num);
        let leaf_node = LeafNode::new(node.buffer);
        let num_cells = leaf_node.num_cells();
        cursor.end_of_table = num_cells == 0;
        cursor
    }

    pub fn open(filename: &str) -> Table {
        let pager = unsafe {
            let mut pager = Pager::open(filename);
            if pager.num_pages == 0 {
                // New database file. Initialize page 0 as leaf node.
                let root_node = pager.get_page(0);
                let mut leaf_node = LeafNode::new(root_node.buffer);
                leaf_node.initialize();
                let mut root_node = Node::new(root_node.buffer);
                root_node.set_root(true);
            }
            pager
        };

        Table {
            pager,
            root_page_num: 0,
        }
    }

    pub unsafe fn close(&mut self) {
        let pager = &mut self.pager;

        for i in 0..pager.num_pages as usize {
            if pager.pages[i as usize].is_null() {
                continue;
            }
            pager.flush(i);
            let _ = Box::from_raw(pager.pages[i]);
            pager.pages[i] = null_mut();
        }

        let result = close(pager.file_descriptor);
        if result == -1 {
            println!("Error closing db file.");
            exit(EXIT_FAILURE);
        }

        for i in 0..TABLE_MAX_PAGES {
            let page = pager.pages[i];
            if !page.is_null() {
                let _ = Box::from_raw(page);
                pager.pages[i] = null_mut();
            }
        }
    }

    // Handle splitting the root.
    // Old root copied to new page, becomes the left child.
    // Address of right child passed in.
    // Re-initialize root page to contain the new root node.
    // New root node points to two children.
    pub(crate) unsafe fn create_new_root(&mut self, right_child_page_num: u32) {
        let pager = &mut self.pager;
        let mut root = pager.get_page(self.root_page_num as usize);
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
        let mut root_internal_node = InternalNode::new(root.buffer);
        root_internal_node.initialize();
        root.set_root(true);
        root_internal_node.set_num_keys(1);
        root_internal_node.set_child(0, left_child_page_num);
        let left_child_max_key = left_child.get_node_max_key();
        root_internal_node.set_key(0, left_child_max_key);
        root_internal_node.set_right_child(right_child_page_num);
        left_child.set_parent(self.root_page_num);
        right_child.set_parent(self.root_page_num);
    }
}

pub struct Cursor {
    pub table: *mut Table,
    pub page_num: u32,
    pub cell_num: u32,
    /// Indicates a position one past the last element
    pub end_of_table: bool,
}

impl Cursor {
    pub unsafe fn value(&mut self) -> *mut u8 {
        let page = (*self.table).pager.get_page(self.page_num as usize);
        LeafNode::new(page.buffer).value(self.cell_num)
    }

    pub unsafe fn advance(&mut self) {
        let page_num = self.page_num;
        let node = (&mut *self.table).pager.get_page(page_num as usize);

        self.cell_num += 1;
        let leaf_node = LeafNode::new(node.buffer);
        if self.cell_num >= leaf_node.num_cells() {
            // Advance to next leaf node
            let next_page_num = leaf_node.next_leaf();
            if next_page_num == 0 {
                // This was the rightmost leaf
                self.end_of_table = true;
            } else {
                self.page_num = next_page_num;
                self.cell_num = 0;
            }
        }
    }
}
