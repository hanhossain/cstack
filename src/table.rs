use crate::node::{internal_node_find, leaf_node_find, InternalNode, Node};
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
        let root_node = self.pager.page(root_page_num as usize);

        match root_node {
            Node::Internal(_) => internal_node_find(self, root_page_num, key),
            Node::Leaf(_) => leaf_node_find(self, root_page_num, key),
        }
    }

    pub unsafe fn start(&mut self) -> Cursor {
        let mut cursor = self.find(0);
        let page_num = cursor.page_num as usize;
        let node = self.pager.page(page_num).unwrap_leaf();
        let num_cells = node.num_cells();
        cursor.end_of_table = num_cells == 0;
        cursor
    }

    pub fn open(filename: &str) -> Table {
        let pager = unsafe {
            let mut pager = Pager::open(filename);
            if pager.num_pages == 0 {
                // New database file. Initialize page 0 as leaf node.
                let mut root_node = pager.new_leaf_page(0);
                root_node.node.set_root(true);
            }
            pager
        };

        Table {
            pager,
            root_page_num: 0,
        }
    }

    pub unsafe fn close(mut self) {
        let mut pager = &mut self.pager;

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

        // get old root page
        let root = pager.get_page(self.root_page_num as usize);

        // get right child page
        let mut right_child = pager.get_page(right_child_page_num as usize);

        // get an unused page for the left child
        let left_child_page_num = pager.get_unused_page_num();
        let mut left_child = pager.get_page(left_child_page_num as usize);

        // Copy data from old root to left child
        memcpy(
            left_child.buffer as *mut c_void,
            root.buffer as *mut c_void,
            PAGE_SIZE,
        );
        left_child.set_root(false);

        // Create a new root node as an internal node with one key and two children
        let mut root = InternalNode::from(root);
        root.initialize();
        root.node.set_root(true);
        root.set_num_keys(1);
        root.set_child(0, left_child_page_num);
        let left_child_max_key = left_child.get_node_max_key();
        root.set_key(0, left_child_max_key);
        root.set_right_child(right_child_page_num);
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
        let mut node = (*self.table)
            .pager
            .page(self.page_num as usize)
            .unwrap_leaf();
        node.value(self.cell_num)
    }

    pub unsafe fn advance(&mut self) {
        let page_num = self.page_num;
        let node = (&mut *self.table)
            .pager
            .page(page_num as usize)
            .unwrap_leaf();

        self.cell_num += 1;
        if self.cell_num >= node.num_cells() {
            // Advance to next leaf node
            let next_page_num = node.next_leaf();
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
