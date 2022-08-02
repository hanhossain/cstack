use crate::node::{
    initialize_leaf_node, internal_node_find, leaf_node_find, leaf_node_next_leaf,
    leaf_node_num_cells, leaf_node_value, Node, NodeType,
};
use crate::pager::{Pager, TABLE_MAX_PAGES};
use libc::{close, EXIT_FAILURE};
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
        let num_cells = leaf_node_num_cells(node.buffer);
        cursor.end_of_table = num_cells == 0;
        cursor
    }

    pub fn open(filename: &str) -> Table {
        let pager = unsafe {
            let mut pager = Pager::open(filename);
            if pager.num_pages == 0 {
                // New database file. Initialize page 0 as leaf node.
                let root_node = pager.get_page(0);
                initialize_leaf_node(root_node.buffer);
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
        leaf_node_value(page.buffer, self.cell_num)
    }

    pub unsafe fn advance(&mut self) {
        let page_num = self.page_num;
        let node = (&mut *self.table).pager.get_page(page_num as usize);

        self.cell_num += 1;
        if self.cell_num >= leaf_node_num_cells(node.buffer) {
            // Advance to next leaf node
            let next_page_num = leaf_node_next_leaf(node.buffer);
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
