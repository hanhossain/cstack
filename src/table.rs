use crate::node::{LeafNode, Node};
use crate::pager::Pager;
use crate::storage::{Storage, StorageFactory};

pub struct Table<T> {
    pub pager: Pager<T>,
    root_page_num: u32,
}

impl<'a, T: Storage + 'a> Table<T> {
    /// Return the position of the given key.
    /// If the key is not present, return the position
    /// where it should be inserted.
    pub fn find(&mut self, key: u32) -> Cursor<T> {
        let root_page_num = self.root_page_num;
        let root_node = self.pager.page(root_page_num as usize);

        match root_node {
            Node::Internal(internal) => internal.find(self, key),
            Node::Leaf(leaf) => leaf.find(self, key),
        }
    }

    pub fn start(&mut self) -> Cursor<T> {
        let mut cursor = self.find(0);
        let num_cells = cursor.node.num_cells();
        cursor.end_of_table = num_cells == 0;
        cursor
    }

    pub fn open<F: StorageFactory<'a, T>>(
        storage_factory: &'a mut F,
        filename: &'a str,
    ) -> Table<T> {
        let mut pager = Pager::open(storage_factory, filename);
        if pager.num_pages == 0 {
            // New database file. Initialize page 0 as leaf node.
            let mut root_node = pager.new_leaf_page(0);
            root_node.node.set_root(true);
        }

        Table {
            pager,
            root_page_num: 0,
        }
    }

    pub fn close(self) {
        self.pager.close();
    }

    // Handle splitting the root.
    // Old root copied to new page, becomes the left child.
    // Address of right child passed in.
    // Re-initialize root page to contain the new root node.
    // New root node points to two children.
    pub(crate) fn create_new_root(&mut self, right_child_page_num: u32) {
        let pager = &mut self.pager;

        // get old root page
        let root = pager.page(self.root_page_num as usize);
        let left_child_max_key = root.get_max_key();

        // get right child page
        let mut right_child = pager.page(right_child_page_num as usize);

        // get an unused page for the left child
        let left_child_page_num = pager.get_unused_page_num();
        let mut left_child = pager.page(left_child_page_num as usize);

        // Copy data from old root to left child
        left_child.buffer_mut().copy_from_slice(root.buffer());
        left_child.set_root(false);

        // Create a new root node as an internal node with one key and two children
        let mut root = pager.new_internal_page(self.root_page_num as usize);
        root.node.set_root(true);
        root.set_num_keys(1);
        root.set_child(0, left_child_page_num);
        root.set_key(0, left_child_max_key);
        root.set_right_child(right_child_page_num);
        left_child.set_parent(self.root_page_num);
        right_child.set_parent(self.root_page_num);
    }
}

/// Leaf node iterator
pub struct Cursor<T> {
    pub table: *mut Table<T>,
    pub cell_num: u32,
    /// Indicates a position one past the last element
    pub end_of_table: bool,
    pub node: LeafNode,
}

impl<T: Storage> Cursor<T> {
    pub fn value(&self) -> &[u8] {
        self.node.value(self.cell_num)
    }

    pub fn advance(&mut self) {
        self.cell_num += 1;
        if self.cell_num >= self.node.num_cells() {
            // Advance to next leaf node
            let next_page_num = self.node.next_leaf();
            if next_page_num == 0 {
                // This was the rightmost leaf
                self.end_of_table = true;
            } else {
                self.node = unsafe { &mut *self.table }
                    .pager
                    .page(next_page_num as usize)
                    .unwrap_leaf();
                self.cell_num = 0;
            }
        }
    }
}
