use crate::node::{CommonNode, InternalNode, LeafNode, Node};
use crate::storage::{Storage, StorageFactory};

pub const TABLE_MAX_PAGES: usize = 100;
pub const PAGE_SIZE: usize = 4096;

pub struct Pager<T> {
    storage: T,
    file_length: u32,
    pub num_pages: u32,
    pages: [Option<Box<[u8; PAGE_SIZE]>>; TABLE_MAX_PAGES],
}

impl<'a, T: Storage + 'a> Pager<T> {
    pub fn open<F: StorageFactory<'a, T>>(
        storage_factory: &'a mut F,
        filename: &'a str,
    ) -> Pager<T> {
        let mut storage = storage_factory.open(filename);

        let file_length = storage.size();
        if file_length as usize % PAGE_SIZE != 0 {
            panic!("Db file is not a whole number of pages. Corrupt file.");
        }

        Pager {
            storage,
            file_length: file_length as u32,
            num_pages: file_length as u32 / PAGE_SIZE as u32,
            pages: std::array::from_fn(|_| None),
        }
    }

    pub fn close(mut self) {
        let mut pager = &mut self;
        for i in 0..pager.num_pages as usize {
            if pager.pages[i as usize].is_none() {
                continue;
            }
            pager.flush(i);
            pager.pages[i] = None;
        }

        for i in 0..TABLE_MAX_PAGES {
            let _ = pager.pages[i].take();
        }
    }

    pub fn page(&mut self, page_num: usize) -> Node {
        Node::from(self.get_page(page_num))
    }

    pub fn new_leaf_page(&mut self, page_num: usize) -> LeafNode {
        let node = self.get_page(page_num as usize);
        let mut node = LeafNode::from(node);
        node.initialize();
        node
    }

    pub fn new_internal_page(&mut self, page_num: usize) -> InternalNode {
        let node = self.get_page(page_num as usize);
        let mut node = InternalNode::from(node);
        node.initialize();
        node
    }

    fn get_page(&mut self, page_num: usize) -> CommonNode {
        if self.pages[page_num].is_none() {
            // Cache miss. Allocate memory and load from file.
            let mut page = Box::new([0u8; PAGE_SIZE]);
            let mut num_pages = self.file_length as usize / PAGE_SIZE;

            // We might save a partial page at the end of the file
            if self.file_length as usize % PAGE_SIZE != 0 {
                num_pages += 1;
            }

            if page_num <= num_pages {
                self.storage.read(page_num, page.as_mut_slice());
            }

            self.pages[page_num] = Some(page);

            if page_num >= self.num_pages as usize {
                self.num_pages = page_num as u32 + 1;
            }
        }

        let buffer = (&mut self.pages[page_num]).as_mut().unwrap().as_mut_ptr();
        CommonNode::new(buffer)
    }

    fn flush(&mut self, page_num: usize) {
        let page = self.pages[page_num]
            .as_ref()
            .expect("Tried to flush null page");
        self.storage.write(page_num, page.as_slice());
    }

    // TODO: Until we start recycling free pages, new pages will always
    // go onto the end of the database file.
    pub fn get_unused_page_num(&self) -> u32 {
        self.num_pages
    }
}
