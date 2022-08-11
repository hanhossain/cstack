use crate::node::{CommonNode, InternalNode, LeafNode, Node};
use libc::EXIT_FAILURE;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::process::exit;
use std::ptr::null_mut;

pub const TABLE_MAX_PAGES: usize = 100;
pub const PAGE_SIZE: usize = 4096;

pub struct Pager {
    file: File,
    file_length: u32,
    pub num_pages: u32,
    pages: [*mut u8; TABLE_MAX_PAGES],
}

impl Pager {
    pub fn open(filename: &str) -> Pager {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(filename)
            .unwrap();

        let file_length = file.seek(SeekFrom::End(0)).unwrap();
        if file_length as usize % PAGE_SIZE != 0 {
            println!("Db file is not a whole number of pages. Corrupt file.");
            exit(EXIT_FAILURE);
        }

        Pager {
            file,
            file_length: file_length as u32,
            num_pages: file_length as u32 / PAGE_SIZE as u32,
            pages: [null_mut(); TABLE_MAX_PAGES],
        }
    }

    pub fn close(mut self) {
        let mut pager = &mut self;
        unsafe {
            for i in 0..pager.num_pages as usize {
                if pager.pages[i as usize].is_null() {
                    continue;
                }
                pager.flush(i);
                let _ = Box::from_raw(pager.pages[i]);
                pager.pages[i] = null_mut();
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
        if page_num > TABLE_MAX_PAGES {
            println!("Tried to fetch page number out of bounds. {page_num} > {TABLE_MAX_PAGES}");
            exit(EXIT_FAILURE);
        }

        if self.pages[page_num].is_null() {
            // Cache miss. Allocate memory and load from file.
            let mut page = Box::new([0u8; PAGE_SIZE]);
            let mut num_pages = self.file_length as usize / PAGE_SIZE;

            // We might save a partial page at the end of the file
            if self.file_length as usize % PAGE_SIZE != 0 {
                num_pages += 1;
            }

            if page_num <= num_pages {
                self.file
                    .seek(SeekFrom::Start(page_num as u64 * PAGE_SIZE as u64))
                    .unwrap();
                self.file.read(page.as_mut_slice()).unwrap();
            }

            self.pages[page_num] = Box::into_raw(page) as *mut u8;

            if page_num >= self.num_pages as usize {
                self.num_pages = page_num as u32 + 1;
            }
        }

        CommonNode::new(self.pages[page_num])
    }

    fn flush(&mut self, page_num: usize) {
        if self.pages[page_num].is_null() {
            println!("Tried to flush null page");
            exit(EXIT_FAILURE);
        }

        self.file
            .seek(SeekFrom::Start(page_num as u64 * PAGE_SIZE as u64))
            .unwrap();

        self.file
            .write_all(unsafe { std::slice::from_raw_parts(self.pages[page_num], PAGE_SIZE) })
            .unwrap();
    }

    // TODO: Until we start recycling free pages, new pages will always
    // go onto the end of the database file.
    pub fn get_unused_page_num(&self) -> u32 {
        self.num_pages
    }
}
