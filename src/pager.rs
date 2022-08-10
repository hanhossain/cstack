use crate::node::{CommonNode, InternalNode, LeafNode, Node};
use libc::{
    c_uint, c_void, close, lseek, open, read, write, EXIT_FAILURE, O_CREAT, O_RDWR, SEEK_END,
    SEEK_SET, S_IRUSR, S_IWUSR,
};
use std::ffi::CString;
use std::process::exit;
use std::ptr::null_mut;

pub const TABLE_MAX_PAGES: usize = 100;
pub const PAGE_SIZE: usize = 4096;

pub struct Pager {
    file_descriptor: i32,
    file_length: u32,
    pub num_pages: u32,
    pages: [*mut u8; TABLE_MAX_PAGES],
}

impl Pager {
    pub unsafe fn open(filename: &str) -> Pager {
        let filename_owned = CString::new(filename).unwrap();
        let filename = filename_owned.as_ptr();
        let fd = open(
            filename,
            O_RDWR | O_CREAT,
            S_IRUSR as c_uint | S_IWUSR as c_uint,
        );
        if fd == -1 {
            println!("Unable to open file");
            exit(EXIT_FAILURE);
        }

        let file_length = lseek(fd, 0, SEEK_END);
        if file_length as usize % PAGE_SIZE != 0 {
            println!("Db file is not a whole number of pages. Corrupt file.");
            exit(EXIT_FAILURE);
        }

        Pager {
            file_length: file_length as u32,
            file_descriptor: fd,
            num_pages: file_length as u32 / PAGE_SIZE as u32,
            pages: [null_mut(); TABLE_MAX_PAGES],
        }
    }

    pub unsafe fn close(mut self) {
        let mut pager = &mut self;

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

    pub unsafe fn page(&mut self, page_num: usize) -> Node {
        Node::from(self.get_page(page_num))
    }

    pub unsafe fn new_leaf_page(&mut self, page_num: usize) -> LeafNode {
        let node = self.get_page(page_num as usize);
        let mut node = LeafNode::from(node);
        node.initialize();
        node
    }

    pub unsafe fn new_internal_page(&mut self, page_num: usize) -> InternalNode {
        let node = self.get_page(page_num as usize);
        let mut node = InternalNode::from(node);
        node.initialize();
        node
    }

    unsafe fn get_page(&mut self, page_num: usize) -> CommonNode {
        if page_num > TABLE_MAX_PAGES {
            println!("Tried to fetch page number out of bounds. {page_num} > {TABLE_MAX_PAGES}");
            exit(EXIT_FAILURE);
        }

        if self.pages[page_num].is_null() {
            // Cache miss. Allocate memory and load from file.
            let page = Box::into_raw(Box::new([0u8; PAGE_SIZE]));
            let mut num_pages = self.file_length as usize / PAGE_SIZE;

            // We might save a partial page at the end of the file
            if self.file_length as usize % PAGE_SIZE != 0 {
                num_pages += 1;
            }

            if page_num <= num_pages {
                lseek(
                    self.file_descriptor,
                    page_num as i64 * PAGE_SIZE as i64,
                    SEEK_SET,
                );
                let bytes_read = read(self.file_descriptor, page as *mut c_void, PAGE_SIZE);
                if bytes_read == -1 {
                    println!("Error reading file");
                    exit(EXIT_FAILURE);
                }
            }

            self.pages[page_num] = page as *mut u8;

            if page_num >= self.num_pages as usize {
                self.num_pages = page_num as u32 + 1;
            }
        }

        CommonNode::new(self.pages[page_num])
    }

    unsafe fn flush(&mut self, page_num: usize) {
        if self.pages[page_num].is_null() {
            println!("Tried to flush null page");
            exit(EXIT_FAILURE);
        }

        let offset = lseek(
            self.file_descriptor,
            (page_num * PAGE_SIZE) as i64,
            SEEK_SET,
        );

        if offset == -1 {
            println!("Error seeking");
            exit(EXIT_FAILURE);
        }

        let bytes_written = write(
            self.file_descriptor,
            self.pages[page_num] as *mut c_void,
            PAGE_SIZE,
        );

        if bytes_written == -1 {
            println!("Error writing");
            exit(EXIT_FAILURE);
        }
    }

    // TODO: Until we start recycling free pages, new pages will always
    // go onto the end of the database file.
    pub fn get_unused_page_num(&self) -> u32 {
        self.num_pages
    }
}
