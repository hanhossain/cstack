use libc::{
    c_char, c_uint, c_void, exit, lseek, malloc, open, read, write, EXIT_FAILURE, O_CREAT, O_RDWR,
    SEEK_END, SEEK_SET, S_IRUSR, S_IWUSR,
};
use std::ptr::null_mut;

pub const TABLE_MAX_PAGES: usize = 100;
pub const PAGE_SIZE: usize = 4096;

#[repr(C)]
pub struct Pager {
    pub file_descriptor: i32,
    pub file_length: u32,
    pub num_pages: u32,
    pub pages: [*mut c_void; TABLE_MAX_PAGES],
}

// Until we start recycling free pages, new pages will always
// go onto the end of the database file.
pub(crate) fn get_unused_page_num(pager: &Pager) -> u32 {
    pager.num_pages
}

pub(crate) unsafe fn get_page(pager: &mut Pager, page_num: usize) -> *mut c_void {
    if page_num > TABLE_MAX_PAGES {
        println!("Tried to fetch page number out of bounds. {page_num} > {TABLE_MAX_PAGES}");
        exit(EXIT_FAILURE);
    }

    if pager.pages[page_num].is_null() {
        // Cache miss. Allocate memory and load from file.
        let page = malloc(PAGE_SIZE);
        let mut num_pages = pager.file_length as usize / PAGE_SIZE;

        // We might save a partial page at the end of the file
        if pager.file_length as usize % PAGE_SIZE != 0 {
            num_pages += 1;
        }

        if page_num <= num_pages {
            lseek(
                pager.file_descriptor,
                page_num as i64 * PAGE_SIZE as i64,
                SEEK_SET,
            );
            let bytes_read = read(pager.file_descriptor, page, PAGE_SIZE);
            if bytes_read == -1 {
                println!("Error reading file");
                exit(EXIT_FAILURE);
            }
        }

        pager.pages[page_num] = page;

        if page_num >= pager.num_pages as usize {
            pager.num_pages = page_num as u32 + 1;
        }
    }

    pager.pages[page_num]
}

pub(crate) unsafe fn pager_flush(pager: &mut Pager, page_num: usize) {
    if pager.pages[page_num].is_null() {
        println!("Tried to flush null page");
        exit(EXIT_FAILURE);
    }

    let offset = lseek(
        pager.file_descriptor,
        (page_num * PAGE_SIZE) as i64,
        SEEK_SET,
    );

    if offset == -1 {
        println!("Error seeking");
        exit(EXIT_FAILURE);
    }

    let bytes_written = write(pager.file_descriptor, pager.pages[page_num], PAGE_SIZE);

    if bytes_written == -1 {
        println!("Error writing");
        exit(EXIT_FAILURE);
    }
}

pub(crate) unsafe fn pager_open(filename: *const c_char) -> Pager {
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
