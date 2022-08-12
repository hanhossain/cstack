use crate::pager::PAGE_SIZE;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};

pub trait Storage {
    fn new(filename: &str) -> Self;
    fn size(&mut self) -> u64;
    fn read(&mut self, page_num: usize, buf: &mut [u8]);
    fn write(&mut self, page_num: usize, buf: &[u8]);
}

pub struct FileStorage {
    file: File,
}

impl Storage for FileStorage {
    fn new(filename: &str) -> FileStorage {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(filename)
            .unwrap();
        FileStorage { file }
    }

    fn size(&mut self) -> u64 {
        self.file.seek(SeekFrom::End(0)).unwrap()
    }

    fn read(&mut self, page_num: usize, buf: &mut [u8]) {
        self.file
            .seek(SeekFrom::Start(page_num as u64 * PAGE_SIZE as u64))
            .unwrap();
        self.file.read(buf).unwrap();
    }

    fn write(&mut self, page_num: usize, buf: &[u8]) {
        self.file
            .seek(SeekFrom::Start(page_num as u64 * PAGE_SIZE as u64))
            .unwrap();
        self.file.write_all(buf).unwrap();
        self.file.flush().unwrap();
    }
}