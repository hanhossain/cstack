use crate::pager::PAGE_SIZE;
#[cfg(test)]
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};

pub trait StorageFactory<'a, S: Storage + 'a> {
    fn open(&'a mut self, filename: &'a str) -> S;
}

pub struct FileStorageFactory;

impl<'a> StorageFactory<'a, FileStorage> for FileStorageFactory {
    fn open(&mut self, filename: &str) -> FileStorage {
        FileStorage::new(filename)
    }
}

pub trait Storage {
    fn size(&mut self) -> u64;
    fn read(&mut self, page_num: usize, buf: &mut [u8]);
    fn write(&mut self, page_num: usize, buf: &[u8]);
}

pub struct FileStorage {
    file: File,
}

impl FileStorage {
    fn new(filename: &str) -> FileStorage {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(filename)
            .unwrap();
        FileStorage { file }
    }
}

impl Storage for FileStorage {
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

#[cfg(test)]
pub struct InMemoryStorageFactory {
    stores: HashMap<String, HashMap<usize, [u8; PAGE_SIZE]>>,
}

#[cfg(test)]
impl InMemoryStorageFactory {
    pub fn new() -> Self {
        Self {
            stores: HashMap::new(),
        }
    }
}

#[cfg(test)]
impl<'a> StorageFactory<'a, InMemoryStorage<'a>> for InMemoryStorageFactory {
    fn open<'b>(&'b mut self, filename: &'b str) -> InMemoryStorage<'_> {
        if self.stores.get(filename).is_none() {
            self.stores.insert(filename.to_string(), HashMap::new());
        }

        let pages = self.stores.get_mut(filename).unwrap();
        InMemoryStorage { pages }
    }
}

#[cfg(test)]
pub struct InMemoryStorage<'a> {
    pages: &'a mut HashMap<usize, [u8; PAGE_SIZE]>,
}

#[cfg(test)]
impl<'a> Storage for InMemoryStorage<'a> {
    fn size(&mut self) -> u64 {
        (self.pages.len() * PAGE_SIZE) as u64
    }

    fn read(&mut self, page_num: usize, buf: &mut [u8]) {
        if let Some(page) = self.pages.get(&page_num) {
            buf.copy_from_slice(page.as_slice());
        }
    }

    fn write(&mut self, page_num: usize, buf: &[u8]) {
        if self.pages.get(&page_num).is_none() {
            self.pages.insert(page_num, [0u8; PAGE_SIZE]);
        }
        let page = self.pages.get_mut(&page_num).unwrap();
        page[..buf.len()].copy_from_slice(buf);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn in_memory_sanity() {
        let mut factory = InMemoryStorageFactory::new();
        let mut storage = factory.open("foobar");
        let text1 = b"first";
        storage.write(0, text1);

        let text2 = b"second";
        storage.write(1, text2);

        let mut buf = [0u8; PAGE_SIZE];
        storage.read(0, &mut buf);
        assert_eq!(text1, &buf[..text1.len()]);

        let mut buf = [0u8; PAGE_SIZE];
        storage.read(1, &mut buf);
        assert_eq!(text2, &buf[..text2.len()]);
    }

    #[test]
    fn in_memory_flush_reopen() {
        let mut factory = InMemoryStorageFactory::new();
        let text1 = b"first";

        {
            let mut storage = factory.open("foobar");
            storage.write(0, text1);
        }

        {
            let mut storage = factory.open("foobar");
            let mut buf = [0u8; PAGE_SIZE];
            storage.read(0, &mut buf);
            assert_eq!(text1, &buf[..text1.len()]);
        }
    }
}
