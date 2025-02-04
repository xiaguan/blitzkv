use tracing::debug;

use crate::storage::device::{SsdDevice, SsdError};
use crate::storage::page::Page;
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::rc::Rc;

const DEFAULT_PAGE_SIZE: u32 = 4096; // 4KB page size

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct Location {
    pub page_id: u64,
    pub page_index: usize,
}

#[derive(Debug)]
enum PageStatus {
    Memory(Rc<RefCell<Page>>),
    Ssd, // Stores page_id for pages that are only on SSD
}

#[derive(Debug)]
pub enum PageManagerError {
    Storage(SsdError),
    InvalidPage,
}

impl From<SsdError> for PageManagerError {
    fn from(error: SsdError) -> Self {
        PageManagerError::Storage(error)
    }
}

#[derive(Debug)]
pub enum DatabaseError {
    KeyNotFound,
    StorageFull,
    InvalidData,
    Storage(PageManagerError),
}

impl From<PageManagerError> for DatabaseError {
    fn from(error: PageManagerError) -> Self {
        DatabaseError::Storage(error)
    }
}

#[derive(Debug)]
struct PageManager {
    pages: HashMap<u64, PageStatus>,
    device: SsdDevice,
    next_id: u64,
    page_size: u32,
}

impl PageManager {
    fn new<P: AsRef<Path>>(path: P, page_size: u32) -> Result<Self, PageManagerError> {
        let device = SsdDevice::new(path, page_size)?;
        Ok(PageManager {
            pages: HashMap::new(),
            device,
            next_id: 0,
            page_size,
        })
    }

    fn set_inner(
        &mut self,
        key: &[u8],
        value: &[u8],
    ) -> Result<Option<Location>, PageManagerError> {
        let required_space = key.len() + value.len() + 8; // 8 bytes for metadata

        // Try to find a page with enough space
        for (page_id, status) in self.pages.iter() {
            match status {
                PageStatus::Memory(page_rc) => {
                    let mut page = page_rc.borrow_mut();
                    let available_space = page.capacity() - page.size();
                    if available_space >= required_space {
                        if let Some(page_index) = page.push_entry(key, value) {
                            // Write the page to storage
                            self.device.write_page(&mut *page)?;
                            return Ok(Some(Location {
                                page_id: *page_id,
                                page_index,
                            }));
                        }
                    }
                }
                PageStatus::Ssd => {}
            }
        }

        // Create a new page if no existing page has enough space
        let mut new_page = Page::new(self.next_id, self.page_size);
        self.next_id += 1;

        if let Some(page_index) = new_page.push_entry(key, value) {
            let page_id = new_page.id();
            // Write the new page to storage
            self.device.write_page(&mut new_page)?;
            self.pages
                .insert(page_id, PageStatus::Memory(Rc::new(RefCell::new(new_page))));
            Ok(Some(Location {
                page_id,
                page_index,
            }))
        } else {
            Ok(None) // Entry too large even for a new page
        }
    }

    // Try to find a page with enough space for the entry
    // If no suitable page exists, create a new one
    fn set(&mut self, key: &[u8], value: &[u8]) -> Result<Option<Location>, PageManagerError> {
        let location = self.set_inner(key, value)?;
        if let Some(location) = location {
            self.pages.insert(location.page_id, PageStatus::Ssd);
        }
        return Ok(location);
    }

    // Remove an entry from a specific page
    fn delete(&mut self, page_id: u64, key: &[u8]) -> Result<bool, PageManagerError> {
        match self.pages.get(&page_id) {
            Some(PageStatus::Memory(page_rc)) => {
                let mut page = page_rc.borrow_mut();
                let result = page.remove_entry(key);
                if result {
                    // Update the page in storage
                    self.device.write_page(&mut *page)?;
                }
                Ok(result)
            }
            Some(PageStatus::Ssd) | None => {
                // Try to load the page from storage
                match self.device.read_page(page_id) {
                    Ok(mut page) => {
                        let result = page.remove_entry(key);
                        if result {
                            self.device.write_page(&mut page)?;
                        }
                        self.pages
                            .insert(page_id, PageStatus::Memory(Rc::new(RefCell::new(page))));
                        Ok(result)
                    }
                    Err(SsdError::InvalidPageId) => Ok(false),
                    Err(e) => Err(e.into()),
                }
            }
        }
    }

    // Get a page, returns a shared reference that can be mutably borrowed
    fn get(
        &mut self,
        location: &Location,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, PageManagerError> {
        match self.pages.get(&location.page_id) {
            Some(PageStatus::Memory(page_rc)) => {
                let page = page_rc.borrow();
                Ok(page.get(location.page_index, key))
            }
            Some(PageStatus::Ssd) | None => {
                // Try to load the page from storage
                match self.device.read_page(location.page_id) {
                    Ok(page) => {
                        let page_rc = Rc::new(RefCell::new(page));
                        let value = page_rc.borrow().get(location.page_index, key);
                        self.pages
                            .insert(location.page_id, PageStatus::Memory(page_rc));
                        Ok(value)
                    }
                    Err(SsdError::InvalidPageId) => Ok(None),
                    Err(e) => Err(e.into()),
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct Database {
    // Memory index mapping keys to their location
    index: BTreeMap<Vec<u8>, Location>,
    // Page manager for storage allocation
    page_manager: PageManager,
}

impl Database {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, DatabaseError> {
        Ok(Database {
            index: BTreeMap::new(),
            page_manager: PageManager::new(path, DEFAULT_PAGE_SIZE)?,
        })
    }

    pub fn set(&mut self, key: &[u8], value: &[u8]) -> Result<(), DatabaseError> {
        // Try to allocate space for the entry
        if let Some(location) = self.page_manager.set(key, value)? {
            // Update index with new location
            debug!(
                "write key {} to location {:?}",
                String::from_utf8(key.to_vec()).unwrap(),
                location
            );
            self.index.insert(key.to_vec(), location);
            Ok(())
        } else {
            Err(DatabaseError::StorageFull)
        }
    }

    pub fn get(&mut self, key: &[u8]) -> Result<Vec<u8>, DatabaseError> {
        // Look up key in index
        if let Some(location) = self.index.get(key) {
            // Get the page from page manager
            debug!(
                "key {} is at {:?}",
                String::from_utf8(key.to_vec()).unwrap(),
                location
            );
            if let Some(value) = self.page_manager.get(location, key)? {
                return Ok(value);
            }
            Err(DatabaseError::InvalidData)
        } else {
            Err(DatabaseError::KeyNotFound)
        }
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<(), DatabaseError> {
        if let Some(location) = self.index.get(key).cloned() {
            if self.page_manager.delete(location.page_id, key)? {
                self.index.remove(key);
                Ok(())
            } else {
                Err(DatabaseError::InvalidData)
            }
        } else {
            Err(DatabaseError::KeyNotFound)
        }
    }

    // Get all keys in sorted order
    pub fn keys(&self) -> Vec<Vec<u8>> {
        self.index.keys().cloned().collect()
    }

    // Get number of key-value pairs
    pub fn len(&self) -> usize {
        self.index.len()
    }

    // Check if database is empty
    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tempfile::tempdir;

    #[test]
    fn test_basic_operations() -> Result<(), DatabaseError> {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.db");
        let mut db = Database::new(file_path)?;

        // Test set
        db.set(b"key1", b"value1")?;
        db.set(b"key2", b"value2")?;

        // Test get
        assert_eq!(db.get(b"key1")?, b"value1");
        assert_eq!(db.get(b"key2")?, b"value2");
        assert!(matches!(
            db.get(b"nonexistent"),
            Err(DatabaseError::KeyNotFound)
        ));

        // Test delete
        db.delete(b"key1")?;
        assert!(matches!(db.get(b"key1"), Err(DatabaseError::KeyNotFound)));

        // Test length
        assert_eq!(db.len(), 1);
        assert!(!db.is_empty());
        Ok(())
    }

    #[test]
    fn test_storage_unit_rotation() -> Result<(), DatabaseError> {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test_rotation.db");
        let mut db = Database::new(file_path)?;

        // Fill up multiple storage units
        for i in 0..1000 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            db.set(key.as_bytes(), value.as_bytes())?;
        }

        for i in 0..1000 {
            let key = format!("key{}", i);
            let value = db.get(key.as_bytes())?;
            assert_eq!(value, format!("value{}", i).as_bytes());
        }

        // random remove some
        for i in 0..1000 {
            if i % 2 == 0 {
                let key = format!("key{}", i);
                db.delete(key.as_bytes())?;
            }
        }

        Ok(())
    }
}
