use tracing::info;

use crate::storage::{PageManager, PageManagerError};
use std::collections::BTreeMap;
use std::path::Path;

const DEFAULT_PAGE_SIZE: u32 = 4096; // 4KB page size

#[derive(Debug)]
pub struct Database {
    // Memory index mapping keys to their location
    index: BTreeMap<Vec<u8>, Location>,
    // Page manager for storage allocation
    page_manager: PageManager,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct Location {
    pub page_id: u64,
    pub page_index: usize,
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

impl Database {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, DatabaseError> {
        Ok(Database {
            index: BTreeMap::new(),
            page_manager: PageManager::new(path, DEFAULT_PAGE_SIZE)?,
        })
    }

    pub fn set(&mut self, key: &[u8], value: &[u8]) -> Result<(), DatabaseError> {
        // Try to allocate space for the entry
        if let Some(location) = self.page_manager.allocate_entry(key, value)? {
            // Update index with new location
            info!(
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
            info!(
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
            if self.page_manager.remove_entry(location.page_id, key)? {
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

    // Get total size of all pages
    pub fn total_size(&self) -> usize {
        self.page_manager.total_used_space()
    }

    // Get total capacity of all pages
    pub fn total_capacity(&self) -> usize {
        self.page_manager.total_capacity()
    }

    // Calculate overall space amplification
    pub fn space_amplification(&self) -> f64 {
        let total_size = self.total_size() as f64;
        let data_size = self.index.len() as f64; // Simplified - actual data size would need key/value sizes
        total_size / data_size
    }
}

impl std::fmt::Display for Database {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        for page in self.page_manager.iter_pages() {
            write!(f, "{}\n", page.borrow())?;
        }
        Ok(())
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

        println!(" {}", db);
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
        for i in 0..10_000 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            db.set(key.as_bytes(), value.as_bytes())?;
        }

        for i in 0..10_000 {
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

        println!(" {}", db);
        Ok(())
    }
}
