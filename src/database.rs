use crate::storage::unit::StorageUnit;
use std::collections::BTreeMap;

const DEFAULT_STORAGE_SIZE: u32 = 400; // 4KB default storage unit size

#[derive(Debug)]
pub struct Database {
    // Memory index mapping keys to their location
    index: BTreeMap<Vec<u8>, Location>,
    // Collection of storage units
    storage_units: Vec<StorageUnit>,
    // Current active storage unit for writes
    current_unit_id: u64,
}

impl std::fmt::Display for Database {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        for unit in &self.storage_units {
            write!(f, "{}\n", unit)?;
        }
        Ok(())
    }
}

#[derive(Debug, Copy, Clone)]
struct Location {
    unit_id: u64,
    offset: u32,
}

#[derive(Debug)]
pub enum DatabaseError {
    KeyNotFound,
    StorageFull,
    InvalidData,
}

impl Database {
    pub fn new() -> Self {
        let initial_unit = StorageUnit::new(0, DEFAULT_STORAGE_SIZE);
        Database {
            index: BTreeMap::new(),
            storage_units: vec![initial_unit],
            current_unit_id: 0,
        }
    }

    pub fn set(&mut self, key: &[u8], value: &[u8]) -> Result<(), DatabaseError> {
        // Try to write to current storage unit
        let current_unit = self.storage_units.last_mut().unwrap();

        if let Some(offset) = current_unit.push_entry(key, value) {
            // Update index with new location
            self.index.insert(
                key.to_vec(),
                Location {
                    unit_id: self.current_unit_id,
                    offset,
                },
            );
            Ok(())
        } else {
            // Current unit is full, create new one
            self.current_unit_id += 1;
            let mut new_unit = StorageUnit::new(self.current_unit_id, DEFAULT_STORAGE_SIZE);

            // Try writing to new unit
            if let Some(offset) = new_unit.push_entry(key, value) {
                self.storage_units.push(new_unit);
                self.index.insert(
                    key.to_vec(),
                    Location {
                        unit_id: self.current_unit_id,
                        offset,
                    },
                );
                Ok(())
            } else {
                Err(DatabaseError::StorageFull)
            }
        }
    }

    pub fn get(&self, key: &[u8]) -> Result<Vec<u8>, DatabaseError> {
        // Look up key in index
        if let Some(location) = self.index.get(key) {
            // Find the storage unit
            if let Some(unit) = self
                .storage_units
                .iter()
                .find(|u| u.id() == location.unit_id)
            {
                // Iterate through entries to find the matching key
                for entry in unit.iter() {
                    if entry.key() == key {
                        return Ok(entry.value().to_vec());
                    }
                }
            }
            Err(DatabaseError::InvalidData)
        } else {
            Err(DatabaseError::KeyNotFound)
        }
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<(), DatabaseError> {
        if self.index.remove(key).is_some() {
            // Key was found and removed from index
            // Note: The actual data remains in storage unit but becomes inaccessible
            // A future compaction process could reclaim this space
            Ok(())
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

    // Get total size of all storage units
    pub fn total_size(&self) -> usize {
        self.storage_units.iter().map(|unit| unit.size()).sum()
    }

    // Get total capacity of all storage units
    pub fn total_capacity(&self) -> usize {
        self.storage_units.iter().map(|unit| unit.capacity()).sum()
    }

    // Calculate overall space amplification
    pub fn space_amplification(&self) -> f64 {
        let total_size = self.total_size() as f64;
        let data_size = self.index.len() as f64; // Simplified - actual data size would need key/value sizes
        total_size / data_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_operations() {
        let mut db = Database::new();

        // Test set
        db.set(b"key1", b"value1").unwrap();
        db.set(b"key2", b"value2").unwrap();

        println!(" {}", db);
        // Test get
        assert_eq!(db.get(b"key1").unwrap(), b"value1");
        assert_eq!(db.get(b"key2").unwrap(), b"value2");
        assert!(matches!(
            db.get(b"nonexistent"),
            Err(DatabaseError::KeyNotFound)
        ));

        // Test delete
        db.delete(b"key1").unwrap();
        assert!(matches!(db.get(b"key1"), Err(DatabaseError::KeyNotFound)));

        // Test length
        assert_eq!(db.len(), 1);
        assert!(!db.is_empty());
    }

    #[test]
    fn test_storage_unit_rotation() {
        let mut db = Database::new();

        // Fill up multiple storage units
        for i in 0..1000 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            db.set(key.as_bytes(), value.as_bytes()).unwrap();
        }

        println!(" {}", db);

        // random remove some
        for i in 0..1000 {
            if i % 2 == 0 {
                let key = format!("key{}", i);
                db.delete(key.as_bytes()).unwrap();
            }
        }

        println!(" {}", db);
    }
}
