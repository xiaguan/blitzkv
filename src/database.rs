use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::rc::Rc;

use tracing::{debug, error, info, warn};

use crate::storage::device::{SsdDevice, SsdError};
use crate::storage::page::Page;

const DEFAULT_PAGE_SIZE: u32 = 4096; // 4KB page size

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct ObjectMetadata {
    pub location: Location, // Location of the object in the page
    pub size: u32,          // Size of the object in bytes
    pub last_accessed: u64, // Timestamp of the last access
    pub freq_accessed: u32, // Frequency of access
}

/// Represents the location of an entry in a page.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct Location {
    pub page_id: u64,
    pub page_index: usize,
}

/// Represents the status of a page, either cached in memory or only stored on SSD.
#[derive(Debug)]
enum PageStatus {
    Memory(Rc<RefCell<Page>>),
    Ssd, // Indicates the page is only on SSD.
}

/// Errors related to page management operations.
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

/// Errors that can occur at the database level.
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

/// Manages pages in memory and on SSD.
#[derive(Debug)]
struct PageManager {
    pages: HashMap<u64, PageStatus>,
    device: SsdDevice,
    next_id: u64,
    page_size: u32,
}

impl PageManager {
    /// Create a new PageManager with the given storage path and page size.
    fn new<P: AsRef<Path>>(path: P, page_size: u32) -> Result<Self, PageManagerError> {
        info!("Initializing SSD device at path {:?}", path.as_ref());
        let device = SsdDevice::new(path, page_size)?;
        Ok(PageManager {
            pages: HashMap::new(),
            device,
            next_id: 0,
            page_size,
        })
    }

    /// Ensure that the page with the given ID is loaded into memory.
    /// If the page is only on SSD, load it and cache it.
    fn ensure_page_loaded(&mut self, page_id: u64) -> Result<Rc<RefCell<Page>>, PageManagerError> {
        match self.pages.get(&page_id) {
            Some(PageStatus::Memory(page)) => Ok(Rc::clone(page)),
            Some(PageStatus::Ssd) | None => {
                debug!("Loading page {} from SSD", page_id);
                let page = self.device.read_page(page_id)?;
                let rc_page = Rc::new(RefCell::new(page));
                self.pages
                    .insert(page_id, PageStatus::Memory(Rc::clone(&rc_page)));
                Ok(rc_page)
            }
        }
    }

    /// Internal method to add an entry to a page.
    /// It first attempts to insert into an existing page with enough space.
    /// If none is found, it creates a new page.
    fn set_inner(
        &mut self,
        key: &[u8],
        value: &[u8],
    ) -> Result<Option<Location>, PageManagerError> {
        let required_space = key.len() + value.len() + 8; // 8 bytes reserved for metadata

        // Attempt to add the entry to an existing memory-cached page.
        for (&page_id, status) in self.pages.iter() {
            if let PageStatus::Memory(page_rc) = status {
                let mut page = page_rc.borrow_mut();
                let available_space = page.capacity() - page.size();
                if available_space >= required_space {
                    if let Some(page_index) = page.push_entry(key, value) {
                        debug!("Added entry to existing page {}", page_id);
                        self.device.write_page(&mut page)?;
                        return Ok(Some(Location {
                            page_id,
                            page_index,
                        }));
                    }
                }
            }
        }

        // No suitable page found; create a new one.
        let page_id = self.next_id;
        let mut new_page = Page::new(page_id, self.page_size);
        if let Some(page_index) = new_page.push_entry(key, value) {
            info!("Creating new page {} for entry", page_id);
            self.device.write_page(&mut new_page)?;
            self.pages
                .insert(page_id, PageStatus::Memory(Rc::new(RefCell::new(new_page))));
            self.next_id += 1;
            return Ok(Some(Location {
                page_id,
                page_index,
            }));
        }

        // The entry is too large to fit in a new page.
        warn!(
            "Entry too large to fit in a new page (page id: {})",
            page_id
        );
        Ok(None)
    }

    /// Public method to insert an entry.
    /// After writing to the page, marks the page as flushed to SSD.
    pub fn set(&mut self, key: &[u8], value: &[u8]) -> Result<Option<Location>, PageManagerError> {
        let location = self.set_inner(key, value)?;
        if let Some(loc) = location {
            // Mark the page as flushed to SSD to indicate it is not actively cached.
            self.pages.insert(loc.page_id, PageStatus::Ssd);
        }
        Ok(location)
    }

    /// Delete an entry from the specified page.
    pub fn delete(&mut self, page_id: u64, key: &[u8]) -> Result<bool, PageManagerError> {
        let page_rc = self.ensure_page_loaded(page_id)?;
        let mut page = page_rc.borrow_mut();
        if page.remove_entry(key) {
            info!("Deleted key from page {}", page_id);
            self.device.write_page(&mut page)?;
            Ok(true)
        } else {
            debug!("Key not found in page {}", page_id);
            Ok(false)
        }
    }

    /// Retrieve an entry from a page.
    pub fn get(
        &mut self,
        location: &Location,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, PageManagerError> {
        let page_rc = self.ensure_page_loaded(location.page_id)?;
        let page = page_rc.borrow();
        Ok(page.get(location.page_index, key))
    }
}

/// A simple key-value database that uses PageManager for storage.
#[derive(Debug)]
pub struct Database {
    /// In-memory index mapping keys to their metadata including storage location.
    index: BTreeMap<Vec<u8>, ObjectMetadata>,
    /// Manages pages stored on SSD.
    page_manager: PageManager,
}

impl Database {
    /// Create a new database with the given storage path.
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, DatabaseError> {
        info!(
            "Initializing database with storage path {:?}",
            path.as_ref()
        );
        Ok(Database {
            index: BTreeMap::new(),
            page_manager: PageManager::new(path, DEFAULT_PAGE_SIZE)?,
        })
    }

    /// Insert or update a key-value pair in the database.
    pub fn set(&mut self, key: &[u8], value: &[u8]) -> Result<(), DatabaseError> {
        // Attemtp to get the current metadata for the key.
        if let Some(metadata) = self.index.get_mut(key) {
            // If the key already exists, update its metadata.
            metadata.freq_accessed += 1;
            if metadata.freq_accessed > 2 {
                info!(
                    "Key '{}' is hot, moving to faster storage",
                    String::from_utf8_lossy(key)
                );
            }
        }
        match self.page_manager.set(key, value)? {
            Some(location) => {
                debug!(
                    "Writing key '{}' to location {:?}",
                    String::from_utf8_lossy(key),
                    location
                );
                let metadata = ObjectMetadata {
                    location,
                    size: (key.len() + value.len()) as u32,
                    last_accessed: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                    freq_accessed: 1,
                };
                self.index.insert(key.to_vec(), metadata);
                Ok(())
            }
            None => {
                error!(
                    "Failed to allocate space for key '{}'",
                    String::from_utf8_lossy(key)
                );
                Err(DatabaseError::StorageFull)
            }
        }
    }

    /// Retrieve the value associated with a key.
    pub fn get(&mut self, key: &[u8]) -> Result<Vec<u8>, DatabaseError> {
        if let Some(metadata) = self.index.get_mut(key) {
            // Update access statistics
            metadata.last_accessed = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();
            metadata.freq_accessed += 1;
            debug!(
                "Retrieving key '{}' from location {:?}",
                String::from_utf8_lossy(key),
                metadata.location
            );
            if let Some(value) = self.page_manager.get(&metadata.location, key)? {
                return Ok(value);
            }
            Err(DatabaseError::InvalidData)
        } else {
            Err(DatabaseError::KeyNotFound)
        }
    }

    /// Delete a key-value pair from the database.
    pub fn delete(&mut self, key: &[u8]) -> Result<(), DatabaseError> {
        if let Some(metadata) = self.index.get(key) {
            if self.page_manager.delete(metadata.location.page_id, key)? {
                debug!(
                    "Deleted key '{}' from database",
                    String::from_utf8_lossy(key)
                );
                self.index.remove(key);
                Ok(())
            } else {
                error!(
                    "Failed to delete key '{}' from page",
                    String::from_utf8_lossy(key)
                );
                Err(DatabaseError::InvalidData)
            }
        } else {
            Err(DatabaseError::KeyNotFound)
        }
    }

    /// Retrieve all keys in the database in sorted order.
    pub fn keys(&self) -> Vec<Vec<u8>> {
        self.index.keys().cloned().collect()
    }

    /// Return the number of key-value pairs in the database.
    pub fn len(&self) -> usize {
        self.index.len()
    }

    /// Check if the database is empty.
    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }
}
