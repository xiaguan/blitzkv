use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::rc::Rc;

use tracing::{debug, error, info, warn};

use crate::storage::device::{SsdDevice, SsdError};
use crate::storage::page::Page;

const DEFAULT_PAGE_SIZE: u32 = 4096; // 4KB page size

/// `ObjectMetadata` only keeps `freq_accessed` to determine data hotness.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct ObjectMetadata {
    pub location: Location,
    pub size: u32,
    pub freq_accessed: u32, // access frequency
}

impl ObjectMetadata {
    /// Update hotness based on access frequency
    /// Returns true if hotness status changed
    pub fn update_hotness(&mut self) -> bool {
        self.freq_accessed += 1;
        let should_be_hot = self.freq_accessed >= 2;
        if should_be_hot != self.location.is_hot {
            self.location.is_hot = should_be_hot;
            true
        } else {
            false
        }
    }
}

/// When `freq_accessed >= 2`, it is considered as hot data.
/// Here `is_hot` is only used as a hint for PageManager.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct Location {
    pub page_id: u64,
    pub page_index: usize,
    pub is_hot: bool, // set to true when freq >= 2
}

/// Page status in memory or on SSD, with additional "pool" information.
#[derive(Debug)]
enum PageStatus {
    /// Memory(page, is_hot)
    /// - `page` represents the actual Page loaded into memory
    /// - `is_hot` is used to distinguish which pool this page belongs to
    Memory(Rc<RefCell<Page>>, bool),
    /// Only on SSD, not in memory
    Ssd,
}

/// PageManager related errors
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

/// Database level errors
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

/// PageManager is responsible for managing memory pages and SSD pages, distinguishing between "cold" and "hot" data.
#[derive(Debug)]
struct PageManager {
    /// Uses a single HashMap, but includes a bool in entries to indicate hot or cold.
    pages: HashMap<u64, PageStatus>,
    device: SsdDevice,
    next_id: u64,
    page_size: u32,
}

impl PageManager {
    /// Create a new PageManager
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

    /// Find the corresponding Page in the specified pool (determined by `is_hot`), load from SSD if it doesn't exist.
    fn ensure_page_loaded(
        &mut self,
        page_id: u64,
        is_hot: bool,
    ) -> Result<Rc<RefCell<Page>>, PageManagerError> {
        match self.pages.get(&page_id) {
            Some(PageStatus::Memory(page, page_is_hot)) => {
                // If already in memory, return directly
                if *page_is_hot == is_hot {
                    return Ok(Rc::clone(page));
                } else {
                    // If pool doesn't match, we could do a "transfer" here, or just return directly.
                    // Simple handling: return existing page, but logic can be extended as needed.
                    return Ok(Rc::clone(page));
                }
            }
            Some(PageStatus::Ssd) | None => {
                // If it doesn't exist or is only on SSD, need to read from SSD to memory
                debug!("Loading page {} from SSD (hot: {})", page_id, is_hot);
                let page = self.device.read_page(page_id)?;
                let rc_page = Rc::new(RefCell::new(page));
                self.pages
                    .insert(page_id, PageStatus::Memory(Rc::clone(&rc_page), is_hot));
                Ok(rc_page)
            }
        }
    }

    /// Find a Page in memory (hot or cold Pool) that can fit (key,value); if none exists, create a new Page.
    fn set_inner(
        &mut self,
        key: &[u8],
        value: &[u8],
        is_hot: bool,
    ) -> Result<Option<Location>, PageManagerError> {
        let required_space = key.len() + value.len() + 8; // 8 bytes for entry metadata

        // Find a page that matches is_hot and has enough space
        for (&page_id, status) in self.pages.iter() {
            if let PageStatus::Memory(page_rc, page_is_hot) = status {
                if *page_is_hot != is_hot {
                    continue;
                }
                let mut page = page_rc.borrow_mut();
                let available_space = page.capacity() - page.size();
                if available_space >= required_space {
                    if let Some(page_index) = page.push_entry(key, value) {
                        debug!("Added entry to existing page {}", page_id);
                        // Write back to SSD
                        self.device.write_page(&mut page)?;
                        return Ok(Some(Location {
                            page_id,
                            page_index,
                            is_hot,
                        }));
                    }
                }
            }
        }

        // If no suitable page found, create a new page
        let page_id = self.next_id;
        let mut new_page = Page::new(page_id, self.page_size);
        if let Some(page_index) = new_page.push_entry(key, value) {
            debug!("Creating new page {} for entry", page_id);
            self.device.write_page(&mut new_page)?;
            self.pages.insert(
                page_id,
                PageStatus::Memory(Rc::new(RefCell::new(new_page)), is_hot),
            );
            self.next_id += 1;
            return Ok(Some(Location {
                page_id,
                page_index,
                is_hot,
            }));
        }

        // If entry doesn't fit in a new page, it's too large
        warn!(
            "Entry too large to fit in a new page (page id: {})",
            page_id
        );
        Ok(None)
    }

    /// Public set interface: marks Page as Ssd after writing (simply removes from memory).
    /// If you want to keep it in memory (e.g., for hot data), you can modify this behavior.
    pub fn set(
        &mut self,
        key: &[u8],
        value: &[u8],
        is_hot: bool,
    ) -> Result<Option<Location>, PageManagerError> {
        let location = self.set_inner(key, value, is_hot)?;
        if let Some(loc) = &location {
            // Mark as Ssd, won't stay in memory
            self.pages.insert(loc.page_id, PageStatus::Ssd);
        }
        Ok(location)
    }

    /// Delete key from specified Page
    pub fn delete(
        &mut self,
        page_id: u64,
        key: &[u8],
        is_hot: bool,
    ) -> Result<bool, PageManagerError> {
        let page_rc = self.ensure_page_loaded(page_id, is_hot)?;
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

    /// Read key from specified Page
    pub fn get(
        &mut self,
        location: &Location,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, PageManagerError> {
        let page_rc = self.ensure_page_loaded(location.page_id, location.is_hot)?;
        let page = page_rc.borrow();
        Ok(page.get(location.page_index, key))
    }
}

/// Database structure, maintains a memory index and a PageManager.
#[derive(Debug)]
pub struct Database {
    /// BTreeMap maintains mapping from key to metadata
    index: BTreeMap<Vec<u8>, ObjectMetadata>,
    page_manager: PageManager,
}

impl Database {
    /// Create new database
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

    /// Set key-value pair
    pub fn set(&mut self, key: &[u8], value: &[u8]) -> Result<(), DatabaseError> {
        // Default to cold for new entries
        let mut is_hot = false;

        // If key exists, update hotness
        if let Some(metadata) = self.index.get_mut(key) {
            if metadata.update_hotness() {
                debug!(
                    "Key '{}' hotness changed to {}",
                    String::from_utf8_lossy(key),
                    metadata.location.is_hot
                );
            }
            is_hot = metadata.location.is_hot;
        }

        // Call PageManager to write
        match self.page_manager.set(key, value, is_hot)? {
            Some(location) => {
                debug!(
                    "Writing key '{}' to location {:?}",
                    String::from_utf8_lossy(key),
                    location
                );
                let metadata = ObjectMetadata {
                    location,
                    size: (key.len() + value.len()) as u32,
                    freq_accessed: 1, // Updated access frequency
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

    /// Read value for key
    pub fn get(&mut self, key: &[u8]) -> Result<Vec<u8>, DatabaseError> {
        if let Some(metadata) = self.index.get_mut(key) {
            // Update hotness and log if changed
            if metadata.update_hotness() {
                debug!(
                    "Key '{}' hotness changed to {}",
                    String::from_utf8_lossy(key),
                    metadata.location.is_hot
                );
            }

            // Read data from corresponding Page
            if let Some(value) = self.page_manager.get(&metadata.location, key)? {
                return Ok(value);
            }
            Err(DatabaseError::InvalidData)
        } else {
            Err(DatabaseError::KeyNotFound)
        }
    }

    /// Delete specified key
    pub fn delete(&mut self, key: &[u8]) -> Result<(), DatabaseError> {
        if let Some(metadata) = self.index.get(key) {
            if self
                .page_manager
                .delete(metadata.location.page_id, key, metadata.location.is_hot)?
            {
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

    /// Return all keys (sorted)
    pub fn keys(&self) -> Vec<Vec<u8>> {
        self.index.keys().cloned().collect()
    }

    /// Number of keys in database
    pub fn len(&self) -> usize {
        self.index.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }
}
