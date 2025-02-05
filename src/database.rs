use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::rc::Rc;

use tracing::{debug, error, info, warn};

use crate::storage::device::{SsdDevice, SsdError, SsdMetrics};
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
    /// Returns true if hot
    pub fn update_hotness(&mut self, hot_threshold: u32) -> bool {
        self.freq_accessed += 1;
        self.freq_accessed >= hot_threshold
    }
}

/// When `freq_accessed >= 2`, it is considered as hot data.
/// Here `is_hot` is only used as a hint for PageManager.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct Location {
    pub page_id: u64,
    pub page_index: usize,
}

/// Page status in memory or on SSD, with additional "pool" information.
#[derive(Debug)]
struct PageStatus {
    in_memory: Option<Rc<RefCell<Page>>>,
    is_hot: bool,
    free_space: usize,
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
    fn ensure_page_loaded(&mut self, page_id: u64) -> Result<Rc<RefCell<Page>>, PageManagerError> {
        if let Some(status) = self.pages.get(&page_id) {
            if let Some(page) = &status.in_memory {
                return Ok(Rc::clone(page));
            }
        }

        let page = self.device.read_page(page_id)?;
        let rc_page = Rc::new(RefCell::new(page));
        self.pages.insert(
            page_id,
            PageStatus {
                in_memory: Some(Rc::clone(&rc_page)),
                is_hot: false,
                free_space: rc_page.borrow().capacity() - rc_page.borrow().size(),
            },
        );
        Ok(rc_page)
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
            if let Some(page_rc) = &status.in_memory {
                if status.is_hot != is_hot {
                    continue;
                }
                if status.free_space >= required_space {
                    let mut page = page_rc.borrow_mut();
                    if let Some(page_index) = page.push_entry(key, value) {
                        debug!("Added entry to existing page {}", page_id);
                        // Write back to SSD
                        self.device.write_page(&mut page)?;
                        return Ok(Some(Location {
                            page_id,
                            page_index,
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
            let rc_page = Rc::new(RefCell::new(new_page));

            let free_space = {
                let page = rc_page.borrow();
                page.free_space() as usize
            };
            self.pages.insert(
                page_id,
                PageStatus {
                    in_memory: Some(rc_page),
                    is_hot,
                    free_space,
                },
            );
            self.next_id += 1;
            return Ok(Some(Location {
                page_id,
                page_index,
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
            if let Some(status) = self.pages.get_mut(&loc.page_id) {
                status.in_memory = None;
            }
        }
        Ok(location)
    }

    /// Read key from specified Page
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

/// Database structure, maintains a memory index and a PageManager.
#[derive(Debug)]
pub struct Database {
    /// BTreeMap maintains mapping from key to metadata
    index: BTreeMap<Vec<u8>, ObjectMetadata>,
    page_manager: PageManager,
    hot_threshold: u32,
}

impl Database {
    /// Create new database
    pub fn new<P: AsRef<Path>>(path: P, hot_threshold: u32) -> Result<Self, DatabaseError> {
        info!(
            "Initializing database with storage path {:?}, hot_threshold: {}",
            path.as_ref(),
            hot_threshold
        );
        Ok(Database {
            index: BTreeMap::new(),
            page_manager: PageManager::new(path, DEFAULT_PAGE_SIZE)?,
            hot_threshold,
        })
    }

    /// Set key-value pair
    pub fn set(&mut self, key: &[u8], value: &[u8]) -> Result<(), DatabaseError> {
        // Default to cold for new entries
        let mut is_hot = false;

        // If key exists, update hotness
        if let Some(metadata) = self.index.get_mut(key) {
            is_hot = metadata.update_hotness(self.hot_threshold);
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
            metadata.update_hotness(self.hot_threshold);

            // Read data from corresponding Page
            if let Some(value) = self.page_manager.get(&metadata.location, key)? {
                return Ok(value);
            }
            Err(DatabaseError::InvalidData)
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

    /// Get the SSD device metrics
    pub fn metrics(&self) -> &SsdMetrics {
        self.page_manager.device.metrics()
    }
}
