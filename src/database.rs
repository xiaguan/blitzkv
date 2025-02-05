use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, HashMap};
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
    Memory(Rc<RefCell<Page>>),
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

/// PagePool manages a set of pages with similar hotness characteristics
#[derive(Debug)]
struct PagePool {
    pages: HashMap<u64, PageStatus>,
    space_index: BTreeMap<u32, BTreeSet<u64>>, // available_space -> page_ids
    page_space_index: HashMap<u64, u32>,       // page_id -> available_space
    pre_allocated: Vec<u64>,                   // Pre-allocated empty page IDs
}

impl PagePool {
    fn new() -> Self {
        PagePool {
            pages: HashMap::new(),
            space_index: BTreeMap::new(),
            page_space_index: HashMap::new(),
            pre_allocated: Vec::new(),
        }
    }

    fn update_space_index(&mut self, page_id: u64, available_space: u32) {
        // If page exists in index, remove it from old space bucket
        if let Some(old_space) = self.page_space_index.get(&page_id) {
            if let Some(pages) = self.space_index.get_mut(old_space) {
                pages.remove(&page_id);
                // Remove bucket if empty
                if pages.is_empty() {
                    self.space_index.remove(old_space);
                }
            }
        }

        // Add to new space bucket
        self.space_index
            .entry(available_space)
            .or_insert_with(BTreeSet::new)
            .insert(page_id);
        self.page_space_index.insert(page_id, available_space);
    }

    fn find_page_with_space(&self, required_space: u32) -> Option<u64> {
        // Find first page with enough space
        for (_, pages) in self.space_index.range(required_space..) {
            if let Some(&page_id) = pages.first() {
                return Some(page_id);
            }
        }
        None
    }
}

// when page pool drop,print the page pool info
impl Drop for PagePool {
    fn drop(&mut self) {
        info!("PagePool has {} pages", self.pages.len());
    }
}

/// PageManager is responsible for managing memory pages and SSD pages, distinguishing between "cold" and "hot" data.
#[derive(Debug)]
struct PageManager {
    hot_pool: PagePool,
    cold_pool: PagePool,
    device: SsdDevice,
    next_id: u64,
    page_size: u32,
}

impl PageManager {
    /// Create a new PageManager
    fn new<P: AsRef<Path>>(path: P, page_size: u32) -> Result<Self, PageManagerError> {
        info!("Initializing SSD device at path {:?}", path.as_ref());
        let device = SsdDevice::new(path, page_size)?;
        let mut pm = PageManager {
            hot_pool: PagePool::new(),
            cold_pool: PagePool::new(),
            device,
            next_id: 0,
            page_size,
        };

        // Pre-allocate some pages for each pool
        pm.pre_allocate_pages(false, 10)?; // Cold pool
        pm.pre_allocate_pages(true, 10)?; // Hot pool

        Ok(pm)
    }

    /// Pre-allocate empty pages for the specified pool
    fn pre_allocate_pages(&mut self, is_hot: bool, count: usize) -> Result<(), PageManagerError> {
        let pool = if is_hot {
            &mut self.hot_pool
        } else {
            &mut self.cold_pool
        };

        for _ in 0..count {
            let page_id = self.next_id;

            pool.pre_allocated.push(page_id);
            pool.update_space_index(page_id, self.page_size);

            self.next_id += 1;
        }

        Ok(())
    }

    /// Find the corresponding Page in the specified pool, load from SSD if it doesn't exist.
    fn ensure_page_loaded(
        &mut self,
        page_id: u64,
        is_hot: bool,
    ) -> Result<Rc<RefCell<Page>>, PageManagerError> {
        let pool = if is_hot {
            &mut self.hot_pool
        } else {
            &mut self.cold_pool
        };

        match pool.pages.get(&page_id) {
            Some(PageStatus::Memory(page)) => Ok(Rc::clone(page)),
            Some(PageStatus::Ssd) | None => {
                // If it doesn't exist or is only on SSD, need to read from SSD to memory
                debug!("Loading page {} from SSD (hot: {})", page_id, is_hot);
                let page = self.device.read_page(page_id)?;
                let rc_page = Rc::new(RefCell::new(page));

                // Update space index
                let available_space = self.page_size - rc_page.borrow().size() as u32;
                pool.update_space_index(page_id, available_space);

                pool.pages
                    .insert(page_id, PageStatus::Memory(Rc::clone(&rc_page)));
                Ok(rc_page)
            }
        }
    }

    /// Move a page between hot and cold pools
    fn migrate_page(&mut self, page_id: u64, to_hot: bool) -> Result<(), PageManagerError> {
        let (source_pool, target_pool) = if to_hot {
            (&mut self.cold_pool, &mut self.hot_pool)
        } else {
            (&mut self.hot_pool, &mut self.cold_pool)
        };

        // Remove from source pool
        if let Some(status) = source_pool.pages.remove(&page_id) {
            match status {
                PageStatus::Memory(page) => {
                    // Update target pool's space index
                    let available_space = self.page_size - page.borrow().size() as u32;
                    target_pool.update_space_index(page_id, available_space);

                    // Insert into target pool
                    target_pool.pages.insert(page_id, PageStatus::Memory(page));
                }
                PageStatus::Ssd => {
                    // Just move the SSD status to target pool
                    target_pool.pages.insert(page_id, PageStatus::Ssd);
                }
            }
        }
        Ok(())
    }

    /// Find a Page in memory (hot or cold Pool) that can fit (key,value); if none exists, create a new Page.
    fn set_inner(
        &mut self,
        key: &[u8],
        value: &[u8],
        is_hot: bool,
    ) -> Result<Option<Location>, PageManagerError> {
        let required_space = (key.len() + value.len() + 8) as u32; // 8 bytes for entry metadata
        let pool = if is_hot {
            &mut self.hot_pool
        } else {
            &mut self.cold_pool
        };

        // For hot data, try to find a page that already has hot data and has enough space
        let page_id = pool.find_page_with_space(required_space);

        // Try to use an existing page with enough space
        if let Some(page_id) = page_id {
            let page_rc = {
                if let Some(PageStatus::Memory(page_rc)) = pool.pages.get(&page_id) {
                    Some(page_rc.clone())
                } else {
                    None
                }
            };
            if let Some(page_rc) = page_rc {
                let mut page = page_rc.borrow_mut();
                if let Some(page_index) = page.push_entry(key, value) {
                    debug!("Added entry to existing page {}", page_id);
                    // Update space index
                    let new_space = page.capacity() as u32 - page.size() as u32;
                    pool.update_space_index(page_id, new_space);
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

        // Try to use a pre-allocated page
        if let Some(page_id) = pool.pre_allocated.pop() {
            let mut new_page = Page::new(page_id, self.page_size);
            if let Some(page_index) = new_page.push_entry(key, value) {
                debug!("Using pre-allocated page {} for entry", page_id);
                self.device.write_page(&mut new_page)?;
                let rc_page = Rc::new(RefCell::new(new_page));
                pool.pages
                    .insert(page_id, PageStatus::Memory(Rc::clone(&rc_page)));

                // Update space index
                let available_space = self.page_size - rc_page.borrow().size() as u32;
                pool.update_space_index(page_id, available_space);

                // If pre-allocated pages are running low, allocate more
                if pool.pre_allocated.len() < 5 {
                    self.pre_allocate_pages(is_hot, 5)?;
                }

                return Ok(Some(Location {
                    page_id,
                    page_index,
                    is_hot,
                }));
            }
        }

        // If no pre-allocated pages or entry doesn't fit, create a new page
        let page_id = self.next_id;
        let mut new_page = Page::new(page_id, self.page_size);
        if let Some(page_index) = new_page.push_entry(key, value) {
            debug!("Creating new page {} for entry", page_id);
            self.device.write_page(&mut new_page)?;
            let rc_page = Rc::new(RefCell::new(new_page));
            pool.pages
                .insert(page_id, PageStatus::Memory(Rc::clone(&rc_page)));

            // Update space index
            let available_space = self.page_size - rc_page.borrow().size() as u32;
            pool.update_space_index(page_id, available_space);

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

    /// Public set interface: marks Page as Ssd after writing for cold data,
    /// but keeps hot data in memory for better performance.
    pub fn set(
        &mut self,
        key: &[u8],
        value: &[u8],
        is_hot: bool,
    ) -> Result<Option<Location>, PageManagerError> {
        let location = self.set_inner(key, value, is_hot)?;
        if let Some(loc) = &location {
            let pool = if is_hot {
                &mut self.hot_pool
            } else {
                &mut self.cold_pool
            };
            pool.pages.insert(loc.page_id, PageStatus::Ssd);
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
        let mut is_hot = false;

        // If key exists, update frequency and hotness
        if let Some(metadata) = self.index.get_mut(key) {
            is_hot = metadata.update_hotness();
        }

        // Call PageManager to write with updated hotness
        match self.page_manager.set(key, value, is_hot)? {
            Some(location) => {
                if let Some(metadata) = self.index.get_mut(key) {
                    metadata.location = location;
                } else {
                    self.index.insert(
                        key.to_vec(),
                        ObjectMetadata {
                            location,
                            size: value.len() as u32,
                            freq_accessed: 1,
                        },
                    );
                }
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
                // Migrate page between pools if hotness changed
                self.page_manager
                    .migrate_page(metadata.location.page_id, metadata.location.is_hot)?;
            }

            // Read data from corresponding Page
            if let Some(value) = self.page_manager.get(&metadata.location, key)? {
                Ok(value)
            } else {
                Err(DatabaseError::InvalidData)
            }
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
