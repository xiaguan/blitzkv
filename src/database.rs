use hashlink::LruCache;
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::rc::Rc;
use std::time::{SystemTime, UNIX_EPOCH};

use hdrhistogram::Histogram;
use tracing::{debug, error, info, warn};

use crate::storage::device::{SsdDevice, SsdError, SsdMetrics};
use crate::storage::page::Page;

const DEFAULT_PAGE_SIZE: u32 = 4096; // 4KB page size
const DEFAULT_CACHE_SIZE: usize = 50; // 100 pages in cache

const DECAY_RATE: f64 = 0.2; // Decay rate parameter lambda

/// `ObjectMetadata` keeps track of access patterns with decay.
#[derive(Debug, Copy, Clone)]
pub struct ObjectMetadata {
    pub location: Location,
    pub size: u32,
    pub freq_accessed: f64, // access frequency with decay
    pub last_access: u64,   // timestamp of last access
}

impl ObjectMetadata {
    /// Update hotness based on access frequency with exponential decay
    /// Returns true if hot
    pub fn update_hotness(&mut self, hot_threshold: u32) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let time_diff = (now - self.last_access) as f64;
        // Apply exponential decay: old_freq * e^(-λt) + 1
        self.freq_accessed = self.freq_accessed * (-DECAY_RATE * time_diff).exp() + 1.0;
        self.last_access = now;
        self.freq_accessed >= hot_threshold as f64
    }
}

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
    pages: HashMap<u64, PageStatus>,
    device: SsdDevice,
    next_id: u64,
    page_size: u32,
    page_cache: LruCache<u64, Rc<RefCell<Page>>>,
    hit_count: usize,
    miss_count: usize,

    hot_free_spaces: BTreeMap<usize, Vec<u64>>,
    cold_free_spaces: BTreeMap<usize, Vec<u64>>,
}

impl PageManager {
    fn new<P: AsRef<Path>>(path: P, page_size: u32) -> Result<Self, PageManagerError> {
        info!("Initializing SSD device at path {:?}", path.as_ref());
        let device = SsdDevice::new(path, page_size)?;
        Ok(PageManager {
            pages: HashMap::new(),
            device,
            next_id: 0,
            page_size,
            page_cache: LruCache::new(DEFAULT_CACHE_SIZE),
            hit_count: 0,
            miss_count: 0,
            hot_free_spaces: BTreeMap::new(),
            cold_free_spaces: BTreeMap::new(),
        })
    }

    fn find_suitable_page_id(&self, required_space: usize, is_hot: bool) -> Option<u64> {
        let map = if is_hot {
            &self.hot_free_spaces
        } else {
            &self.cold_free_spaces
        };

        let mut range = map.range(required_space..);
        if let Some((&free_space, page_ids)) = range.next() {
            assert!(free_space >= required_space);
            if !page_ids.is_empty() {
                return Some(page_ids[0]);
            }
        }
        None
    }

    fn update_free_space_index(
        &mut self,
        page_id: u64,
        old_free: usize,
        new_free: usize,
        is_hot: bool,
    ) {
        let map = if is_hot {
            &mut self.hot_free_spaces
        } else {
            &mut self.cold_free_spaces
        };

        if old_free > 0 {
            if let Some(page_list) = map.get_mut(&old_free) {
                if let Some(pos) = page_list.iter().position(|pid| *pid == page_id) {
                    page_list.swap_remove(pos);
                }
                if page_list.is_empty() {
                    map.remove(&old_free);
                }
            }
        }

        if new_free > 0 {
            map.entry(new_free).or_insert_with(Vec::new).push(page_id);
        }
    }

    fn ensure_page_loaded(&mut self, page_id: u64) -> Result<Rc<RefCell<Page>>, PageManagerError> {
        // First check the LRU cache
        if let Some(page) = self.page_cache.get(&page_id) {
            self.hit_count += 1;
            return Ok(Rc::clone(page));
        }
        self.miss_count += 1;

        // Finally read from disk
        let page = self.device.read_page(page_id)?;
        let free_space = page.free_space() as usize;
        let rc_page = Rc::new(RefCell::new(page));

        let entry = self.pages.entry(page_id).or_insert_with(|| PageStatus {
            in_memory: None,
            is_hot: false,
            free_space,
        });
        entry.in_memory = Some(Rc::clone(&rc_page));
        entry.free_space = free_space;
        let is_hot = entry.is_hot;

        self.update_free_space_index(page_id, 0, free_space, is_hot);

        // Add to cache
        self.page_cache.insert(page_id, Rc::clone(&rc_page));

        Ok(rc_page)
    }

    fn set_inner(
        &mut self,
        key: &[u8],
        value: &[u8],
        is_hot: bool,
    ) -> Result<Option<Location>, PageManagerError> {
        let required_space = key.len() + value.len() + 8;

        if let Some(page_id) = self.find_suitable_page_id(required_space, is_hot) {
            let page_rc = self.ensure_page_loaded(page_id)?;
            let old_free = {
                let status = self.pages.get(&page_id).unwrap();
                status.free_space
            };

            {
                let mut page = page_rc.borrow_mut();
                if let Some(page_index) = page.push_entry(key, value) {
                    self.device.write_page(&mut page)?;

                    let new_free = page.free_space() as usize;
                    let status = self.pages.get_mut(&page_id).unwrap();
                    status.free_space = new_free;

                    self.update_free_space_index(page_id, old_free, new_free, is_hot);

                    return Ok(Some(Location {
                        page_id,
                        page_index,
                    }));
                }
            }
        }

        let page_id = self.next_id;
        let mut new_page = Page::new(page_id, self.page_size);
        if let Some(page_index) = new_page.push_entry(key, value) {
            debug!("Creating new page {} for entry", page_id);
            self.device.write_page(&mut new_page)?;
            let free_space = new_page.free_space() as usize;
            let rc_page = Rc::new(RefCell::new(new_page));

            self.pages.insert(
                page_id,
                PageStatus {
                    in_memory: Some(Rc::clone(&rc_page)),
                    is_hot,
                    free_space,
                },
            );

            self.update_free_space_index(page_id, 0, free_space, is_hot);

            // Add new page to cache
            self.page_cache.insert(page_id, rc_page);

            self.next_id += 1;
            Ok(Some(Location {
                page_id,
                page_index,
            }))
        } else {
            warn!(
                "Entry too large to fit in a new page (page id: {})",
                page_id
            );
            Ok(None)
        }
    }

    pub fn set(
        &mut self,
        key: &[u8],
        value: &[u8],
        is_hot: bool,
    ) -> Result<Option<Location>, PageManagerError> {
        let location = self.set_inner(key, value, is_hot)?;
        // After writing, we keep the page in memory since it's already up to date
        // Only update free space tracking
        if let Some(loc) = &location {
            if let Some(status) = self.pages.get(&loc.page_id) {
                let old_free = status.free_space;
                let is_hot = status.is_hot;
                if let Some(page_rc) = &status.in_memory {
                    let new_free = page_rc.borrow().free_space() as usize;
                    if let Some(status) = self.pages.get_mut(&loc.page_id) {
                        status.free_space = new_free;
                    }
                    self.update_free_space_index(loc.page_id, old_free, new_free, is_hot);
                }
            }
        }
        Ok(location)
    }

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
    /// Histogram for tracking access frequencies
    freq_histogram: Histogram<u64>,
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
            freq_histogram: Histogram::<u64>::new(3).unwrap(),
        })
    }

    /// Set key-value pair
    pub fn set(&mut self, key: &[u8], value: &[u8]) -> Result<(), DatabaseError> {
        // Default to cold for new entries
        let mut is_hot = false;

        // If key exists, update hotness
        if let Some(metadata) = self.index.get_mut(key) {
            is_hot = metadata.update_hotness(self.hot_threshold);
            // Record frequency in histogram
            self.freq_histogram
                .record(metadata.freq_accessed as u64)
                .unwrap();
        }

        // Call PageManager to write
        match self.page_manager.set(key, value, is_hot)? {
            Some(location) => {
                debug!(
                    "Writing key '{}' to location {:?}",
                    String::from_utf8_lossy(key),
                    location
                );
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs();
                let metadata = ObjectMetadata {
                    location,
                    size: (key.len() + value.len()) as u32,
                    freq_accessed: 1.0,
                    last_access: now,
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

    /// Get the frequency histogram
    pub fn freq_histogram(&self) -> &Histogram<u64> {
        &self.freq_histogram
    }

    pub fn hit_ratio(&self) -> f64 {
        info!(
            "Hit count {}, miss count {}",
            self.page_manager.hit_count, self.page_manager.miss_count
        );
        (self.page_manager.hit_count as f64)
            / (self.page_manager.hit_count as f64 + self.page_manager.miss_count as f64)
    }
}
