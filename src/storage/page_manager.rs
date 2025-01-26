use crate::database::Location;

use super::device::{SsdDevice, SsdError};
use super::page::Page;
use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::path::Path;
use std::rc::Rc;

// Wrapper for Page that implements Ord for the binary heap
// We want a max heap based on available space
#[derive(Debug)]
struct PageWrapper {
    page: Rc<RefCell<Page>>,
}

impl PageWrapper {
    fn new(page: Page) -> Self {
        PageWrapper {
            page: Rc::new(RefCell::new(page)),
        }
    }

    fn available_space(&self) -> usize {
        let page = self.page.borrow();
        page.capacity() - page.size()
    }
}

// Implement ordering based on available space
impl Ord for PageWrapper {
    fn cmp(&self, other: &Self) -> Ordering {
        self.available_space().cmp(&other.available_space())
    }
}

impl PartialOrd for PageWrapper {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for PageWrapper {
    fn eq(&self, other: &Self) -> bool {
        self.available_space() == other.available_space()
    }
}

impl Eq for PageWrapper {}

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
pub struct PageManager {
    pages: BinaryHeap<PageWrapper>,
    device: SsdDevice,
    next_id: u64,
    page_size: u32,
}

impl PageManager {
    pub fn new<P: AsRef<Path>>(path: P, page_size: u32) -> Result<Self, PageManagerError> {
        let device = SsdDevice::new(path, page_size)?;
        Ok(PageManager {
            pages: BinaryHeap::new(),
            device,
            next_id: 0,
            page_size,
        })
    }

    // Try to find a page with enough space for the entry
    // If no suitable page exists, create a new one
    pub fn allocate_entry(
        &mut self,
        key: &[u8],
        value: &[u8],
    ) -> Result<Option<Location>, PageManagerError> {
        let required_space = key.len() + value.len() + 8; // 8 bytes for metadata

        // Try to find a page with enough space
        let mut found_page = None;
        if let Some(wrapper) = self.pages.pop() {
            let space = wrapper.available_space();
            if space >= required_space {
                found_page = Some(wrapper);
            } else {
                self.pages.push(wrapper);
            }
        }

        if let Some(wrapper) = found_page {
            let page_rc = Rc::clone(&wrapper.page);
            let mut page = page_rc.borrow_mut();
            if let Some(page_index) = page.push_entry(key, value) {
                let id = page.id();
                // Write the page to storage
                self.device.write_page(&mut *page)?;
                drop(page); // Explicitly drop the borrow
                self.pages.push(wrapper);
                return Ok(Some(Location {
                    page_id: id,
                    page_index,
                }));
            }
            drop(page);
            self.pages.push(wrapper);
        }

        // Create a new page if no existing page has enough space
        let mut new_page = Page::new(self.next_id, self.page_size);
        self.next_id += 1;

        if let Some(page_index) = new_page.push_entry(key, value) {
            let page_id = new_page.id();
            // Write the new page to storage
            self.device.write_page(&mut new_page)?;
            self.pages.push(PageWrapper::new(new_page));
            self.device.sync()?;
            Ok(Some(Location {
                page_id,
                page_index,
            }))
        } else {
            Ok(None) // Entry too large even for a new page
        }
    }

    // Remove an entry from a specific page
    pub fn remove_entry(&mut self, page_id: u64, key: &[u8]) -> Result<bool, PageManagerError> {
        // Try to find the page in memory
        let mut found_page = None;
        let mut temp_heap = BinaryHeap::new();

        while let Some(wrapper) = self.pages.pop() {
            let is_target = {
                let page = wrapper.page.borrow();
                page.id() == page_id
            };
            if is_target {
                found_page = Some(wrapper);
                break;
            }
            temp_heap.push(wrapper);
        }

        // Restore other pages
        while let Some(wrapper) = temp_heap.pop() {
            self.pages.push(wrapper);
        }

        if let Some(wrapper) = found_page {
            let page_rc = Rc::clone(&wrapper.page);
            let mut page = page_rc.borrow_mut();
            let result = page.remove_entry(key);
            if result {
                // Update the page in storage
                self.device.write_page(&mut *page)?;
                self.device.sync()?;
            }
            drop(page); // Explicitly drop the borrow
            self.pages.push(wrapper);
            Ok(result)
        } else {
            // Try to load the page from storage
            match self.device.read_page(page_id) {
                Ok(mut page) => {
                    let result = page.remove_entry(key);
                    if result {
                        self.device.write_page(&mut page)?;
                        self.device.sync()?;
                    }
                    self.pages.push(PageWrapper::new(page));
                    Ok(result)
                }
                Err(SsdError::InvalidPageId) => Ok(false),
                Err(e) => Err(e.into()),
            }
        }
    }

    // Get the number of pages being managed
    pub fn page_count(&self) -> usize {
        self.pages.len()
    }

    // Get the total capacity across all pages
    pub fn total_capacity(&self) -> usize {
        self.pages.iter().map(|w| w.page.borrow().capacity()).sum()
    }

    // Get the total used space across all pages
    pub fn total_used_space(&self) -> usize {
        self.pages.iter().map(|w| w.page.borrow().size()).sum()
    }

    // Get a page, returns a shared reference that can be mutably borrowed
    pub fn get(
        &mut self,
        location: &Location,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, PageManagerError> {
        // If not in memory, try to read from storage
        match self.device.read_page(location.page_id) {
            Ok(page) => {
                let wrapper = PageWrapper::new(page);
                let page_rc = Rc::clone(&wrapper.page);
                self.pages.push(wrapper);
                let value = { page_rc.borrow().get(location.page_index, key) };
                Ok(value)
            }
            Err(SsdError::InvalidPageId) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    // Get an iterator over all pages
    pub fn iter_pages(&self) -> impl Iterator<Item = Rc<RefCell<Page>>> + '_ {
        self.pages.iter().map(|w| Rc::clone(&w.page))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_page_allocation() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.db");
        let mut manager = PageManager::new(file_path, 4096).unwrap();

        // Allocate a small entry
        let key1 = b"key1";
        let value1 = b"value1";
        let location0 = manager.allocate_entry(key1, value1).unwrap().unwrap();

        assert_eq!(location0.page_id, 0);
        assert_eq!(location0.page_index, 0);
        // Allocate another entry - should use the same page
        let key2 = b"key2";
        let value2 = b"value2";
        let location1 = manager.allocate_entry(key2, value2).unwrap().unwrap();
        assert_eq!(location1.page_id, 0);
        assert_eq!(location1.page_index, 1);
        assert_eq!(manager.page_count(), 1);
    }
}
