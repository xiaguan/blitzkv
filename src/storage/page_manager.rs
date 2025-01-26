use crate::database::Location;

use super::device::{SsdDevice, SsdError};
use super::page::Page;
use std::cell::RefCell;

use std::collections::HashMap;
use std::path::Path;
use std::rc::Rc;

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
pub struct PageManager {
    pages: HashMap<u64, PageStatus>,
    device: SsdDevice,
    next_id: u64,
    page_size: u32,
}

impl PageManager {
    pub fn new<P: AsRef<Path>>(path: P, page_size: u32) -> Result<Self, PageManagerError> {
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
    pub fn set(&mut self, key: &[u8], value: &[u8]) -> Result<Option<Location>, PageManagerError> {
        let location = self.set_inner(key, value)?;
        if let Some(location) = location {
            self.pages.insert(location.page_id, PageStatus::Ssd);
        }
        return Ok(location);
    }

    // Remove an entry from a specific page
    pub fn delete(&mut self, page_id: u64, key: &[u8]) -> Result<bool, PageManagerError> {
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

    // Get the number of pages being managed
    pub fn page_count(&self) -> usize {
        self.pages.len()
    }

    // Get a page, returns a shared reference that can be mutably borrowed
    pub fn get(
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
