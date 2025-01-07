use super::unit::Page;
use std::cmp::Ordering;
use std::collections::BinaryHeap;

// Wrapper for Page that implements Ord for the binary heap
// We want a max heap based on available space
#[derive(Debug)]
struct PageWrapper {
    page: Page,
}

impl PageWrapper {
    fn new(page: Page) -> Self {
        PageWrapper { page }
    }

    fn available_space(&self) -> usize {
        self.page.capacity() - self.page.size()
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
pub struct PageManager {
    pages: BinaryHeap<PageWrapper>,
    next_id: u64,
    page_size: u32,
}

impl PageManager {
    pub fn new(page_size: u32) -> Self {
        PageManager {
            pages: BinaryHeap::new(),
            next_id: 0,
            page_size,
        }
    }

    // Try to find a page with enough space for the entry
    // If no suitable page exists, create a new one
    pub fn allocate_entry(&mut self, key: &[u8], value: &[u8]) -> Option<u64> {
        let required_space = key.len() + value.len() + 8; // 8 bytes for metadata

        // Try to find a page with enough space
        if let Some(mut wrapper) = self.pages.pop() {
            if wrapper.available_space() >= required_space {
                // Use this page
                if let Some(_) = wrapper.page.push_entry(key, value) {
                    let page_id = wrapper.page.id();
                    self.pages.push(wrapper);
                    return Some(page_id);
                }
            }
            // Put the page back if we couldn't use it
            self.pages.push(wrapper);
        }

        // Create a new page if no existing page has enough space
        let mut new_page = Page::new(self.next_id, self.page_size);
        self.next_id += 1;

        if let Some(_) = new_page.push_entry(key, value) {
            let page_id = new_page.id();
            self.pages.push(PageWrapper::new(new_page));
            Some(page_id)
        } else {
            None // Entry too large even for a new page
        }
    }

    // Remove an entry from a specific page
    pub fn remove_entry(&mut self, page_id: u64, key: &[u8]) -> bool {
        // Remove the page from the heap
        let mut target_page = None;
        let mut temp_heap = BinaryHeap::new();

        while let Some(wrapper) = self.pages.pop() {
            if wrapper.page.id() == page_id {
                target_page = Some(wrapper);
                break;
            }
            temp_heap.push(wrapper);
        }

        // Restore other pages
        while let Some(wrapper) = temp_heap.pop() {
            self.pages.push(wrapper);
        }

        // Remove the entry if we found the page
        if let Some(mut wrapper) = target_page {
            let result = wrapper.page.remove_entry(key);
            self.pages.push(wrapper);
            result
        } else {
            false
        }
    }

    // Get the number of pages being managed
    pub fn page_count(&self) -> usize {
        self.pages.len()
    }

    // Get the total capacity across all pages
    pub fn total_capacity(&self) -> usize {
        self.pages.iter().map(|w| w.page.capacity()).sum()
    }

    // Get the total used space across all pages
    pub fn total_used_space(&self) -> usize {
        self.pages.iter().map(|w| w.page.size()).sum()
    }

    // Get a reference to a specific page
    pub fn get_page(&self, page_id: u64) -> Option<&Page> {
        self.pages
            .iter()
            .find(|w| w.page.id() == page_id)
            .map(|w| &w.page)
    }

    // Get an iterator over all pages
    pub fn iter_pages(&self) -> impl Iterator<Item = &Page> {
        self.pages.iter().map(|w| &w.page)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_allocation() {
        let mut manager = PageManager::new(4096);

        // Allocate a small entry
        let key1 = b"key1";
        let value1 = b"value1";
        let page_id1 = manager.allocate_entry(key1, value1).unwrap();

        // Allocate another entry - should use the same page
        let key2 = b"key2";
        let value2 = b"value2";
        let page_id2 = manager.allocate_entry(key2, value2).unwrap();

        assert_eq!(page_id1, page_id2);
        assert_eq!(manager.page_count(), 1);
    }

    #[test]
    fn test_page_removal() {
        let mut manager = PageManager::new(4096);

        // Add an entry
        let key = b"test_key";
        let value = b"test_value";
        let page_id = manager.allocate_entry(key, value).unwrap();

        // Remove the entry
        assert!(manager.remove_entry(page_id, key));

        // Try to remove non-existent entry
        assert!(!manager.remove_entry(page_id, b"nonexistent"));
    }
}
