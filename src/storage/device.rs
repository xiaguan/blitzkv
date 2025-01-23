use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

use super::page::Page;

#[derive(Debug)]
pub struct SsdDevice {
    file: File,
    page_size: u32,
    metrics: SsdMetrics,
}

#[derive(Debug, Default)]
pub struct SsdMetrics {
    reads: u64,
    writes: u64,
    read_bytes: u64,
    write_bytes: u64,
    read_amplification: f64,
    write_amplification: f64,
}

impl fmt::Display for SsdMetrics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "SsdMetrics:
  Reads: {}
  Writes: {}
  Read Bytes: {}
  Write Bytes: {}
  Read Amplification: {:.2}
  Write Amplification: {:.2}",
            self.reads,
            self.writes,
            self.read_bytes,
            self.write_bytes,
            self.read_amplification,
            self.write_amplification
        )
    }
}

impl SsdMetrics {
    pub fn reads(&self) -> u64 {
        self.reads
    }

    pub fn writes(&self) -> u64 {
        self.writes
    }

    pub fn read_bytes(&self) -> u64 {
        self.read_bytes
    }

    pub fn write_bytes(&self) -> u64 {
        self.write_bytes
    }

    pub fn read_amplification(&self) -> f64 {
        self.read_amplification
    }

    pub fn write_amplification(&self) -> f64 {
        self.write_amplification
    }
}

#[derive(Debug)]
pub enum SsdError {
    Io(io::Error),
    InvalidPageSize,
    InvalidPageId,
}

impl From<io::Error> for SsdError {
    fn from(error: io::Error) -> Self {
        SsdError::Io(error)
    }
}

impl SsdDevice {
    /// Creates a new SSD device with the specified page size
    pub fn new<P: AsRef<Path>>(path: P, page_size: u32) -> Result<Self, SsdError> {
        if page_size == 0 {
            return Err(SsdError::InvalidPageSize);
        }

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        Ok(SsdDevice {
            file,
            page_size,
            metrics: SsdMetrics::default(),
        })
    }

    /// Reads a page from the device
    pub fn read_page(&mut self, page_id: u64) -> Result<Page, SsdError> {
        let offset = self.calculate_offset(page_id);
        self.file.seek(SeekFrom::Start(offset))?;

        let mut buffer = vec![0u8; self.page_size as usize];
        let bytes_read = self.file.read(&mut buffer)?;

        // Update metrics
        self.metrics.reads += 1;
        self.metrics.read_bytes += bytes_read as u64;
        if bytes_read > 0 {
            self.metrics.read_amplification = self.page_size as f64 / bytes_read as f64;
        }

        if bytes_read == 0 {
            // Create a new empty page if we're reading beyond the file
            Ok(Page::new(page_id, self.page_size))
        } else {
            Ok(Page::read_from_buffer(&buffer))
        }
    }

    /// Writes a page to the device
    pub fn write_page(&mut self, page: &mut Page) -> Result<(), SsdError> {
        if page.capacity() as u32 != self.page_size {
            return Err(SsdError::InvalidPageSize);
        }

        let offset = self.calculate_offset(page.id());
        self.file.seek(SeekFrom::Start(offset))?;

        let buffer = page.to_bytes();
        let bytes_written = self.file.write(&buffer)?;

        // Update metrics
        self.metrics.writes += 1;
        self.metrics.write_bytes += bytes_written as u64;
        if bytes_written > 0 {
            self.metrics.write_amplification = self.page_size as f64 / bytes_written as f64;
        }

        Ok(())
    }

    /// Ensures all changes are written to disk
    pub fn sync(&mut self) -> Result<(), SsdError> {
        self.file.sync_all()?;
        Ok(())
    }

    /// Returns the current metrics
    pub fn metrics(&self) -> &SsdMetrics {
        &self.metrics
    }

    /// Returns the page size of the device
    pub fn page_size(&self) -> u32 {
        self.page_size
    }

    // Calculate the offset for a given page ID
    fn calculate_offset(&self, page_id: u64) -> u64 {
        page_id * self.page_size as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn test_ssd_device_operations() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.ssd");

        // Create device
        let mut device = SsdDevice::new(&file_path, 4096).unwrap();

        // Create and write a page
        let mut page = Page::new(0, 4096);
        page.push_entry(b"key1", b"value1").unwrap();
        device.write_page(&mut page).unwrap();

        // Read the page back
        let _read_page = device.read_page(0).unwrap();

        // Verify metrics
        let metrics = device.metrics();
        assert_eq!(metrics.reads(), 1);
        assert_eq!(metrics.writes(), 1);
        assert!(metrics.read_bytes() > 0);
        assert!(metrics.write_bytes() > 0);

        println!("Metrcis is {metrics}");

        // Cleanup
        fs::remove_file(file_path).unwrap();
    }

    #[test]
    fn test_invalid_page_size() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("invalid.ssd");

        let result = SsdDevice::new(&file_path, 0);
        assert!(matches!(result, Err(SsdError::InvalidPageSize)));
    }
}
