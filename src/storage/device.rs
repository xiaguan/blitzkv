use hdrhistogram::Histogram;
use std::alloc::{alloc, dealloc, Layout};
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::ptr::NonNull;
use std::time::Instant;
use tracing::{debug, error, info, instrument, warn};

use super::page::Page;

const O_DIRECT: i32 = 0o0040000;

struct AlignedBuffer {
    ptr: NonNull<u8>,
    size: usize,
    layout: Layout,
}

impl AlignedBuffer {
    /// 创建指定大小的对齐内存缓冲区
    fn new(size: usize) -> io::Result<Self> {
        let layout = Layout::from_size_align(size, size)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

        // SAFETY: 布局大小不为零
        let ptr = unsafe { alloc(layout) };
        if ptr.is_null() {
            return Err(io::Error::new(io::ErrorKind::Other, "Allocation failed"));
        }

        Ok(Self {
            ptr: NonNull::new(ptr).unwrap(),
            size,
            layout,
        })
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.size) }
    }
}

impl Drop for AlignedBuffer {
    fn drop(&mut self) {
        unsafe { dealloc(self.ptr.as_ptr(), self.layout) };
    }
}

#[derive(Debug)]
pub struct SsdDevice {
    file: File,
    page_size: u32,
    metrics: SsdMetrics,
}

pub struct SsdMetrics {
    reads: u64,
    writes: u64,
    read_bytes: u64,
    write_bytes: u64,
    read_latency_hist: Histogram<u64>,
    write_latency_hist: Histogram<u64>,
}

impl Default for SsdMetrics {
    fn default() -> Self {
        Self {
            reads: 0,
            writes: 0,
            read_bytes: 0,
            write_bytes: 0,
            read_latency_hist: Histogram::<u64>::new(3).unwrap(), // 3 significant figures
            write_latency_hist: Histogram::<u64>::new(3).unwrap(),
        }
    }
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
  Read Latency (μs):
    p50: {:.2}
    p95: {:.2}
    p99: {:.2}
    max: {:.2}
  Write Latency (μs):
    p50: {:.2}
    p95: {:.2}
    p99: {:.2}
    max: {:.2}",
            self.reads,
            self.writes,
            self.read_bytes,
            self.write_bytes,
            self.read_latency_hist.value_at_percentile(50.0) as f64 / 1000.0,
            self.read_latency_hist.value_at_percentile(95.0) as f64 / 1000.0,
            self.read_latency_hist.value_at_percentile(99.0) as f64 / 1000.0,
            self.read_latency_hist.max() as f64 / 1000.0,
            self.write_latency_hist.value_at_percentile(50.0) as f64 / 1000.0,
            self.write_latency_hist.value_at_percentile(95.0) as f64 / 1000.0,
            self.write_latency_hist.value_at_percentile(99.0) as f64 / 1000.0,
            self.write_latency_hist.max() as f64 / 1000.0,
        )
    }
}

impl fmt::Debug for SsdMetrics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SsdMetrics")
            .field("reads", &self.reads)
            .field("writes", &self.writes)
            .field("read_bytes", &self.read_bytes)
            .field("write_bytes", &self.write_bytes)
            .field(
                "read_latency_hist (p50, p95, p99, max)",
                &(
                    self.read_latency_hist.value_at_percentile(50.0),
                    self.read_latency_hist.value_at_percentile(95.0),
                    self.read_latency_hist.value_at_percentile(99.0),
                    self.read_latency_hist.max(),
                ),
            )
            .field(
                "write_latency_hist (p50, p95, p99, max)",
                &(
                    self.write_latency_hist.value_at_percentile(50.0),
                    self.write_latency_hist.value_at_percentile(95.0),
                    self.write_latency_hist.value_at_percentile(99.0),
                    self.write_latency_hist.max(),
                ),
            )
            .finish()
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

    pub fn read_latency_percentile(&self, percentile: f64) -> f64 {
        self.read_latency_hist.value_at_percentile(percentile) as f64 / 1000.0
    }

    pub fn write_latency_percentile(&self, percentile: f64) -> f64 {
        self.write_latency_hist.value_at_percentile(percentile) as f64 / 1000.0
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
    #[instrument(skip(path))]
    pub fn new<P: AsRef<Path>>(path: P, page_size: u32) -> Result<Self, SsdError> {
        if page_size == 0 {
            error!("Attempted to create SsdDevice with invalid page size: 0");
            return Err(SsdError::InvalidPageSize);
        }
        info!("Creating new SsdDevice with page_size: {}", page_size);

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(O_DIRECT)
            .create(true)
            .open(path)?;

        Ok(SsdDevice {
            file,
            page_size,
            metrics: SsdMetrics::default(),
        })
    }

    /// Reads a page from the device
    #[instrument(skip(self))]
    pub fn read_page(&mut self, page_id: u64) -> Result<Page, SsdError> {
        debug!("Reading page {} from device", page_id);

        let mut buffer =
            AlignedBuffer::new(self.page_size as usize).map_err(|e| SsdError::Io(e))?;

        let offset = self.calculate_offset(page_id);
        self.file.seek(SeekFrom::Start(offset))?;
        let start = Instant::now();
        let bytes_read = self
            .file
            .read(buffer.as_mut_slice())
            .map_err(SsdError::Io)?;
        assert_eq!(bytes_read, self.page_size as usize);
        // Record latency in nanoseconds
        let elapsed_nanos = start.elapsed().as_nanos() as u64;
        self.metrics
            .read_latency_hist
            .record(elapsed_nanos)
            .unwrap();

        // Update metrics
        self.metrics.reads += 1;
        self.metrics.read_bytes += bytes_read as u64;

        if bytes_read == 0 {
            // Create a new empty page if we're reading beyond the file
            warn!(
                "Reading beyond file end for page {}, creating empty page",
                page_id
            );
            Ok(Page::new(page_id, self.page_size))
        } else {
            debug!(
                "Successfully read {} bytes for page {}",
                bytes_read, page_id
            );
            Ok(Page::read_from_buffer(&buffer.as_mut_slice()))
        }
    }

    /// Writes a page to the device
    #[instrument(skip(self, page))]
    pub fn write_page(&mut self, page: &mut Page) -> Result<(), SsdError> {
        if page.capacity() as u32 != self.page_size {
            error!(
                "Page size mismatch: expected {}, got {}",
                self.page_size,
                page.capacity()
            );
            return Err(SsdError::InvalidPageSize);
        }
        debug!("Writing page {} to device", page.id());

        let offset = self.calculate_offset(page.id());
        self.file.seek(SeekFrom::Start(offset)).unwrap();

        let size = self.page_size as usize;
        let layout = Layout::from_size_align(size, size).unwrap();
        let ptr = unsafe { alloc(layout) as *mut u8 };
        let mut buffer = unsafe { Vec::from_raw_parts(ptr, size, size) };
        page.write_to_buffer(&mut buffer);
        let start = Instant::now();
        let bytes_written = self.file.write(&buffer).unwrap();
        // Record latency in nanoseconds
        let elapsed_nanos = start.elapsed().as_nanos() as u64;
        self.metrics
            .write_latency_hist
            .record(elapsed_nanos)
            .unwrap();

        // Update metrics
        self.metrics.writes += 1;
        self.metrics.write_bytes += bytes_written as u64;
        // self.sync().unwrap();

        // Manually deallocate the memory
        unsafe {
            let ptr = buffer.as_mut_ptr();
            let capacity = buffer.capacity();
            std::mem::forget(buffer); // Prevent double-free
            dealloc(
                ptr as *mut u8,
                Layout::from_size_align(capacity, capacity).unwrap(),
            );
        }
        Ok(())
    }

    /// Ensures all changes are written to disk
    #[instrument(skip(self))]
    fn sync(&mut self) -> Result<(), SsdError> {
        debug!("Syncing device to disk");
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
