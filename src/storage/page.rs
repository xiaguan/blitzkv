// A Storage unit represents a single storage location capable of holding one item.
// - For SSDs, a storage unit corresponds to a page.
// - For object storage, it represents a single object.
// - Each storage unit has a fixed size and adheres to a consistent layout:
//   - **Layout**: [Header] + [Data]
//     - **Header**: Contains fixed-length metadata for the storage unit.
//     - **Data**: Contains a collection of entries, where each entry consists of its own metadata along with a key-value pair.
use std::convert::TryInto;
use std::slice::Iter;

const MAGIC_HEADER: &str = "blitzkv";

#[derive(Debug)]
pub struct Page {
    header: PageHeader,
    data: Vec<Entry>,
    current_size: usize,
}

// impl display for StorageUnit
impl std::fmt::Display for Page {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "storage_unit: id={} size={}, entry_count={}, current_size={}",
            self.header.id,
            self.header.size,
            self.data.len(),
            self.current_size
        )
    }
}

#[derive(Debug)]
struct PageHeader {
    magic: String, // Magic header to identify storage format
    id: u64,       // Unique identifier for the storage unit
    size: u32,     // Total size of the storage unit in bytes
    crc32: u32,    // CRC32 checksum of the data section
}

#[derive(Debug)]
pub struct Entry {
    metadata: EntryMetadata,
    key: Vec<u8>,   // Key stored as bytes for flexibility
    value: Vec<u8>, // Value stored as bytes for flexibility
}

#[derive(Debug)]
struct EntryMetadata {
    key_size: u32,
    value_size: u32,
}

// Constants for fixed sizes
const MAGIC_SIZE: usize = 7; // Length of "blitzkv"
const ID_SIZE: usize = std::mem::size_of::<u64>();
const SIZE_FIELD_SIZE: usize = std::mem::size_of::<u32>();
const CRC32_SIZE: usize = std::mem::size_of::<u32>();
const HEADER_SIZE: usize = MAGIC_SIZE + ID_SIZE + SIZE_FIELD_SIZE + CRC32_SIZE;

const ENTRY_METADATA_SIZE: usize = SIZE_FIELD_SIZE * 2; // key_size + value_size + deleted flag

impl PageHeader {
    // Serialize header into a mutable buffer
    fn write_to_buffer(&self, buf: &mut [u8]) -> usize {
        assert!(buf.len() >= HEADER_SIZE);
        buf[0..MAGIC_SIZE].copy_from_slice(self.magic.as_bytes());
        buf[MAGIC_SIZE..MAGIC_SIZE + ID_SIZE].copy_from_slice(&self.id.to_le_bytes());
        let size_offset = MAGIC_SIZE + ID_SIZE;
        buf[size_offset..size_offset + SIZE_FIELD_SIZE].copy_from_slice(&self.size.to_le_bytes());
        let crc32_offset = size_offset + SIZE_FIELD_SIZE;
        buf[crc32_offset..crc32_offset + CRC32_SIZE].copy_from_slice(&self.crc32.to_le_bytes());
        HEADER_SIZE
    }

    // Deserialize header from a buffer
    fn read_from_buffer(buf: &[u8]) -> (Self, usize) {
        assert!(buf.len() >= HEADER_SIZE);
        let magic = String::from_utf8_lossy(&buf[0..MAGIC_SIZE]).to_string();
        let id_offset = MAGIC_SIZE;
        let id = u64::from_le_bytes(buf[id_offset..id_offset + ID_SIZE].try_into().unwrap());
        let size_offset = id_offset + ID_SIZE;
        let size = u32::from_le_bytes(
            buf[size_offset..size_offset + SIZE_FIELD_SIZE]
                .try_into()
                .unwrap(),
        );
        let crc32_offset = size_offset + SIZE_FIELD_SIZE;
        let crc32 = u32::from_le_bytes(
            buf[crc32_offset..crc32_offset + CRC32_SIZE]
                .try_into()
                .unwrap(),
        );

        (
            PageHeader {
                magic,
                id,
                size,
                crc32,
            },
            HEADER_SIZE,
        )
    }
}

impl EntryMetadata {
    // Serialize metadata into a mutable buffer
    fn write_to_buffer(&self, buf: &mut [u8]) -> usize {
        assert!(buf.len() >= ENTRY_METADATA_SIZE);
        buf[0..SIZE_FIELD_SIZE].copy_from_slice(&self.key_size.to_le_bytes());
        buf[SIZE_FIELD_SIZE..SIZE_FIELD_SIZE * 2].copy_from_slice(&self.value_size.to_le_bytes());
        ENTRY_METADATA_SIZE
    }

    // Deserialize metadata from a buffer
    fn read_from_buffer(buf: &[u8]) -> (Self, usize) {
        assert!(buf.len() >= ENTRY_METADATA_SIZE);
        let key_size = u32::from_le_bytes(buf[0..SIZE_FIELD_SIZE].try_into().unwrap());
        let value_size = u32::from_le_bytes(
            buf[SIZE_FIELD_SIZE..SIZE_FIELD_SIZE * 2]
                .try_into()
                .unwrap(),
        );

        (
            EntryMetadata {
                key_size,
                value_size,
            },
            ENTRY_METADATA_SIZE,
        )
    }
}

impl Entry {
    // Serialize entry into a mutable buffer
    fn write_to_buffer(&self, buf: &mut [u8]) -> usize {
        let total_size = self.total_size();
        assert!(buf.len() >= total_size);
        let mut offset = 0;

        // Write metadata
        offset += self.metadata.write_to_buffer(&mut buf[offset..]);

        // Write key
        let key_size = self.key.len();
        buf[offset..offset + key_size].copy_from_slice(&self.key);
        offset += key_size;

        // Write value
        let value_size = self.value.len();
        buf[offset..offset + value_size].copy_from_slice(&self.value);
        offset += value_size;

        offset
    }

    // Deserialize entry from a buffer
    fn read_from_buffer(buf: &[u8]) -> (Self, usize) {
        let mut offset = 0;

        // Read metadata
        let (metadata, meta_size) = EntryMetadata::read_from_buffer(&buf[offset..]);
        offset += meta_size;

        let key_size = metadata.key_size as usize;
        let value_size = metadata.value_size as usize;

        // Read key
        let key = buf[offset..offset + key_size].to_vec();
        offset += key_size;

        // Read value
        let value = buf[offset..offset + value_size].to_vec();
        offset += value_size;

        (
            Entry {
                metadata,
                key,
                value,
            },
            offset,
        )
    }

    // Calculate total size of the entry when serialized
    fn total_size(&self) -> usize {
        ENTRY_METADATA_SIZE + self.key.len() + self.value.len()
    }

    // Public accessors for key and value
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    pub fn value(&self) -> &[u8] {
        &self.value
    }
}

impl Page {
    // Create a new storage unit with a given ID and size
    pub fn new(id: u64, size: u32) -> Self {
        Page {
            header: PageHeader {
                magic: MAGIC_HEADER.to_string(),
                id,
                size,
                crc32: 0,
            },
            data: Vec::new(),
            current_size: HEADER_SIZE + SIZE_FIELD_SIZE, // Initial size includes header and entry count
        }
    }

    // Attempt to add an entry to the storage unit
    // Returns the offset of the entry if successful, or None if the entry exceeds the size limit
    pub fn push_entry(&mut self, key: &[u8], value: &[u8]) -> Option<u32> {
        let offset = self.current_size as u32;
        let new_size = self.current_size + ENTRY_METADATA_SIZE + key.len() + value.len();

        if new_size as u32 > self.header.size {
            return None; // Exceeds the size limit
        }

        self.data.push(Entry {
            metadata: EntryMetadata {
                key_size: key.len() as u32,
                value_size: value.len() as u32,
            },
            key: key.to_vec(),
            value: value.to_vec(),
        });
        self.current_size = new_size;
        Some(offset)
    }

    // Serialize entire storage unit into a buffer
    pub fn write_to_buffer(&mut self, buf: &mut [u8]) -> usize {
        let mut offset = 0;

        // Write header with placeholder CRC32
        self.header.crc32 = 0;
        offset += self.header.write_to_buffer(&mut buf[offset..]);

        // Write number of entries
        let entry_count = self.data.len() as u32;
        buf[offset..offset + SIZE_FIELD_SIZE].copy_from_slice(&entry_count.to_le_bytes());
        offset += SIZE_FIELD_SIZE;

        // Write entries
        for entry in &self.data {
            let entry_size = entry.total_size();
            offset += entry.write_to_buffer(&mut buf[offset..offset + entry_size]);
        }

        // Compute CRC32 of the data section
        let crc32_start = HEADER_SIZE; // After header
        let crc32_end = offset;
        let crc32 = crc32fast::hash(&buf[crc32_start..crc32_end]);
        self.header.crc32 = crc32;

        // Write the CRC32 into the header
        let crc32_offset = MAGIC_SIZE + ID_SIZE + SIZE_FIELD_SIZE;
        buf[crc32_offset..crc32_offset + CRC32_SIZE]
            .copy_from_slice(&self.header.crc32.to_le_bytes());

        offset
    }

    // Deserialize entire storage unit from a buffer
    pub fn read_from_buffer(buf: &[u8]) -> Self {
        let mut offset = 0;

        // Read header
        let (header, header_size) = PageHeader::read_from_buffer(&buf[offset..]);
        offset += header_size;

        // Read number of entries
        let entry_count =
            u32::from_le_bytes(buf[offset..offset + SIZE_FIELD_SIZE].try_into().unwrap()) as usize;
        offset += SIZE_FIELD_SIZE;

        let mut data = Vec::with_capacity(entry_count);

        // Read entries
        for _ in 0..entry_count {
            let (entry, entry_size) = Entry::read_from_buffer(&buf[offset..]);
            offset += entry_size;
            data.push(entry);
        }

        // Verify CRC32 checksum
        let crc32_start = HEADER_SIZE; // After header
        let crc32_end = offset;
        let computed_crc32 = crc32fast::hash(&buf[crc32_start..crc32_end]);
        if computed_crc32 != header.crc32 {
            panic!("CRC32 checksum mismatch");
        }

        Page {
            header,
            data,
            current_size: offset,
        }
    }

    // Serialize entire storage unit and return the buffer
    pub fn to_bytes(&mut self) -> Vec<u8> {
        let total_size = self.current_size;
        let mut buf = vec![0u8; total_size];
        let bytes_written = self.write_to_buffer(&mut buf);
        buf.truncate(bytes_written);
        buf
    }

    // Returns an iterator over the entries
    pub fn iter(&self) -> Iter<Entry> {
        self.data.iter()
    }

    // Calculate the total size of the storage unit
    pub fn size(&self) -> usize {
        self.current_size
    }

    // Get the capacity of the storage unit
    pub fn capacity(&self) -> usize {
        self.header.size as usize
    }

    // Get write amplification factor
    pub fn space_amplification(&self) -> f64 {
        // all key size and value size
        let total_data_size: usize = self
            .data
            .iter()
            .map(|entry| (entry.metadata.key_size as usize) + (entry.metadata.value_size as usize))
            .sum();
        self.header.size as f64 / total_data_size as f64
    }

    // Get the storage unit ID
    pub fn id(&self) -> u64 {
        self.header.id
    }

    pub fn remove_entry(&mut self, key: &[u8]) -> bool {
        let mut index = 0;
        let mut found = false;
        for entry in &self.data {
            if entry.key == key {
                found = true;
                break;
            }
            index += 1;
        }
        if found {
            let removed_entry = self.data.remove(index);
            self.current_size -= removed_entry.total_size();
            true
        } else {
            false
        }
    }
}
