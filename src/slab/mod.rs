use std::cmp::min;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::mem;
use std::path::{Path, PathBuf};

// 类似于 C++ 版本中的 Slice 类
#[derive(Debug, Clone)]
pub struct Slice {
    data: Vec<u8>,
}

impl Slice {
    // 从 &str 创建 Slice
    fn from_str(s: &str) -> Self {
        Slice {
            data: s.as_bytes().to_vec(),
        }
    }

    // 从 Vec<u8> 创建 Slice
    fn from_vec(v: Vec<u8>) -> Self {
        Slice { data: v }
    }

    // 深拷贝
    fn deep_copy(&self) -> Self {
        Slice {
            data: self.data.clone(),
        }
    }

    // 获取数据长度
    fn len(&self) -> usize {
        self.data.len()
    }

    // 获取数据指针
    fn as_ptr(&self) -> *const u8 {
        self.data.as_ptr()
    }

    // 转换为 String
    fn to_string(&self) -> String {
        String::from_utf8_lossy(&self.data).to_string()
    }

    // 比较
    fn compare(&self, other: &Slice) -> std::cmp::Ordering {
        let min_len = min(self.len(), other.len());
        let result = unsafe {
            libc::memcmp(
                self.data.as_ptr() as *const libc::c_void,
                other.data.as_ptr() as *const libc::c_void,
                min_len,
            )
        };

        if result == 0 {
            self.len().cmp(&other.len())
        } else if result < 0 {
            std::cmp::Ordering::Less
        } else {
            std::cmp::Ordering::Greater
        }
    }
}

impl PartialEq for Slice {
    fn eq(&self, other: &Self) -> bool {
        self.data == other.data
    }
}

impl Eq for Slice {}

impl PartialOrd for Slice {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.compare(other))
    }
}

impl Ord for Slice {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.compare(other)
    }
}

// Slab 结构体
#[derive(Debug, Clone, Copy)]
pub struct Slab {
    size: u64,
    index: u64,
    file: u64,
}

// 管理单个 slab 大小的文件
#[derive(Debug)]
struct SingleFileSlab {
    files: Vec<u64>,
    slab_size: u64,
    slab_per_file: u64,
    cur_file: u64,
    cur_slab: u64,
    free_slab: Vec<Slab>,
}

impl SingleFileSlab {
    fn new(slab_per_file: u64, slab_size: u64) -> Self {
        SingleFileSlab {
            files: Vec::new(),
            slab_size,
            slab_per_file,
            cur_file: 0,
            cur_slab: slab_per_file, // 初始化为 slab_per_file 表示当前文件为空
            free_slab: Vec::new(),
        }
    }
}

// 文件管理
#[derive(Debug)]
struct FileSet {
    directory: PathBuf,
    next_file_id: u64,
}

impl FileSet {
    fn new(directory: &str) -> Self {
        FileSet {
            directory: PathBuf::from(directory),
            next_file_id: 0,
        }
    }

    fn create(&mut self, file_name: &str) -> std::io::Result<u64> {
        let file_path = self.directory.join(file_name);
        OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(file_path)?;

        let file_id = self.next_file_id;
        self.next_file_id += 1;
        Ok(file_id)
    }

    fn open(&self, file_id: u64, file_name: &str) -> std::io::Result<File> {
        let file_path = self.directory.join(file_name);
        OpenOptions::new().read(true).write(true).open(file_path)
    }
}

#[derive(Debug)]
pub struct FileSlab {
    file_set: FileSet,
    slabs: Vec<SingleFileSlab>,
}

impl FileSlab {
    const VALID: u16 = 1 << 10;

    pub fn new(directory: &str, slab_per_file: u64, slab_sizes: &[u64]) -> Self {
        let file_set = FileSet::new(directory);
        let mut slabs = Vec::new();
        for &size in slab_sizes {
            slabs.push(SingleFileSlab::new(slab_per_file, size));
        }
        FileSlab { file_set, slabs }
    }

    fn valid(valid: u16) -> bool {
        valid == FileSlab::VALID
    }

    pub fn create(&mut self, key: &Slice, value: &Slice) -> Result<Slab, &'static str> {
        let item_size = key.len() + value.len() + 3 * mem::size_of::<u16>();

        for slab in &mut self.slabs {
            if slab.slab_size >= item_size as u64 {
                if let Some(free_slab) = slab.free_slab.pop() {
                    return Ok(free_slab);
                } else if slab.cur_slab == slab.slab_per_file {
                    let file_name = format!("slab_{}_{}", slab.slab_size, slab.files.len());
                    match self.file_set.create(&file_name) {
                        Ok(file_id) => {
                            slab.cur_file = file_id;
                            slab.cur_slab = 0;
                            slab.files.push(slab.cur_file);
                        }
                        Err(_) => return Err("Failed to create file"),
                    }
                } else {
                    slab.cur_slab += 1;
                }
                return Ok(Slab {
                    size: slab.slab_size,
                    index: slab.cur_slab,
                    file: slab.cur_file,
                });
            }
        }
        Err("Size too large")
    }

    pub fn read(&mut self, slab: Slab) -> Result<(Slice, Slice), &'static str> {
        let slab_file = self
            .slabs
            .iter()
            .find(|s| s.slab_size == slab.size)
            .ok_or("Slab size not found")?;
        let file_name = format!(
            "slab_{}_{}",
            slab.size,
            slab_file
                .files
                .iter()
                .position(|&f| f == slab.file)
                .ok_or("File not found")?
        );

        let mut file = match self.file_set.open(slab.file, &file_name) {
            Ok(f) => f,
            Err(_) => return Err("Failed to open file"),
        };

        let offset = slab.size * slab.index;
        if let Err(_) = file.seek(SeekFrom::Start(offset)) {
            return Err("Failed to seek file");
        }

        let mut header = [0u8; 6]; // valid + key_size + value_size
        if let Err(_) = file.read_exact(&mut header) {
            return Err("Failed to read header");
        }

        let valid = u16::from_le_bytes([header[0], header[1]]);
        if !FileSlab::valid(valid) {
            return Err("Invalid slab");
        }

        let key_size = u16::from_le_bytes([header[2], header[3]]) as usize;
        let value_size = u16::from_le_bytes([header[4], header[5]]) as usize;

        let mut key_data = vec![0u8; key_size];
        let mut value_data = vec![0u8; value_size];

        if let Err(_) = file.read_exact(&mut key_data) {
            return Err("Failed to read key");
        }
        if let Err(_) = file.read_exact(&mut value_data) {
            return Err("Failed to read value");
        }

        Ok((Slice::from_vec(key_data), Slice::from_vec(value_data)))
    }

    pub fn write(&mut self, slab: Slab, key: &Slice, value: &Slice) -> Result<(), &'static str> {
        let key_size = key.len();
        let value_size = value.len();
        let item_size = key_size + value_size + 3 * mem::size_of::<u16>();
        let slab_file = self
            .slabs
            .iter()
            .find(|s| s.slab_size == slab.size)
            .ok_or("Slab size not found")?;
        let file_name = format!(
            "slab_{}_{}",
            slab.size,
            slab_file
                .files
                .iter()
                .position(|&f| f == slab.file)
                .ok_or("File not found")?
        );

        let mut file = match self.file_set.open(slab.file, &file_name) {
            Ok(f) => f,
            Err(_) => return Err("Failed to open file"),
        };

        let offset = slab.size * slab.index;
        if let Err(_) = file.seek(SeekFrom::Start(offset)) {
            return Err("Failed to seek file");
        }

        let mut data = Vec::with_capacity(item_size);
        data.extend_from_slice(&FileSlab::VALID.to_le_bytes());
        data.extend_from_slice(&(key_size as u16).to_le_bytes());
        data.extend_from_slice(&(value_size as u16).to_le_bytes());
        data.extend_from_slice(&key.data);
        data.extend_from_slice(&value.data);

        if let Err(_) = file.write_all(&data) {
            return Err("Failed to write data");
        }

        Ok(())
    }

    pub fn delete(&mut self, slab: Slab) -> Result<(), &'static str> {
        for s in &mut self.slabs {
            if s.slab_size == slab.size {
                s.free_slab.push(slab);

                let file_name = format!(
                    "slab_{}_{}",
                    slab.size,
                    s.files
                        .iter()
                        .position(|&f| f == slab.file)
                        .ok_or("File not found")?
                );

                let mut file = match self.file_set.open(slab.file, &file_name) {
                    Ok(f) => f,
                    Err(_) => return Err("Failed to open file"),
                };

                let offset = slab.size * slab.index;
                if let Err(_) = file.seek(SeekFrom::Start(offset)) {
                    return Err("Failed to seek file");
                }

                // 写入无效标志
                let invalid_flag: [u8; 2] = 0u16.to_le_bytes();
                if let Err(_) = file.write_all(&invalid_flag) {
                    return Err("Failed to write invalid flag");
                }

                return Ok(());
            }
        }
        Err("Slab not found")
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_slab_basic() {
        let mut file_slab = super::FileSlab::new("./slab_data", 2, &[128, 256, 512]);

        let key1 = super::Slice::from_str("key1");
        let value1 = super::Slice::from_str("value1");
        let key2 = super::Slice::from_str("key2");
        let value2 = super::Slice::from_str("value2_longer");

        let slab1 = file_slab.create(&key1, &value1).unwrap();
        file_slab.write(slab1, &key1, &value1).unwrap();

        let slab2 = file_slab.create(&key2, &value2).unwrap();
        file_slab.write(slab2, &key2, &value2).unwrap();

        let (read_key1, read_value1) = file_slab.read(slab1).unwrap();
        assert_eq!(read_key1, key1);
        assert_eq!(read_value1, value1);

        let (read_key2, read_value2) = file_slab.read(slab2).unwrap();
        assert_eq!(read_key2, key2);
        assert_eq!(read_value2, value2);

        file_slab.delete(slab1).unwrap();

        let key3 = super::Slice::from_str("key3");
        let value3 = super::Slice::from_str("value3");
        let slab3 = file_slab.create(&key3, &value3).unwrap();
        assert_eq!(slab3.index, slab1.index);
        assert_eq!(slab3.file, slab1.file);

        file_slab.write(slab3, &key3, &value3).unwrap();
        let (read_key3, read_value3) = file_slab.read(slab3).unwrap();
        assert_eq!(read_key3, key3);
        assert_eq!(read_value3, value3);
    }
}
