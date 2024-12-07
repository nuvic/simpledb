use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::Mutex,
};

use crate::file::{BlockId, Page};

pub struct FileManager {
    db_directory: PathBuf,
    block_size: usize,
    is_new: bool,
    open_files: Mutex<HashMap<String, File>>,
}

impl FileManager {
    pub fn new(db_directory: impl AsRef<Path>, block_size: usize) -> io::Result<Self> {
        let db_directory = db_directory.as_ref().to_path_buf();
        let is_new = !db_directory.exists();

        if is_new {
            fs::create_dir_all(&db_directory)?;
        }

        // Clean up temp files
        if let Ok(entries) = fs::read_dir(&db_directory) {
            for entry in entries.flatten() {
                let filename = entry.file_name();
                if filename.to_string_lossy().starts_with("temp") {
                    let _ = fs::remove_file(entry.path());
                }
            }
        }

        Ok(Self {
            db_directory,
            block_size,
            is_new,
            open_files: Mutex::new(HashMap::new()),
        })
    }

    pub fn read(&self, block: &BlockId, page: &mut Page) -> io::Result<()> {
        let mut file = self.get_file(block.filename())?;
        let offset = block.number() * self.block_size as u64;

        // Seek to correct block position
        file.seek(SeekFrom::Start(offset))?;

        // Get mutable reference to page's buffer and read directly into it
        let buf = page.contents();
        file.read_exact(buf)?;

        Ok(())
    }

    pub fn write(&self, block: &BlockId, page: &mut Page) -> io::Result<()> {
        let mut file = self.get_file(block.filename())?;
        let offset = block.number() * self.block_size as u64;

        file.seek(SeekFrom::Start(offset))?;
        file.write_all(page.contents())?;
        file.sync_data()?;
        Ok(())
    }

    pub fn append(&self, filename: &str) -> io::Result<BlockId> {
        let new_block_num = self.length(filename)?;
        let block = BlockId::new(filename.to_string(), new_block_num);
        let empty_data = vec![0; self.block_size];

        let mut file = self.get_file(filename)?;
        file.seek(SeekFrom::End(0))?;
        file.write_all(&empty_data)?;
        file.sync_data()?;

        Ok(block)
    }

    pub fn length(&self, filename: &str) -> io::Result<u64> {
        let file = self.get_file(filename)?;
        let len = file.metadata()?.len();
        Ok(len / self.block_size as u64)
    }

    pub fn is_new(&self) -> bool {
        self.is_new
    }

    pub fn block_size(&self) -> usize {
        self.block_size
    }

    fn get_file(&self, filename: &str) -> io::Result<File> {
        let mut files = self
            .open_files
            .lock()
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "failed to acquire lock"))?;

        if let Some(file) = files.get(filename) {
            Ok(file.try_clone()?)
        } else {
            let filepath = self.db_directory.join(filename);
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(filepath)?;

            let clone = file.try_clone()?;
            files.insert(filename.to_string(), file);
            Ok(clone)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn setup() -> (TempDir, FileManager) {
        let temp_dir = TempDir::new().unwrap();
        let fm = FileManager::new(temp_dir.path(), 400).unwrap();
        (temp_dir, fm)
    }

    #[test]
    fn test_write_read_basic() {
        let (_temp_dir, fm) = setup();
        let block = BlockId::new("test.dat".to_string(), 0);

        // First write some data
        let mut write_page = Page::new(400);
        write_page.contents()[0..5].copy_from_slice(b"hello");
        fm.write(&block, &mut write_page).unwrap();

        // Read it back
        let mut read_page = Page::new(400);
        fm.read(&block, &mut read_page).unwrap();

        assert_eq!(&read_page.contents()[0..5], b"hello");
    }

    #[test]
    fn test_write_read_multiple_blocks() {
        let (_temp_dir, fm) = setup();

        // Write to multiple blocks
        for i in 0..3 {
            let block = BlockId::new("test.dat".to_string(), i);
            let mut page = Page::new(400);
            page.contents()[0] = i as u8;
            fm.write(&block, &mut page).unwrap();
        }

        // Read and verify each block
        for i in 0..3 {
            let block = BlockId::new("test.dat".to_string(), i);
            let mut page = Page::new(400);
            fm.read(&block, &mut page).unwrap();
            assert_eq!(page.contents()[0], i as u8);
        }
    }

    #[test]
    fn test_read_nonexistent_file() {
        let (_temp_dir, fm) = setup();
        let block = BlockId::new("nonexistent.dat".to_string(), 0);
        let mut page = Page::new(400);

        let result = fm.read(&block, &mut page);
        assert!(result.is_err());
    }
}
