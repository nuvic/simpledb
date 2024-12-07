use crate::file::FileManager;
use std::path::Path;

pub struct SimpleDB {
    file_manager: FileManager,
}

impl SimpleDB {
    pub const BLOCK_SIZE: usize = 400;
    pub const BUFFER_SIZE: u32 = 8;
    pub const LOG_FILE: &'static str = "simpledb.log";

    pub fn new(
        dirname: impl AsRef<Path>,
        block_size: usize,
        buffer_size: u32,
    ) -> std::io::Result<Self> {
        let file_manager = FileManager::new(dirname, block_size)?;

        Ok(SimpleDB { file_manager })
    }

    pub fn file_manager(&self) -> &FileManager {
        &self.file_manager
    }
}
