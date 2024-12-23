use crate::{buffer::BufferManager, file::FileManager, log::LogManager};
use std::path::Path;
use std::sync::{Arc, Mutex};

pub struct SimpleDB {
    fm: Arc<FileManager>,
    lm: Arc<Mutex<LogManager>>,
    bm: BufferManager,
}

impl SimpleDB {
    pub const BLOCK_SIZE: usize = 400;
    pub const BUFFER_SIZE: u32 = 8;
    pub const LOG_FILE: &'static str = "simpledb.log";

    pub fn new(
        dirname: impl AsRef<Path>,
        block_size: usize,
        buffer_size: u32,
    ) -> std::io::Result<SimpleDB> {
        let fm = Arc::new(FileManager::new(dirname, block_size)?);
        let lm = Arc::new(Mutex::new(LogManager::new(
            Arc::clone(&fm),
            Self::LOG_FILE.to_string(),
        )?));
        let bm = BufferManager::new(Arc::clone(&fm), Arc::clone(&lm), buffer_size as usize);

        Ok(SimpleDB { fm, lm, bm })
    }

    pub fn file_manager(&self) -> Arc<FileManager> {
        Arc::clone(&self.fm)
    }

    pub fn log_manager(&self) -> &Arc<Mutex<LogManager>> {
        &self.lm
    }

    pub fn buffer_manager(&self) -> &BufferManager {
        &self.bm
    }
}
