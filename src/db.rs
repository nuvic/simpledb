use crate::{file::FileManager, log::LogManager};
use std::path::Path;
use std::sync::Arc;

pub struct SimpleDB {
    fm: Arc<FileManager>,
    lm: LogManager,
}

impl SimpleDB {
    pub const BLOCK_SIZE: usize = 400;
    pub const BUFFER_SIZE: u32 = 8;
    pub const LOG_FILE: &'static str = "simpledb.log";

    pub fn new(
        dirname: impl AsRef<Path>,
        block_size: usize,
        _buffer_size: u32,
    ) -> std::io::Result<SimpleDB> {
        let fm = Arc::new(FileManager::new(dirname, block_size)?);
        let lm = LogManager::new(Arc::clone(&fm), Self::LOG_FILE.to_string())?;

        Ok(SimpleDB { fm, lm })
    }

    pub fn file_manager(&self) -> Arc<FileManager> {
        Arc::clone(&self.fm)
    }

    pub fn log_manager(&mut self) -> &mut LogManager {
        &mut self.lm
    }
}
