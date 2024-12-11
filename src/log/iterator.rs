use crate::file::{BlockId, FileManager, Page};
use std::io;
use std::sync::Arc;

pub struct LogIterator {
    fm: Arc<FileManager>,
    block: BlockId,
    page: Page,
    current_pos: usize,
    boundary: usize,
}

/// A class that provides the ability to move through the
/// records of the log file in reverse order
impl LogIterator {
    pub fn new(fm: Arc<FileManager>, block: BlockId) -> Result<Self, io::Error> {
        let page = Page::new(fm.block_size());

        let mut iterator = Self {
            fm,
            block,
            page,
            current_pos: 0,
            boundary: 0,
        };

        iterator.move_to_block()?;
        Ok(iterator)
    }

    /// Moves the specified log block
    /// and positions it at the first record in that block
    /// (i.e., the most recent one)
    fn move_to_block(&mut self) -> Result<(), io::Error> {
        self.fm.read(&self.block, &mut self.page)?;
        self.boundary = self.page.get_int(0) as usize;
        self.current_pos = self.boundary;
        Ok(())
    }
}

impl Iterator for LogIterator {
    type Item = Result<Vec<u8>, std::io::Error>;

    /// Move to the next log record in the block
    /// If there are no more log records in the block,
    /// then move to the previous block
    /// and return the log record from there.
    fn next(&mut self) -> Option<Self::Item> {
        if self.current_pos >= self.fm.block_size() {
            if self.block.number() == 0 {
                return None;
            }
            self.block = BlockId::new(self.block.filename(), self.block.number() - 1);
            if let Err(e) = self.move_to_block() {
                return Some(Err(e));
            }
        }

        let bytes = self.page.get_bytes(self.current_pos);
        self.current_pos += std::mem::size_of::<i32>() + bytes.len();
        Some(Ok(bytes))
    }
}
