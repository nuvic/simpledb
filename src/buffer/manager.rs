use crate::{
    buffer::BufferPage,
    file::{BlockId, FileManager},
    log::LogManager,
};
use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

#[derive(Debug)]
pub struct BufferError(pub String);

pub struct BufferManager {
    buffer_pool: Vec<Arc<Mutex<BufferPage>>>,
    num_available: Mutex<usize>,
    max_time: u64,
}

// Manages the pinning and unpinning of buffers to blocks.
impl BufferManager {
    const DEFAULT_MAX_TIME: u64 = 10_000;

    // Creates a buffer manager having the specified number
    // of buffer slots.
    pub fn new(fm: Arc<FileManager>, lm: Arc<Mutex<LogManager>>, num_buffs: usize) -> Self {
        Self::new_with_timeout(fm, lm, num_buffs, Self::DEFAULT_MAX_TIME)
    }

    pub fn new_with_timeout(
        fm: Arc<FileManager>,
        lm: Arc<Mutex<LogManager>>,
        num_buffs: usize,
        max_time: u64,
    ) -> Self {
        let buffer_pool = (0..num_buffs)
            .map(|_| {
                Arc::new(Mutex::new(BufferPage::new(
                    Arc::clone(&fm),
                    Arc::clone(&lm),
                )))
            })
            .collect();

        BufferManager {
            buffer_pool,
            num_available: Mutex::new(num_buffs),
            max_time,
        }
    }

    // Returns the number of available (i.e. unpinned) buffers.
    pub fn available(&self) -> usize {
        *self.num_available.lock().unwrap()
    }

    // Flushes the dirty buffers modified by the specified transaction.
    pub fn flush_all(&self, txnum: i32) -> std::io::Result<()> {
        for buff in &self.buffer_pool {
            let mut buff = buff.lock().unwrap();
            if buff.modifying_txn() == txnum {
                buff.flush()?;
            }
        }
        Ok(())
    }

    // Unpins the specified data buffer
    pub fn unpin(&mut self, buffer: Arc<Mutex<BufferPage>>) {
        let mut buffer = buffer.lock().unwrap();
        buffer.unpin();

        if !buffer.is_pinned() {
            let mut num_available = self.num_available.lock().unwrap();
            *num_available += 1;
        }
    }

    // Pins a buffer to the specified block, potentially
    // waiting until a buffer becomes available.
    // If no buffer becomes available within a fixed
    // time period, then a BufferError is thrown.
    pub fn pin(&self, block: BlockId) -> Result<Arc<Mutex<BufferPage>>, BufferError> {
        let deadline = Instant::now() + Duration::from_millis(self.max_time);

        while Instant::now() < deadline {
            if let Ok(Some(buffer)) = self.try_to_pin(block.clone()) {
                return Ok(buffer);
            }
            std::thread::sleep(Duration::from_millis(10));
        }

        Err(BufferError("Could not pin buffer: timeout".into()))
    }

    // Tries to pin a buffer to the specified block.
    // If there is already a buffer assigned to that block
    // then that buffer is used;
    // otherwise, an unpinned buffer from the pool is chosen.
    // Returns a null value if there are no available buffers.
    fn try_to_pin(&self, block: BlockId) -> Result<Option<Arc<Mutex<BufferPage>>>, std::io::Error> {
        if let Some(buff) = self.find_existing_buffer(&block) {
            let mut buffer = buff.lock().unwrap();
            if !buffer.is_pinned() {
                let mut num_available = self.num_available.lock().unwrap();
                *num_available -= 1;
            }
            buffer.pin();
            return Ok(Some(buff.clone()));
        }

        if let Some(buff) = self.choose_unpinned_buffer() {
            let mut buffer = buff.lock().unwrap();
            buffer.assign_to_block(block)?;
            let mut num_available = self.num_available.lock().unwrap();
            *num_available -= 1;
            buffer.pin();
            Ok(Some(buff.clone()))
        } else {
            Ok(None)
        }
    }

    fn find_existing_buffer(&self, block: &BlockId) -> Option<Arc<Mutex<BufferPage>>> {
        self.buffer_pool.iter().find_map(|buff| {
            let buffer = buff.lock().unwrap();
            if buffer.block().map_or(false, |b| b == block) {
                Some(Arc::clone(buff))
            } else {
                None
            }
        })
    }

    // Naive implementation
    fn choose_unpinned_buffer(&self) -> Option<Arc<Mutex<BufferPage>>> {
        self.buffer_pool.iter().find_map(|buff| {
            let buffer = buff.lock().unwrap();
            if !buffer.is_pinned() {
                Some(Arc::clone(buff))
            } else {
                None
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn setup() -> (TempDir, Arc<FileManager>, Arc<Mutex<LogManager>>) {
        let temp_dir = TempDir::new().unwrap();
        let fm = Arc::new(FileManager::new(temp_dir.path(), 400).unwrap());
        let lm = Arc::new(Mutex::new(
            LogManager::new(Arc::clone(&fm), "test.log".to_string()).unwrap(),
        ));
        (temp_dir, fm, lm)
    }

    #[test]
    fn test_buffer_pinning() {
        let (_temp_dir, fm, lm) = setup();
        let mut bm = BufferManager::new_with_timeout(Arc::clone(&fm), Arc::clone(&lm), 3, 100);

        assert_eq!(bm.available(), 3, "All buffers should be available");

        // Create 3 blocks to pin
        let block1 = BlockId::new("test_file1".to_string(), 1);
        let block2 = BlockId::new("test_file1".to_string(), 2);
        let block3 = BlockId::new("test_file1".to_string(), 3);
        let block4 = BlockId::new("test_file1".to_string(), 4);

        // Pin all 3 buffers
        let buff1 = bm.pin(block1).unwrap();
        assert_eq!(bm.available(), 2);
        let _buff2 = bm.pin(block2).unwrap();
        assert_eq!(bm.available(), 1);
        let _buff3 = bm.pin(block3).unwrap();
        assert_eq!(bm.available(), 0);

        // Unpin one buffer
        bm.unpin(buff1);
        assert_eq!(bm.available(), 1);

        // Should be able to pin a new block
        let _buff4 = bm.pin(block4).unwrap();
        assert_eq!(bm.available(), 0);
    }

    #[test]
    fn test_buffer_pin_timeout() {
        let (_temp_dir, fm, lm) = setup();
        let bm = BufferManager::new_with_timeout(Arc::clone(&fm), Arc::clone(&lm), 3, 100);

        assert_eq!(bm.available(), 3, "All buffers should be available");

        // Create 3 blocks to pin
        let block1 = BlockId::new("test_file1".to_string(), 1);
        let block2 = BlockId::new("test_file1".to_string(), 2);
        let block3 = BlockId::new("test_file1".to_string(), 3);
        let block4 = BlockId::new("test_file1".to_string(), 4);

        // Pin all 3 buffers
        let _buff1 = bm.pin(block1).unwrap();
        assert_eq!(bm.available(), 2);
        let _buff2 = bm.pin(block2).unwrap();
        assert_eq!(bm.available(), 1);
        let _buff3 = bm.pin(block3).unwrap();
        assert_eq!(bm.available(), 0);

        // Try to pin when n buffers available
        match bm.pin(block4.clone()) {
            Err(BufferError(msg)) => {
                assert!(msg.contains("timeout"));
            }
            Ok(_) => panic!("Expected buffer pin to fail with timeout"),
        }
    }
}
