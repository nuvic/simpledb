use crate::{
    buffer::BufferPage,
    file::{BlockId, FileManager},
    log::LogManager,
};
use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

const MAX_TIME: u64 = 10_000;

#[derive(Debug)]
pub struct BufferError(pub String);

pub struct BufferManager {
    buffer_pool: Vec<Arc<Mutex<BufferPage>>>,
    num_available: Mutex<usize>,
}

// Manages the pinning and unpinning of buffers to blocks.
impl BufferManager {
    // Creates a buffer manager having the specified number
    // of buffer slots.
    pub fn new(fm: Arc<FileManager>, lm: Arc<Mutex<LogManager>>, num_buffs: usize) -> Self {
        let buffer_pool = (0..num_buffs)
            .map(|_| Arc::new(Mutex::new(BufferPage::new(fm.clone(), lm.clone()))))
            .collect();

        BufferManager {
            buffer_pool,
            num_available: Mutex::new(num_buffs),
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

    // Unpins the specified data buffer. If its pin count
    // goes to zero, then notify any waiting threads.
    pub fn unpin(&mut self, buffer: Arc<Mutex<BufferPage>>) {
        let buffer = buffer.lock().unwrap();

        if !buffer.is_pinned() {
            let mut num_available = self.num_available.lock().unwrap();
            *num_available += 1;
            // TODO:
            // self.notify_all();
        }
    }

    // Pins a buffer to the specified block, potentially
    // waiting until a buffer becomes available.
    // If no buffer becomes available within a fixed
    // time period, then a {@link BufferAbortException} is thrown.
    pub fn pin(&self, block: BlockId) -> Result<Arc<Mutex<BufferPage>>, BufferError> {
        let deadline = Instant::now() + Duration::from_secs(MAX_TIME);

        while Instant::now() < deadline {
            if let Ok(Some(buffer)) = self.try_to_pin(block.clone()) {
                return Ok(buffer);
            }
            std::thread::sleep(Duration::from_millis(100));
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
        self.buffer_pool
            .iter()
            .find(|buff| {
                let buffer = buff.lock().unwrap();
                buffer.block().map_or(false, |b| b == block)
            })
            .cloned()
    }

    // Naive implementation
    fn choose_unpinned_buffer(&self) -> Option<Arc<Mutex<BufferPage>>> {
        self.buffer_pool
            .iter()
            .find(|buff| {
                let buffer = buff.lock().unwrap();
                !buffer.is_pinned()
            })
            .cloned()
    }
}
