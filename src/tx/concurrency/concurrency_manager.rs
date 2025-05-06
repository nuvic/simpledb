use std::{collections::HashMap, sync::Mutex};

use super::{lock_table::LockAbortError, LockTable};
use crate::file::BlockId;

// The concurrency manager for the transaction.
// Each transaction has its own concurrency manager.
// The concurrency manager keeps track of which locks the
// transaction currently has, and interacts with the
// global lock table as needed.
pub struct ConcurrencyManager {
    lock_table: LockTable,
    locks: Mutex<HashMap<BlockId, String>>,
}

impl ConcurrencyManager {
    pub fn new() -> Self {
        ConcurrencyManager {
            lock_table: LockTable::new(),
            locks: Mutex::new(HashMap::new()),
        }
    }

    // Obtain an SLock on the block, if necessary.
    // The method will ask the lock table for an SLock
    // if the transaction currently has no locks on that block.
    pub fn slock(&self, blk: BlockId) -> Result<(), LockAbortError> {
        let mut locks = self.locks.lock().unwrap();

        if locks.get(&blk).is_none() {
            self.lock_table.slock(blk.clone())?;
            locks.insert(blk, "S".into());
        }

        Ok(())
    }

    // Obtain an XLock on the block, if necessary.
    // If the transaction does not have an XLock on that block,
    // then the method first gets an SLock on that block
    // (if necessary), and then upgrades it to an XLock.
    pub fn xlock(&self, blk: BlockId) -> Result<(), LockAbortError> {
        let mut locks = self.locks.lock().unwrap();

        if !self.has_xlock(&blk) {
            self.slock(blk.clone())?;
            self.lock_table.x_lock(&blk)?;
            locks.insert(blk, "X".into());
        }

        Ok(())
    }

    // Release all locks by asking the lock table to
    // unlock each one.
    pub fn release(&self) {
        let mut locks = self.locks.lock().unwrap();

        let keys: Vec<_> = locks.keys().cloned().collect();

        for blk in keys {
            self.lock_table.unlock(blk);
        }

        locks.clear();
    }

    fn has_xlock(&self, blk: &BlockId) -> bool {
        let locks = self.locks.lock().unwrap();
        locks
            .get(blk)
            .is_some_and(|locktype| locktype.as_str() == "X")
    }
}

impl Default for ConcurrencyManager {
    fn default() -> Self {
        Self::new()
    }
}
