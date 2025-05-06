use std::collections::HashMap;
use std::sync::{Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use crate::file::BlockId;

#[derive(Debug)]
pub struct LockAbortError;

impl std::fmt::Display for LockAbortError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Lock acquisition aborted due to timeout")
    }
}

impl std::error::Error for LockAbortError {}

pub struct LockTable {
    locks: Mutex<HashMap<BlockId, i32>>,
    cond_var: Condvar,
    max_time: Duration,
}

impl Default for LockTable {
    fn default() -> Self {
        Self::new()
    }
}

// The lock table, which provides methods to lock and unlock blocks.
// If a transaction requests a lock that causes a conflict with an
// existing lock, then that transaction is placed on a wait list.
// There is only one wait list for all blocks.
// When the last lock on a block is unlocked, then all transactions
// are removed from the wait list and rescheduled.
// If one of those transactions discovers that the lock it is waiting for
// is still locked, it will place itself back on the wait list.
impl LockTable {
    pub fn new() -> Self {
        LockTable {
            locks: Mutex::new(HashMap::new()),
            cond_var: Condvar::new(),
            max_time: Duration::from_secs(10),
        }
    }

    // Grant an SLock on the specified block
    // If an XLock exists when the method is called,
    // then the calling thread will be placed on a wait list
    // until the lock is released.
    // If the thread remains on the wait list for a certain
    // amount of time (currently 10 seconds),
    // then an exception is thrown.
    pub fn slock(&self, blk: BlockId) -> Result<(), LockAbortError> {
        let start_time = Instant::now();
        let mut locks = self.locks.lock().unwrap();

        while self.has_xlock(&locks, &blk) && !self.waiting_too_long(start_time) {
            let result = self.cond_var.wait_timeout(locks, self.max_time).unwrap();
            locks = result.0;
        }

        if self.has_xlock(&locks, &blk) {
            return Err(LockAbortError);
        }

        let val = self.get_lock_value(&locks, &blk);
        locks.insert(blk.clone(), val + 1);

        Ok(())
    }

    // Grant an XLock on the specified block.
    // If a lock of any type exists when the method is called,
    // then the calling thread will be placed on a wait list
    // until the locks are released.
    // If the thread remains on the wait list for a certain
    // amount of time (currently 10 seconds)
    // then an exception is thrown.
    pub fn x_lock(&self, blk: &BlockId) -> Result<(), LockAbortError> {
        let start_time = Instant::now();
        let mut locks = self.locks.lock().unwrap();

        while self.has_other_s_locks(&locks, blk) && !self.waiting_too_long(start_time) {
            // Wait until notified or timeout occurs
            let result = self.cond_var.wait_timeout(locks, self.max_time).unwrap();
            locks = result.0;
        }

        // Check if we still have other S-locks after waiting
        if self.has_other_s_locks(&locks, blk) {
            return Err(LockAbortError);
        }

        locks.insert(blk.clone(), -1);

        Ok(())
    }

    pub fn unlock(&self, blk: BlockId) {
        let mut locks = self.locks.lock().unwrap();
        let val = *locks.get(&blk).unwrap_or(&0);
        if val > 1 {
            locks.insert(blk, val - 1);
        } else {
            locks.remove(&blk);
            self.cond_var.notify_all();
        }
    }

    fn has_xlock(&self, locks: &MutexGuard<HashMap<BlockId, i32>>, blk: &BlockId) -> bool {
        self.get_lock_value(locks, blk) < 0
    }

    fn has_other_s_locks(&self, locks: &MutexGuard<HashMap<BlockId, i32>>, blk: &BlockId) -> bool {
        self.get_lock_value(locks, blk) > 1
    }

    fn waiting_too_long(&self, start_time: Instant) -> bool {
        start_time.elapsed() > self.max_time
    }

    fn get_lock_value(&self, locks: &MutexGuard<HashMap<BlockId, i32>>, blk: &BlockId) -> i32 {
        *locks.get(blk).unwrap_or(&0)
    }
}
