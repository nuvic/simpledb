use crate::file::{BlockId, FileManager, Page};
use crate::log::LogIterator;
use std::io;
use std::sync::Arc;

const INT_SIZE: usize = std::mem::size_of::<i32>();

pub struct LogManager {
    fm: Arc<FileManager>,
    logfile: String,
    logpage: Page,
    current_blk: BlockId,
    latest_lsn: i32,
    last_saved_lsn: i32,
}

impl LogManager {
    /// Creates a new log manager instance.
    ///
    /// If the log file does not yet exist, it is created
    /// with an empty first block.
    pub fn new(fm: Arc<FileManager>, logfile: String) -> io::Result<Self> {
        let mut logpage = Page::new(fm.block_size());
        let logsize = fm.length(&logfile)?;

        let current_blk = if logsize == 0 {
            Self::append_new_block(&fm, &logfile, &mut logpage)?
        } else {
            let blk = BlockId::new(&logfile, logsize - 1);
            fm.read(&blk, &mut logpage)?;
            blk
        };

        Ok(LogManager {
            fm,
            logfile,
            logpage,
            current_blk,
            latest_lsn: 0,
            last_saved_lsn: 0,
        })
    }

    /// Ensures the log record for the specified LSN is written to disk
    /// All earlier log records will also be written to disk
    pub fn flush(&mut self, lsn: i32) -> Result<(), io::Error> {
        if lsn >= self.last_saved_lsn {
            self.flush_internal()?;
        }
        Ok(())
    }

    pub fn iter(&mut self) -> Result<LogIterator, io::Error> {
        self.flush_internal()?;
        LogIterator::new(Arc::clone(&self.fm), self.current_blk.clone())
    }

    /// Appends a log record to the log buffer.
    /// The record consists of an arbitrary array of bytes.
    /// Log records are written right to left in the buffer.
    /// The size of the record is written before the bytes.
    /// The beginning of the buffer contains the location
    /// of the last-written record (the "boundary").
    /// Storing the records backwards makes it easy to read
    /// them in reverse order.
    ///
    /// The boundary value is stored as the first 4 bytes of the page's buffer
    ///
    /// Initial empty block (after appendNewBlock):
    /// +----------------+----------------------------------+
    /// | Boundary=4096  |           Empty Space           |
    /// +----------------+----------------------------------+
    /// 0                                               4096
    ///
    ///
    /// After appending first record (size=100):
    /// +----------------+----------------------+------------+
    /// | Boundary=3996  |     Empty Space     | Record 1   |
    /// +----------------+----------------------+------------+
    /// 0                                    3996         4096
    ///                                       ↑
    ///                                       New boundary points here
    ///
    ///
    /// After appending second record (size=50):
    /// +----------------+----------------+-------+----------+
    /// | Boundary=3946  |  Empty Space  | Rec 2  | Record 1 |
    /// +----------------+----------------+-------+----------+
    /// 0                              3946     3996      4096
    ///                                 ↑
    ///                                 New boundary points here
    pub fn append(&mut self, logrec: &[u8]) -> Result<i32, io::Error> {
        let boundary = self.logpage.get_int(0);

        let recsize = logrec.len();
        let bytes_needed = (recsize + INT_SIZE) as i32;

        // check if record fits in block
        if boundary - bytes_needed < (INT_SIZE as i32) {
            // if log record doesn't fit, move to the next block
            self.flush_internal()?;
            self.current_blk = Self::append_new_block(&self.fm, &self.logfile, &mut self.logpage)?;

            let boundary = self.logpage.get_int(0);
            let recpos = boundary - bytes_needed;

            self.logpage.set_bytes(recpos as usize, logrec);
            // Update boundary to point to new record start
            self.logpage.set_int(0, recpos);
        } else {
            let recpos = boundary - bytes_needed;
            self.logpage.set_bytes(recpos as usize, logrec);
            self.logpage.set_int(0, recpos);
        }

        self.latest_lsn += 1;
        Ok(self.latest_lsn)
    }

    fn append_new_block(
        fm: &FileManager,
        logfile: &str,
        logpage: &mut Page,
    ) -> Result<BlockId, io::Error> {
        let blk = fm.append(logfile)?;
        logpage.set_int(0, fm.block_size() as i32);
        fm.write(&blk, logpage)?;
        Ok(blk)
    }

    fn flush_internal(&mut self) -> Result<(), io::Error> {
        self.fm.write(&self.current_blk, &mut self.logpage)?;
        self.last_saved_lsn = self.latest_lsn;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::SimpleDB;
    use tempfile::tempdir;

    fn create_log_record(s: &str, n: i32) -> Vec<u8> {
        let spos = 0;
        let npos = Page::max_length(s.len());
        let mut page = Page::new(npos + INT_SIZE);
        page.set_string(spos, s);
        page.set_int(npos, n);
        page.to_vec()
    }

    fn create_records(lm: &mut LogManager, start: i32, end: i32) -> Vec<i32> {
        let mut lsns = Vec::new();
        for i in start..=end {
            let rec = create_log_record(&format!("record{}", i), i + 100);
            let lsn = lm.append(&rec).unwrap();
            lsns.push(lsn);
        }
        lsns
    }

    fn print_log_records(lm: &mut LogManager) -> Vec<(String, i32)> {
        let mut records = Vec::new();
        let iter = lm.iter().unwrap();

        for rec_result in iter {
            let rec = rec_result.unwrap();
            let page = Page::from_bytes(rec);
            let s = page.get_string(0);
            let npos = Page::max_length(s.len());
            let val = page.get_int(npos);
            records.push((s, val));
        }
        records
    }

    #[test]
    fn test_log_operations() {
        let temp_dir = tempdir().unwrap();
        let db_dir = temp_dir.path().to_path_buf();

        let mut db = SimpleDB::new(db_dir, 400, 8).unwrap();

        let lm = db.log_manager();

        // Test initial empty log
        let records = print_log_records(lm);
        assert!(records.is_empty(), "Initial log should be empty");

        // Create first batch of records
        let lsn1 = create_records(lm, 1, 35);
        assert_eq!(lsn1.len(), 35);

        // Verify first batch
        let records = print_log_records(lm);
        assert_eq!(records.len(), 35);
        assert_eq!(records[0].0, "record35");
        assert_eq!(records[0].1, 135);

        // Create second batch of records
        let lsns2 = create_records(lm, 36, 70);
        assert_eq!(lsns2.len(), 35);

        // Flush up to record 65
        lm.flush(65).unwrap();

        // Verify all records
        let records = print_log_records(lm);
        assert_eq!(records.len(), 70);
        assert_eq!(records[0].0, "record70");
        assert_eq!(records[0].1, 170);
        assert_eq!(records[69].0, "record1");
        assert_eq!(records[69].1, 101);
    }
}
