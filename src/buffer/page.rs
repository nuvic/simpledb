use crate::{
    file::{BlockId, FileManager, Page},
    log::LogManager,
};
use std::sync::{Arc, Mutex};

pub struct BufferPage {
    fm: Arc<FileManager>,
    lm: Arc<Mutex<LogManager>>,
    contents: Page,
    block: Option<BlockId>,
    pins: u32,
    txnum: i32,
    lsn: i32,
}

// An individual buffer. A databuffer wraps a page
// and stores information about its status,
// such as the associated disk block,
// the number of times the buffer has been pinned,
// whether its contents have been modified,
// and if so, the id and lsn of the modifying transaction.
impl BufferPage {
    pub fn new(fm: Arc<FileManager>, lm: Arc<Mutex<LogManager>>) -> Self {
        let block_size = fm.block_size();
        BufferPage {
            fm,
            lm,
            contents: Page::new(block_size),
            block: None,
            pins: 0,
            txnum: -1,
            lsn: -1,
        }
    }

    pub fn contents(&mut self) -> &mut Page {
        &mut self.contents
    }

    pub fn block(&self) -> Option<&BlockId> {
        self.block.as_ref()
    }

    pub fn set_modified(&mut self, txnum: i32, lsn: i32) {
        self.txnum = txnum;
        if lsn >= 0 {
            self.lsn = lsn;
        }
    }

    // Return true if the buffer is currently pinned
    // (that is, if it has a nonzero pin count).
    pub fn is_pinned(&self) -> bool {
        self.pins > 0
    }

    pub fn modifying_txn(&self) -> i32 {
        self.txnum
    }

    // Reads the contents of the specified block into
    // the contents of the buffer.
    // If the buffer was dirty, then its previous contents
    // are first written to disk.
    pub fn assign_to_block(&mut self, b: BlockId) -> std::io::Result<()> {
        self.flush()?;
        self.block = Some(b.clone());
        self.fm.read(&b, &mut self.contents)?;
        self.pins = 0;
        Ok(())
    }

    // Write the buffer to its disk block if it is dirty.
    pub fn flush(&mut self) -> std::io::Result<()> {
        if self.txnum >= 0 {
            self.lm.lock().unwrap().flush(self.lsn)?;
            if let Some(block) = &self.block {
                self.fm.write(block, &mut self.contents)?;
            }
            self.txnum = -1;
        }
        Ok(())
    }

    pub fn pin(&mut self) {
        self.pins += 1;
    }

    pub fn unpin(&mut self) {
        if self.pins > 0 {
            self.pins -= 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn setup() -> (Arc<FileManager>, Arc<Mutex<LogManager>>, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let fm = Arc::new(FileManager::new(temp_dir.path(), 400).unwrap());
        let lm = Arc::new(Mutex::new(
            LogManager::new(fm.clone(), "test.log".to_string()).unwrap(),
        ));
        (fm, lm, temp_dir)
    }

    fn init_file(fm: &FileManager, filename: &str, blocknum: u64) -> std::io::Result<()> {
        let block = BlockId::new(filename.to_string(), blocknum);
        let mut page = Page::new(fm.block_size());
        fm.write(&block, &mut page)
    }

    #[test]
    fn test_buffer_page_operations() -> std::io::Result<()> {
        let (fm, lm, _temp_dir) = setup();
        init_file(&fm, "testfile", 1)?;

        let mut buffer = BufferPage::new(fm.clone(), lm.clone());

        // Test initial state
        assert!(!buffer.is_pinned());
        assert_eq!(buffer.modifying_txn(), -1);

        // Test pinning
        buffer.pin();
        assert!(buffer.is_pinned());
        assert_eq!(buffer.pins, 1);

        // Test unpinning
        buffer.unpin();
        assert!(!buffer.is_pinned());

        // Test block assignment and content modification
        let block = BlockId::new("testfile".to_string(), 1);
        buffer.assign_to_block(block.clone())?;

        // Modify contents
        {
            let page = buffer.contents();
            page.set_int(80, 42);
        }

        // Mark as modified
        buffer.set_modified(1, 0);
        assert_eq!(buffer.modifying_txn(), 1);

        // Test flush
        buffer.flush()?;
        assert_eq!(buffer.modifying_txn(), -1);

        Ok(())
    }

    #[test]
    fn test_multiple_modifications() -> std::io::Result<()> {
        let (fm, lm, _temp_dir) = setup();
        init_file(&fm, "testfile", 1)?;

        let mut buffer = BufferPage::new(fm.clone(), lm.clone());
        let block = BlockId::new("testfile".to_string(), 1);

        buffer.assign_to_block(block.clone())?;

        // First modification
        {
            let page = buffer.contents();
            page.set_int(80, 100);
        }
        buffer.set_modified(1, 0);
        buffer.flush()?;

        // Second modification
        {
            let page = buffer.contents();
            page.set_int(80, 200);
        }
        buffer.set_modified(2, 1);

        assert_eq!(buffer.modifying_txn(), 2);

        Ok(())
    }
}
