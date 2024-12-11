pub mod db;
pub mod file;
pub mod log;

pub use db::SimpleDB;
pub use file::{BlockId, FileManager};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_blockid_creation() {
        let block = BlockId::new("test.txt", 1);
        assert_eq!(block.filename(), "test.txt");
        assert_eq!(block.number(), 1);
    }
}
