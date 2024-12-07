use simpledb::SimpleDB;
use tempfile::TempDir;

#[test]
fn test_simpledb_creation() {
    let temp_dir = TempDir::new().unwrap();
    let temp_path = temp_dir.path();

    let _db = SimpleDB::new(temp_path, 400, 8).unwrap();

    assert!(temp_path.exists());
    assert!(temp_path.is_dir());
}
