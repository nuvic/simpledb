use simpledb::{BlockId, SimpleDB};

fn main() -> std::io::Result<()> {
    let db = SimpleDB::new("filetest", 400, 8)?;
    let fm = db.file_manager();
    let block = BlockId::new("testfile", 2);

    Ok(())
}
