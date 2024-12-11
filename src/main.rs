use simpledb::SimpleDB;

fn main() -> std::io::Result<()> {
    let _db = SimpleDB::new("filetest", 400, 8)?;

    Ok(())
}
