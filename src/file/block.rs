pub struct BlockId {
    filename: String,
    number: u64,
}

impl BlockId {
    pub fn new(filename: impl Into<String>, number: u64) -> Self {
        Self {
            filename: filename.into(),
            number,
        }
    }

    pub fn filename(&self) -> &str {
        &self.filename
    }

    pub fn number(&self) -> u64 {
        self.number
    }
}

impl std::fmt::Display for BlockId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[file: {}, block: {}]", self.filename, self.number)
    }
}
