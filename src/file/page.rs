use std::convert::TryInto;

pub struct Page {
    buffer: Vec<u8>,
}

impl Page {
    // Create a new page with specified block_size
    pub fn new(block_size: usize) -> Self {
        Self {
            buffer: vec![0; block_size],
        }
    }

    // Create a page from existing bytes
    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        Self { buffer: bytes }
    }

    pub fn get_int(&self, offset: usize) -> i32 {
        let bytes = &self.buffer[offset..offset + 4];
        i32::from_be_bytes(bytes.try_into().unwrap())
    }

    pub fn set_int(&mut self, offset: usize, value: i32) {
        let bytes = value.to_be_bytes();
        self.buffer[offset..offset + 4].copy_from_slice(&bytes);
    }

    pub fn get_bytes(&self, offset: usize) -> Vec<u8> {
        let length = self.get_int(offset) as usize;
        let start = offset + 4;
        self.buffer[start..start + length].to_vec()
    }

    pub fn set_bytes(&mut self, offset: usize, bytes: &[u8]) {
        self.set_int(offset, bytes.len() as i32);
        let start = offset + 4;
        self.buffer[start..start + bytes.len()].copy_from_slice(bytes);
    }

    pub fn get_string(&self, offset: usize) -> String {
        let bytes = self.get_bytes(offset);
        String::from_utf8(bytes).unwrap_or_default()
    }

    pub fn set_string(&mut self, offset: usize, s: &str) {
        self.set_bytes(offset, s.as_bytes());
    }

    // get max length needed for a string of given length
    pub fn max_length(strlen: usize) -> usize {
        // 4 bytes for length + UTF-8 chars
        4 + (strlen * 4)
    }

    // Similar to Java's contents() but returns mutable slice for direct writing
    pub(crate) fn contents(&mut self) -> &mut [u8] {
        &mut self.buffer[..]
    }

    pub fn length(&self) -> usize {
        self.buffer.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_int_operations() {
        let mut page = Page::new(100);
        page.set_int(2, 42);
        assert_eq!(page.get_int(2), 42);
    }

    #[test]
    fn test_string_operations() {
        let mut page = Page::new(100);
        let test_str = "Hello, world!";
        page.set_string(5, test_str);
        assert_eq!(page.get_string(5), test_str);
    }
}
