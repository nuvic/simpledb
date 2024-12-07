use std::error::Error;
use std::fmt;

#[derive(Debug)]
pub enum DbError {
    IoError(std::io::Error),
    InvalidBlockSize,
    InvalidBufferSize,
}

