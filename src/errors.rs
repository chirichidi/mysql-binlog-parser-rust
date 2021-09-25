use thiserror::Error;

#[derive(Error, Debug)]
pub enum BinlogFileError {
    #[error("error parsing event")]
    EventParseError(#[from] EventParseError),
    #[error("bad magic value at start of binlog: got {0:?}")]
    BadMagic([u8; 4]),
    #[error("error opening binlog file")]
    OpenError(std::io::Error),
    #[error("other I/O error reading binlog file")]
    Io(#[from] std::io::Error),
}

#[derive(Error, Debug)]
pub enum EventParseError {
    #[error("I/O error reading column: {0:?}")]
    Io(#[from] std::io::Error),
}
