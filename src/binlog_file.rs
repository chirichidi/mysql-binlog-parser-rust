use std::io::{Seek, Read};
use std::path::Path;
use std::fs::File;
use crate::errors::BinlogFileError;

pub struct BinlogFile<I: Seek + Read> {
    file: I,
    event_set_start_offset: u64
}

impl BinlogFile<File> {
    pub fn from_path<P: AsRef<Path>>(path: P) -> Result<Self, BinlogFileError> {
        // need it?
        // match path.as_ref().extension() {
        //     Some(s) => {
        //         if s.to_str() == "gz" {
        //
        //         }
        //     }
        //     _ => {}
        // }
        let file = File::open(path.as_ref()).map_err(BinlogFileError::OpenError)?;
        Self::from_reader(file)
    }
}

impl<I> BinlogFile<I> where
    I: Seek + Read
{
    pub fn from_reader(mut reader: I) -> Result<Self, BinlogFileError> {
        // https://dev.mysql.com/doc/internals/en/binary-log-structure-and-contents.html
        let mut magic_number_bytes = [0u8; 4];
        reader.read_exact(&mut magic_number_bytes)?;
        if magic_number_bytes != [0xfe, 0x62, 0x69, 0x6e] {
            return Err(BinlogFileError::BadMagic(magic_number_bytes));
        }

        Ok(BinlogFile {
            file: reader,
            event_set_start_offset: 4
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::binlog_file::BinlogFile;

    #[test]
    fn test_open_binlog_file() {
        //given
        let path = "tests/asset/mysql-bin.100746";

        //when
        let binlog_file = BinlogFile::from_path(path).unwrap();

        //then
        assert_eq!(binlog_file.event_set_start_offset, 4);
    }
}