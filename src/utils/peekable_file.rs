use std::{
    io::{self, BufRead, BufReader, Read, Seek},
    os::unix::ffi::OsStringExt,
    path::PathBuf,
};

/// Parses hybrids between csv and binary files.
pub struct HybridFileParser<R> {
    reader: BufReader<R>,
}

pub trait PeekableFile: Seek + Read {}
impl<T: Seek + Read> PeekableFile for T {}

impl<R: Read> Read for HybridFileParser<R> {
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        return self.reader.read(buf);
    }
}

impl<R: Read + Seek> HybridFileParser<R> {
    pub fn new(read: R) -> Self {
        return HybridFileParser {
            reader: BufReader::new(read),
        };
    }

    pub fn is_depleted(&mut self) -> bool {
        let buf = self.reader.fill_buf();
        buf.is_err() || buf.unwrap().is_empty()
    }

    pub fn read_until_limited(&mut self, limit: usize, delimiter: u8) -> io::Result<Vec<u8>> {
        let mut res = vec![];
        loop {
            let buf = self.reader.fill_buf()?;
            if buf.is_empty() {
                break;
            }
            let bytes_read = buf.len();
            for (i, next_byte) in buf.iter().enumerate() {
                if *next_byte == delimiter {
                    self.reader.consume(i + 1);
                    return Ok(res);
                }

                res.push(*next_byte);
                if res.len() >= limit {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Didn't find expected delimiter.",
                    ));
                }
            }
            self.reader.consume(bytes_read);
        }

        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "Reached end of file instead of delimiter.",
        ));
    }

    pub fn read_path(&mut self, num_exact_bytes: usize) -> io::Result<PathBuf> {
        let mut buf: Vec<u8> = vec![0u8; num_exact_bytes];
        self.reader.read_exact(buf.as_mut_slice())?;
        let path = PathBuf::from(std::ffi::OsString::from_vec(buf));
        return Ok(path);
    }

    pub fn read_string(&mut self, limit: usize, delimiter: u8) -> io::Result<String> {
        let bytes = self.read_until_limited(limit, delimiter)?;
        return String::from_utf8(bytes).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Expected UTF-8 data {}", e),
            )
        });
    }
    pub fn read_u64(&mut self, delimiter: u8) -> io::Result<u64> {
        return self
            .read_string(30, delimiter)?
            .trim()
            .parse()
            .map_err(|e| {
                io::Error::new(io::ErrorKind::InvalidData, format!("Expected int: {}", e))
            });
    }
    pub fn read_i64(&mut self, delimiter: u8) -> io::Result<i64> {
        return self
            .read_string(30, delimiter)?
            .trim()
            .parse()
            .map_err(|e| {
                io::Error::new(io::ErrorKind::InvalidData, format!("Expected int: {}", e))
            });
    }

    pub fn read_hex_u64(&mut self, delimiter: u8) -> io::Result<u64> {
        const LIMIT: usize = 30; // 64/16+1
        let hex_string = self.read_string(LIMIT, delimiter)?;
        return u64::from_str_radix(&hex_string.trim(), 16).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Expected hex string, error: {}", e),
            )
        });
    }
    pub fn skip_bytes(&mut self, num: i64) -> io::Result<()> {
        self.reader.seek_relative(num)
    }
}
