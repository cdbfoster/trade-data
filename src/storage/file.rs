// This file is part of trade-data.
//
// trade-data is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// trade-data is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with trade-data.  If not, see <http://www.gnu.org/licenses/>.

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::str::FromStr;

use {Data, Timestamp};
use storage::{Retrieve, Storable, Store};

// The number of additional bytes stored per item
const PADDING: u64 = 15; // 14 bytes for the timestamp and a space, and then a newline at the end

pub struct FileStorage<T> {
    file: File,
    position: u64,
    end: u64,
    last_time: Timestamp,
    _phantom: PhantomData<T>,
}

impl<T> FileStorage<T> where T: 'static + Copy + Storable<FileStorage<T>> {
    pub fn new(filename: &str) -> io::Result<Self> {
        let mut file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(filename)?;

        // Get the length of the file by seeking to the end
        let end = file.seek(SeekFrom::End(0))?;

        // If the file is bigger than a single element,
        let last_time = if end >= PADDING + T::size() as u64 {
            // Seek to the beginning of the last item
            file.seek(SeekFrom::End(-(PADDING as i64  + T::size() as i64)))?;

            // Read the rest of the file to a string
            let mut last_value = String::with_capacity(PADDING as usize + T::size());
            file.read_to_string(&mut last_value)?;

            // Split on whitespace and parse the first chunk
            let mut parts = last_value.split_whitespace();
            Timestamp::from_str(parts.next().unwrap()).unwrap()
        } else {
            0
        };

        Ok(Self {
            file: file,
            position: end,
            end: end,
            last_time: last_time,
            _phantom: PhantomData,
        })
    }
}

impl<T> Store for FileStorage<T> where T: 'static + Copy + Storable<FileStorage<T>> {
    fn store(&mut self, timestamp: Timestamp, data: Box<Data>) -> io::Result<()> {
        if timestamp < self.last_time {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "Passed timestamp was before the last recorded timestamp!"));
        }

        if let Some(&data) = data.downcast_ref::<T>() {
            // Go to the end if we're not there already
            if self.position != self.end {
                self.file.seek(SeekFrom::End(0))?;
            }

            // Format the data and write it, getting the number of bytes written
            let mut offset = self.file.write(&format!("{:013} ", timestamp).into_bytes())? as u64;
            offset += self.file.write(&data.into_bytes())? as u64;
            offset += self.file.write(b"\n")? as u64;

            // Update our end and position
            self.end += offset;
            self.position = self.end;

            self.last_time = timestamp;

            Ok(())
        } else {
            Err(io::Error::new(io::ErrorKind::InvalidInput, "FileStorage was passed the wrong kind of data!"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::fs;
    use std::mem;

    struct SetupFile {
        filename: &'static str,
    }

    impl SetupFile {
        fn new(filename: &'static str) -> Self {
            fs::remove_file(filename).ok();
            Self {
                filename: filename,
            }
        }
    }

    impl Drop for SetupFile {
        fn drop(&mut self) {
            fs::remove_file(self.filename).ok();
        }
    }

    impl Storable<FileStorage<i32>> for i32 {
        fn size() -> usize {
            4
        }

        fn into_bytes(self) -> Vec<u8> {
            format!("{:4}", self).into_bytes()
        }
    }

    #[test]
    fn test_file_storage_store() {
        let _setup_file = SetupFile::new("test_file_storage_store");

        let mut fs = FileStorage::<i32>::new("test_file_storage_store").unwrap();

        fs.store(1, Box::new(1)).unwrap();
        fs.store(2, Box::new(2)).unwrap();
        fs.store(3, Box::new(3)).unwrap();

        mem::drop(fs);

        // Read in the values we wrote and compare to what we expected
        let mut value = String::new();
        File::open("test_file_storage_store").unwrap().read_to_string(&mut value).unwrap();
        assert_eq!(&value.into_bytes(), &String::from("0000000000001    1\n0000000000002    2\n0000000000003    3\n").into_bytes());
    }

    #[test]
    #[should_panic]
    fn test_file_storage_cannot_write_old_time() {
        let _setup_file = SetupFile::new("test_file_storage_cannot_write_old_time");

        let mut fs = FileStorage::<i32>::new("test_file_storage_cannot_write_old_time").unwrap();

        fs.store(2, Box::new(1)).unwrap();
        fs.store(1, Box::new(2)).unwrap();
    }

    #[test]
    #[should_panic]
    fn test_file_storage_reads_last_time() {
        let _setup_file = SetupFile::new("test_file_storage_reads_last_time");

        let mut fs = FileStorage::<i32>::new("test_file_storage_reads_last_time").unwrap();
        fs.store(1, Box::new(1)).unwrap();
        fs.store(2, Box::new(2)).unwrap();
        mem::drop(fs);

        let mut fs = FileStorage::<i32>::new("test_file_storage_reads_last_time").unwrap();
        fs.store(1, Box::new(3)).unwrap();
    }
}
