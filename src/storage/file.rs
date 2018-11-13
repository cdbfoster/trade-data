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
use std::io::{self, BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::ops::Range;
use std::str::{self, FromStr};

use {Data, Timestamp};
use storage::{Retrieval, RetrievalDirection, RetrievalOptions, Storable, Storage};

// The number of additional bytes stored per item
const PADDING: u64 = 15; // 14 bytes for the timestamp and a space, and then a newline at the end

pub struct FileStorage<T> {
    file: File,
    item_size: usize,
    items: usize,
    first_time: Timestamp,
    last_time: Timestamp,
    _phantom: PhantomData<T>,
}

impl<T> FileStorage<T> where T: Storable<FileStorage<T>> {
    pub fn new(filename: &str) -> io::Result<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(filename)?;

        // Get the length of the file by seeking to the end
        let end = file.seek(SeekFrom::End(0))?;

        let item_size = PADDING as usize + T::size();

        let items = if end as usize % item_size == 0 {
            end as usize / item_size
        } else {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "FileStorage file is an invalid size!"));
        };

        // If the file is bigger than a single element,
        let (first_time, last_time) = if end >= item_size as u64 {
            let mut buffer = vec![0u8; item_size];

            // Seek to the beginning of the first item
            file.seek(SeekFrom::Start(0))?;
            let first_time = read_record::<T, File>(&mut file, &mut buffer)?.0;

            // Seek to the beginning of the last item
            file.seek(SeekFrom::End(-(item_size as i64)))?;
            let last_time = read_record::<T, File>(&mut file, &mut buffer)?.0;

            (first_time, last_time)
        } else {
            (0, 0)
        };

        Ok(Self {
            file: file,
            item_size: item_size,
            items: items,
            first_time: first_time,
            last_time: last_time,
            _phantom: PhantomData,
        })
    }
}

impl<T> Storage for FileStorage<T> where T: Storable<FileStorage<T>> {
    fn store(&mut self, timestamp: Timestamp, data: Box<Data>) -> io::Result<()> {
        if timestamp <= self.last_time {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "Passed timestamp was equal to or before the last recorded timestamp!"));
        }

        if let Some(&data) = data.downcast_ref::<T>() {
            self.file.seek(SeekFrom::End(0))?;

            // Format the data and write it
            self.file.write(&format!("{:013} ", timestamp).into_bytes())?;
            self.file.write(&data.into_bytes())?;
            self.file.write(b"\n")?;

            self.items += 1;
            self.last_time = timestamp;

            Ok(())
        } else {
            Err(io::Error::new(io::ErrorKind::InvalidInput, "FileStorage was passed the wrong kind of data!"))
        }
    }

    fn retrieve(&self, timestamp: Timestamp, retrieval_direction: RetrievalDirection) -> io::Result<Retrieval> {
        Ok(Retrieval::new(Box::new((timestamp, 0))))
    }

    fn retrieve_all(&self, retrieval_options: RetrievalOptions) -> io::Result<Retrieval> {
        Ok(Retrieval::new(Box::new(Vec::<(Timestamp, T)>::new())))
    }

    fn retrieve_from(&self, timestamp: Timestamp, retrieval_options: RetrievalOptions) -> io::Result<Retrieval> {
        Ok(Retrieval::new(Box::new(Vec::<(Timestamp, T)>::new())))
    }

    fn retrieve_to(&self, timestamp: Timestamp, retrieval_options: RetrievalOptions) -> io::Result<Retrieval> {
        Ok(Retrieval::new(Box::new(Vec::<(Timestamp, T)>::new())))
    }

    fn retrieve_range(&self, range: Range<Timestamp>, retrieval_options: RetrievalOptions) -> io::Result<Retrieval> {
        Ok(Retrieval::new(Box::new(Vec::<(Timestamp, T)>::new())))
    }

    fn len(&self) -> usize {
        self.items
    }
}

fn read_record<T: Storable<FileStorage<T>>, U: Read>(file: &mut U, buffer: &mut [u8]) -> io::Result<(Timestamp, T)> {
    debug_assert_eq!(buffer.len(), PADDING as usize + T::size(), "read_record was passed a buffer of the wrong size!");

    file.read_exact(buffer)?;

    if let Ok(str_buffer) = str::from_utf8(buffer) {
        // Parse the string into chunks
        let mut parts = str_buffer.split_whitespace();

        Ok((
            Timestamp::from_str(parts.next().unwrap()).unwrap(), // The first chunk is the timestamp
            T::from_bytes(parts.next().unwrap().as_bytes())?,    // The second chunk is the data
        ))
    } else {
        Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid data!"))
    }
}

//fn binary_search_for_timestamp(file: &mut File, timestamp: Timestamp) ->

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

        fn from_bytes(buffer: &[u8]) -> io::Result<i32> {
            if let Ok(string) = String::from_utf8(buffer.to_vec()) {
                if let Ok(value) = i32::from_str(&string) {
                    return Ok(value);
                }
            }

            Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid data!"))
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
    fn test_file_storage_cannot_write_old_time() {
        let _setup_file = SetupFile::new("test_file_storage_cannot_write_old_time");

        let mut fs = FileStorage::<i32>::new("test_file_storage_cannot_write_old_time").unwrap();

        fs.store(2, Box::new(1)).unwrap();
        if fs.store(1, Box::new(2)).is_ok() || fs.store(2, Box::new(2)).is_ok() {
            panic!("Store should have failed here.");
        }
    }

    #[test]
    fn test_file_storage_reads_last_time() {
        let _setup_file = SetupFile::new("test_file_storage_reads_last_time");

        let mut fs = FileStorage::<i32>::new("test_file_storage_reads_last_time").unwrap();
        fs.store(1, Box::new(1)).unwrap();
        fs.store(2, Box::new(2)).unwrap();
        mem::drop(fs);

        let mut fs = FileStorage::<i32>::new("test_file_storage_reads_last_time").unwrap();
        if fs.store(2, Box::new(3)).is_ok() {
            panic!("Store should have failed here.");
        }
    }
}
