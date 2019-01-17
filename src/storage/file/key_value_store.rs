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

use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};

use key_value_store::{Data, KeyValueStore, Retrieval, Storable};
use storage::file::{FileStorage, write_record};

impl<K, V> KeyValueStore for FileStorage<K, V> where K: Storable<FileStorage<K, V>> + Ord, V: Storable<FileStorage<K, V>> {
    fn len(&self) -> usize {
        self.items
    }

    fn store(&mut self, key: Box<Data>, value: Box<Data>) -> io::Result<()> {
        let key = if let Some(&key) = key.downcast_ref::<K>() {
            key
        } else {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "FileStorage was passed the wrong kind of key"));
        };

        if self.items > 0 && key <= self.last_key {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "Passed key was equal to or before the last recorded key"));
        }

        if let Some(&value) = value.downcast_ref::<V>() {
            self.file.borrow_mut().seek(SeekFrom::End(0))?;

            write_record(&mut *self.file.borrow_mut(), key, value)?;

            if self.items == 0 {
                self.first_key = key;
            } else {
                self.end_offset += self.item_size as u64;
            }

            self.items += 1;
            self.last_key = key;

            Ok(())
        } else {
            Err(io::Error::new(io::ErrorKind::InvalidInput, "FileStorage was passed the wrong kind of data"))
        }
    }

    //fn retrieve(&self, key: Box<Data>) -> io::Result<Retrieval> {}
}


#[cfg(test)]
mod tests {
    use super::*;

    use std::fs::File;
    use std::mem;

    use time_series::Timestamp;
    use util::SetupFile;

    #[test]
    fn test_cannot_write_old_time() {
        let _setup_file = SetupFile::new("test_cannot_write_old_time");

        let mut fs = FileStorage::<Timestamp, i32>::new("test_cannot_write_old_time").unwrap();

        fs.store(Box::new(2 as Timestamp), Box::new(1 as i32)).unwrap();
        if fs.store(Box::new(1 as Timestamp), Box::new(2 as i32)).is_ok() || fs.store(Box::new(2 as Timestamp), Box::new(2 as i32)).is_ok() {
            panic!("Store should have failed here.");
        }
    }

    #[test]
    fn test_len() {
        let _setup_file = SetupFile::new("test_len");

        let mut fs = FileStorage::<Timestamp, i32>::new("test_len").unwrap();

        fs.store(Box::new(10 as Timestamp), Box::new(1 as i32)).unwrap();
        fs.store(Box::new(14 as Timestamp), Box::new(2 as i32)).unwrap();
        fs.store(Box::new(15 as Timestamp), Box::new(3 as i32)).unwrap();
        fs.store(Box::new(20 as Timestamp), Box::new(4 as i32)).unwrap();
        fs.store(Box::new(26 as Timestamp), Box::new(5 as i32)).unwrap();

        assert_eq!(fs.len(), 5);
    }

    #[test]
    fn test_reads_last_time() {
        let _setup_file = SetupFile::new("test_reads_last_time");

        let mut fs = FileStorage::<Timestamp, i32>::new("test_reads_last_time").unwrap();
        fs.store(Box::new(1 as Timestamp), Box::new(1 as i32)).unwrap();
        fs.store(Box::new(2 as Timestamp), Box::new(2 as i32)).unwrap();
        mem::drop(fs);

        let mut fs = FileStorage::<Timestamp, i32>::new("test_reads_last_time").unwrap();
        if fs.store(Box::new(2 as Timestamp), Box::new(3 as i32)).is_ok() {
            panic!("Store should have failed here.");
        }
    }

    //#[test]
    //fn test_retrieve() { }

    #[test]
    fn test_store() {
        let _setup_file = SetupFile::new("test_store");

        let mut fs = FileStorage::<Timestamp, i32>::new("test_store").unwrap();

        fs.store(Box::new(1 as Timestamp), Box::new(1 as i32)).unwrap();
        fs.store(Box::new(2 as Timestamp), Box::new(2 as i32)).unwrap();
        fs.store(Box::new(3 as Timestamp), Box::new(3 as i32)).unwrap();

        mem::drop(fs);

        // Read in the values we wrote and compare to what we expected
        let mut value = String::new();
        File::open("test_store").unwrap().read_to_string(&mut value).unwrap();
        assert_eq!(&value.into_bytes(), &String::from("0000000000001    1\n0000000000002    2\n0000000000003    3\n").into_bytes());
    }
}
