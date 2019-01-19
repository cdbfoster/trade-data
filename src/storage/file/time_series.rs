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

use std::fs::File;
use std::io::{self, BufReader, Seek, SeekFrom};
use std::ops::Range;

use key_value_store::{Retrieval, Storable};
use storage::file::{binary_search_for_key, FileStorage, read_record};
use time_series::{RetrievalDirection, TimeSeries, Timestamp};

impl<V> TimeSeries for FileStorage<Timestamp, V> where V: Storable<FileStorage<Timestamp, V>> {
    fn retrieve_nearest(&self, timestamp: Timestamp, retrieval_direction: Option<RetrievalDirection>) -> io::Result<Retrieval> {
        let mut file = self.file.borrow_mut();

        let record_offset = {
            let mut read_buffer = vec![0u8; <Timestamp as Storable<FileStorage<Timestamp, V>>>::size()];
            binary_search_for_key::<Timestamp, V, File>(&mut file, &mut read_buffer, retrieval_direction, timestamp, 0, self.end_offset)?
        };
        file.seek(SeekFrom::Start(record_offset))?;

        let mut read_buffer = vec![0u8; self.item_size];

        Ok(Retrieval::new(Box::new(read_record::<Timestamp, V, File>(&mut file, &mut read_buffer)?)))
    }

    fn retrieve_all(&self) -> io::Result<Retrieval> {
        // Buffer the file to reduce the number of disk reads
        let file = &mut *self.file.borrow_mut();
        let mut file_buffer = BufReader::new(file);
        file_buffer.seek(SeekFrom::Start(0))?;

        let mut results = Vec::with_capacity(self.items);

        let mut read_buffer = vec![0u8; self.item_size];
        for _ in 0..self.items {
            results.push(read_record::<Timestamp, V, BufReader<&mut File>>(&mut file_buffer, &mut read_buffer)?);
        }

        Ok(Retrieval::new(Box::new(results)))
    }

    fn retrieve_from(&self, timestamp: Timestamp) -> io::Result<Retrieval> {
        // Don't use self.find_from because that wants to grab the record on or before the timestamp, not on or after
        let from_offset = {
            if timestamp <= self.last_key {
                let mut read_buffer = vec![0u8; <Timestamp as Storable<FileStorage<Timestamp, V>>>::size()];
                binary_search_for_key::<Timestamp, V, File>(&mut self.file.borrow_mut(), &mut read_buffer, Some(RetrievalDirection::Forward), timestamp, 0, self.end_offset)?
            } else {
                return Ok(Retrieval::new(Box::new(Vec::<(Timestamp, V)>::new())));
            }
        };

        // Buffer the file to reduce the number of disk reads
        let file = &mut *self.file.borrow_mut();
        let mut file_buffer = BufReader::new(file);
        file_buffer.seek(SeekFrom::Start(from_offset))?;

        let from_item = from_offset as usize / self.item_size;

        let mut results = Vec::with_capacity(self.items - from_item);

        let mut read_buffer = vec![0u8; self.item_size];
        for _ in from_item..self.items {
            results.push(read_record::<Timestamp, V, BufReader<&mut File>>(&mut file_buffer, &mut read_buffer)?);
        }

        Ok(Retrieval::new(Box::new(results)))
    }

    fn retrieve_to(&self, timestamp: Timestamp) -> io::Result<Retrieval> {
        let to_offset = match self.find_to(timestamp) {
            Ok(offset) => offset,
            Err(error) => return if error.kind() == io::ErrorKind::InvalidInput || error.kind() == io::ErrorKind::NotFound {
                Ok(Retrieval::new(Box::new(Vec::<(Timestamp, V)>::new())))
            } else {
                Err(error)
            },
        };

        // Buffer the file to reduce the number of disk reads
        let file = &mut *self.file.borrow_mut();
        let mut file_buffer = BufReader::new(file);
        file_buffer.seek(SeekFrom::Start(0))?;

        let to_item = to_offset as usize / self.item_size + 1;

        let mut results = Vec::with_capacity(to_item);

        let mut read_buffer = vec![0u8; self.item_size];
        for _ in 0..to_item {
            results.push(read_record::<Timestamp, V, BufReader<&mut File>>(&mut file_buffer, &mut read_buffer)?);
        }

        Ok(Retrieval::new(Box::new(results)))
    }

    fn retrieve_range(&self, range: Range<Timestamp>) -> io::Result<Retrieval> {
        // Don't use self.find_from because that wants to grab the record on or before the timestamp, not on or after
        let from_offset = {
            let mut read_buffer = vec![0u8; <Timestamp as Storable<FileStorage<Timestamp, V>>>::size()];
            if range.start <= self.last_key {
                binary_search_for_key::<Timestamp, V, File>(&mut self.file.borrow_mut(), &mut read_buffer, Some(RetrievalDirection::Forward), range.start, 0, self.end_offset)?
            } else {
                return Ok(Retrieval::new(Box::new(Vec::<(Timestamp, V)>::new())));
            }
        };

        let to_offset = match self.find_to(range.end) {
            Ok(offset) => offset,
            Err(error) => return if error.kind() == io::ErrorKind::InvalidInput || error.kind() == io::ErrorKind::NotFound {
                Ok(Retrieval::new(Box::new(Vec::<(Timestamp, V)>::new())))
            } else {
                Err(error)
            },
        };

        // Buffer the file to reduce the number of disk reads
        let file = &mut *self.file.borrow_mut();
        let mut file_buffer = BufReader::new(file);
        file_buffer.seek(SeekFrom::Start(from_offset))?;

        let from_item = from_offset as usize / self.item_size;
        let to_item = to_offset as usize / self.item_size + 1;

        let mut results = Vec::with_capacity(to_item - from_item);

        let mut read_buffer = vec![0u8; self.item_size];
        for _ in from_item..to_item {
            results.push(read_record::<Timestamp, V, BufReader<&mut File>>(&mut file_buffer, &mut read_buffer)?);
        }

        Ok(Retrieval::new(Box::new(results)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use key_value_store::KeyValueStore;
    use util::SetupFile;

    #[test]
    fn test_retrieve_nearest() {
        let _setup_file = SetupFile::new("test_retrieve_nearest");

        let mut fs = FileStorage::<Timestamp, i32>::new("test_retrieve_nearest").unwrap();

        fs.store(Box::new(10 as Timestamp), Box::new(1 as i32)).unwrap();
        fs.store(Box::new(20 as Timestamp), Box::new(2 as i32)).unwrap();
        fs.store(Box::new(30 as Timestamp), Box::new(3 as i32)).unwrap();
        fs.store(Box::new(40 as Timestamp), Box::new(4 as i32)).unwrap();

        let retrieval = fs.retrieve_nearest(5, Some(RetrievalDirection::Forward)).unwrap();
        assert_eq!(retrieval.as_single::<Timestamp, i32>(), Some(&(10, 1)));

        assert!(fs.retrieve_nearest(5, Some(RetrievalDirection::Backward)).is_err());

        assert!(fs.retrieve_nearest(15, None).is_err());

        let retrieval = fs.retrieve_nearest(25, Some(RetrievalDirection::Backward)).unwrap();
        assert_eq!(retrieval.as_single::<Timestamp, i32>(), Some(&(20, 2)));
    }

    #[test]
    fn test_retrieve_all() {
        let _setup_file = SetupFile::new("test_retrieve_all");

        let mut fs = FileStorage::<Timestamp, i32>::new("test_retrieve_all").unwrap();

        fs.store(Box::new(10 as Timestamp), Box::new(1 as i32)).unwrap();
        fs.store(Box::new(20 as Timestamp), Box::new(2 as i32)).unwrap();
        fs.store(Box::new(30 as Timestamp), Box::new(3 as i32)).unwrap();
        fs.store(Box::new(40 as Timestamp), Box::new(4 as i32)).unwrap();

        let retrieval = fs.retrieve_all().unwrap();
        assert_eq!(retrieval.as_vec::<Timestamp, i32>(), Some(&vec![(10, 1), (20, 2), (30, 3), (40, 4)]));
    }

    #[test]
    fn test_retrieve_from() {
        let _setup_file = SetupFile::new("test_retrieve_from");

        let mut fs = FileStorage::<Timestamp, i32>::new("test_retrieve_from").unwrap();

        fs.store(Box::new(10 as Timestamp), Box::new(1 as i32)).unwrap();
        fs.store(Box::new(20 as Timestamp), Box::new(2 as i32)).unwrap();
        fs.store(Box::new(30 as Timestamp), Box::new(3 as i32)).unwrap();
        fs.store(Box::new(40 as Timestamp), Box::new(4 as i32)).unwrap();

        let retrieval = fs.retrieve_from(9).unwrap();
        assert_eq!(retrieval.as_vec::<Timestamp, i32>(), Some(&vec![(10, 1), (20, 2), (30, 3), (40, 4)]));

        let retrieval = fs.retrieve_from(12).unwrap();
        assert_eq!(retrieval.as_vec::<Timestamp, i32>(), Some(&vec![(20, 2), (30, 3), (40, 4)]));

        let retrieval = fs.retrieve_from(40).unwrap();
        assert_eq!(retrieval.as_vec::<Timestamp, i32>(), Some(&vec![(40, 4)]));

        let retrieval = fs.retrieve_from(44).unwrap();
        assert_eq!(retrieval.as_vec::<Timestamp, i32>(), Some(&vec![]));
    }

    #[test]
    fn test_retrieve_to() {
        let _setup_file = SetupFile::new("test_retrieve_to");

        let mut fs = FileStorage::<Timestamp, i32>::new("test_retrieve_to").unwrap();

        fs.store(Box::new(10 as Timestamp), Box::new(1 as i32)).unwrap();
        fs.store(Box::new(20 as Timestamp), Box::new(2 as i32)).unwrap();
        fs.store(Box::new(30 as Timestamp), Box::new(3 as i32)).unwrap();
        fs.store(Box::new(40 as Timestamp), Box::new(4 as i32)).unwrap();

        let retrieval = fs.retrieve_to(9).unwrap();
        assert_eq!(retrieval.as_vec::<Timestamp, i32>(), Some(&vec![]));

        let retrieval = fs.retrieve_to(12).unwrap();
        assert_eq!(retrieval.as_vec::<Timestamp, i32>(), Some(&vec![(10, 1)]));

        let retrieval = fs.retrieve_to(40).unwrap();
        assert_eq!(retrieval.as_vec::<Timestamp, i32>(), Some(&vec![(10, 1), (20, 2), (30, 3)]));

        let retrieval = fs.retrieve_to(44).unwrap();
        assert_eq!(retrieval.as_vec::<Timestamp, i32>(), Some(&vec![(10, 1), (20, 2), (30, 3), (40, 4)]));
    }

    #[test]
    fn test_retrieve_range() {
        let _setup_file = SetupFile::new("test_retrieve_range");

        let mut fs = FileStorage::<Timestamp, i32>::new("test_retrieve_range").unwrap();

        fs.store(Box::new(10 as Timestamp), Box::new(1 as i32)).unwrap();
        fs.store(Box::new(20 as Timestamp), Box::new(2 as i32)).unwrap();
        fs.store(Box::new(30 as Timestamp), Box::new(3 as i32)).unwrap();
        fs.store(Box::new(40 as Timestamp), Box::new(4 as i32)).unwrap();

        let retrieval = fs.retrieve_range(9..21).unwrap();
        assert_eq!(retrieval.as_vec::<Timestamp, i32>(), Some(&vec![(10, 1), (20, 2)]));

        let retrieval = fs.retrieve_range(9..30).unwrap();
        assert_eq!(retrieval.as_vec::<Timestamp, i32>(), Some(&vec![(10, 1), (20, 2)]));

        let retrieval = fs.retrieve_range(10..31).unwrap();
        assert_eq!(retrieval.as_vec::<Timestamp, i32>(), Some(&vec![(10, 1), (20, 2), (30, 3)]));

        let retrieval = fs.retrieve_range(21..44).unwrap();
        assert_eq!(retrieval.as_vec::<Timestamp, i32>(), Some(&vec![(30, 3), (40, 4)]));
    }
}
