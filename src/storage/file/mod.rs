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

use std::cell::RefCell;
use std::cmp;
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::str::{self, FromStr};

use key_value_store::{Data, KeyValueStore, Retrieval, Storable};
use time_series::{RetrievalDirection, TimeSeries, Timestamp};

pub struct FileStorage<K, V> {
    file: RefCell<File>,
    item_size: usize,
    items: usize,
    first_key: K,
    last_key: K,
    end_offset: u64,
    _phantom: PhantomData<V>,
}

impl<K, V> FileStorage<K, V> where K: Storable<FileStorage<K, V>> + Ord, V: Storable<FileStorage<K, V>> {
    pub fn new(filename: &str) -> io::Result<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(filename)?;

        // Get the length of the file by seeking to the end
        let end = file.seek(SeekFrom::End(0))?;

        let item_size = K::size() + 1 + V::size() + 1;

        let items = if end as usize % item_size == 0 {
            end as usize / item_size
        } else {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "FileStorage file is an invalid size"));
        };

        // If the file is bigger than a single element,
        let (first_key, last_key, end_offset) = if end >= item_size as u64 {
            let mut buffer = vec![0u8; K::size()];

            // Seek to the beginning of the first item
            file.seek(SeekFrom::Start(0))?;
            let first_key = read_key::<K, V, File>(&mut file, &mut buffer)?;

            // Seek to the beginning of the last item
            let end_offset = file.seek(SeekFrom::End(-(item_size as i64)))?;
            let last_key = read_key::<K, V, File>(&mut file, &mut buffer)?;

            (first_key, last_key, end_offset)
        } else {
            (K::default(), K::default(), 0)
        };

        Ok(Self {
            file: RefCell::new(file),
            item_size: item_size,
            items: items,
            first_key: first_key,
            last_key: last_key,
            end_offset: end_offset,
            _phantom: PhantomData,
        })
    }

    /// Finds the key and offset of the first record that occurs on or before the search key.
    /// If the search key is before the first record, it returns the key and offset of the first record.
    fn find_from(&self, search_key: K) -> io::Result<(K, u64)> {
        // Scratch buffer into which we'll read new timestamps for parsing
        let mut read_buffer = vec![0u8; K::size()];

        let from_offset = if search_key >= self.first_key {
            binary_search_for_key::<K, V, File>(&mut *self.file.borrow_mut(), &mut read_buffer, Some(RetrievalDirection::Backward), search_key, 0, self.end_offset)?
        } else {
            0
        };

        self.file.borrow_mut().seek(SeekFrom::Start(from_offset))?;
        let from_key = cmp::max(read_key::<K, V, File>(&mut *self.file.borrow_mut(), &mut read_buffer)?, search_key);

        Ok((from_key, from_offset))
    }

    /// Finds the offset of the first record that occurs before the search key.
    fn find_to(&self, search_key: K) -> io::Result<u64> {
        // Scratch buffer into which we'll read new keys for parsing
        let mut read_buffer = vec![0u8; K::size()];

        let to_offset = binary_search_for_key::<K, V, File>(&mut *self.file.borrow_mut(), &mut read_buffer, Some(RetrievalDirection::Backward), search_key, 0, self.end_offset)?;

        self.file.borrow_mut().seek(SeekFrom::Start(to_offset))?;
        let to_key = read_key::<K, V, File>(&mut *self.file.borrow_mut(), &mut read_buffer)?;

        // find_to is exclusive.  If the bounding key is found exactly, exclude that record from the result.
        Ok(if to_key != search_key {
            to_offset
        } else if to_offset > 0 {
            to_offset - self.item_size as u64
        } else {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "find_to search key was equal to the first record"));
        })
    }
}

fn binary_search_for_key<K, V, F>(
    file: &mut F,
    buffer: &mut [u8],
    retrieval_direction: Option<RetrievalDirection>,
    search_key: K,
    start_offset: u64,
    end_offset: u64,
) -> io::Result<u64> where K: Storable<FileStorage<K, V>> + Ord, V: Storable<FileStorage<K, V>>, F: Read + Seek {
    if start_offset == end_offset {
        return Err(io::Error::new(io::ErrorKind::NotFound, "No items in search range"));
    }

    // Check the beginning of the range
    file.seek(SeekFrom::Start(start_offset))?;
    let start_key = read_key::<K, V, F>(file, buffer)?;

    if search_key < start_key {
        // If the search key is before the range, but we want to retrieve forward, return the beginning
        return if retrieval_direction != Some(RetrievalDirection::Forward) {
            Err(io::Error::new(io::ErrorKind::NotFound, "Search key is before the search range"))
        } else {
            Ok(start_offset)
        };
    } else if search_key == start_key {
        return Ok(start_offset);
    }

    // Check the end of the range
    file.seek(SeekFrom::Start(end_offset))?;
    let end_key = read_key::<K, V, F>(file, buffer)?;

    if search_key > end_key {
        // If the search key is after the range, but we want to retrieve backward, return the end
        return if retrieval_direction != Some(RetrievalDirection::Backward) {
            Err(io::Error::new(io::ErrorKind::NotFound, "Search timestamp is after the search range"))
        } else {
            Ok(end_offset)
        };
    } else if search_key == end_key {
        return Ok(end_offset);
    }

    fn bisect_and_descend<K, V, F>(
        file: &mut F,
        buffer: &mut [u8],
        retrieval_direction: Option<RetrievalDirection>,
        search_key: K,
        start_offset: u64,
        end_offset: u64,
    ) -> io::Result<u64> where K: Storable<FileStorage<K, V>> + Ord, V: Storable<FileStorage<K, V>>, F: Read + Seek {
        let range = end_offset - start_offset;
        let range_items = range / (K::size() + 1 + V::size() + 1) as u64;

        // If we've narrowed it down to just one item, the search key must occur between it and the next item.
        // Depending on the direction we want to retrieve, return it, the next item, or neither.
        if range_items == 1 {
            return match retrieval_direction {
                Some(RetrievalDirection::Forward) => Ok(end_offset),
                Some(RetrievalDirection::Backward) => Ok(start_offset),
                None => Err(io::Error::new(io::ErrorKind::NotFound, "Search key was not found")),
            };
        }

        let center_offset = start_offset + range_items / 2 * (K::size() + 1 + V::size() + 1) as u64;

        // Check the center of the range (rounded down)
        file.seek(SeekFrom::Start(center_offset))?;
        let center_key = read_key::<K, V, F>(file, buffer)?;

        // Descend into whichever half contains the search key
        if search_key < center_key {
            bisect_and_descend::<K, V, F>(file, buffer, retrieval_direction, search_key, start_offset, center_offset)
        } else if search_key > center_key {
            bisect_and_descend::<K, V, F>(file, buffer, retrieval_direction, search_key, center_offset, end_offset)
        } else {
            Ok(center_offset)
        }
    }

    bisect_and_descend::<K, V, F>(file, buffer, retrieval_direction, search_key, start_offset, end_offset)
}

fn read_key<K, V, F>(file: &mut F, buffer: &mut [u8]) -> io::Result<K> where K: Storable<FileStorage<K, V>> + Ord, V: Storable<FileStorage<K, V>>, F: Read {
    debug_assert_eq!(buffer.len(), K::size(), "read_key was passed a buffer of the wrong size");

    file.read_exact(buffer)?;

    if let Ok(str_buffer) = str::from_utf8(buffer) {
        Ok(K::from_bytes(str_buffer.as_bytes())?)
    } else {
        Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid data"))
    }
}

fn read_record<K, V, F>(file: &mut F, buffer: &mut [u8]) -> io::Result<(K, V)> where K: Storable<FileStorage<K, V>> + Ord, V: Storable<FileStorage<K, V>>, F: Read {
    debug_assert_eq!(buffer.len(), K::size() + 1 + V::size() + 1, "read_record was passed a buffer of the wrong size");

    file.read_exact(buffer)?;

    if let Ok(str_buffer) = str::from_utf8(buffer) {
        // Parse the string into chunks
        let mut parts = str_buffer.split_whitespace();

        Ok((
            K::from_bytes(parts.next().unwrap().as_bytes())?,    // The first chunk is the key
            V::from_bytes(parts.next().unwrap().as_bytes())?,    // The second chunk is the value
        ))
    } else {
        Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid data"))
    }
}

fn write_record<K, V, F>(file: &mut F, key: K, value: V) -> io::Result<()>  where K: Storable<FileStorage<K, V>> + Ord, V: Storable<FileStorage<K, V>>, F: Write {
    // We don't want to incur a write per part of the data
    let mut buffer = BufWriter::with_capacity(K::size() + 1 + V::size() + 1, file);

    // Format the data and write it
    buffer.write(&key.into_bytes())?;
    buffer.write(b" ")?;
    buffer.write(&value.into_bytes())?;
    buffer.write(b"\n")?;
    buffer.flush()
}

mod key_value_store;
mod pooled_time_series;
mod time_series;
