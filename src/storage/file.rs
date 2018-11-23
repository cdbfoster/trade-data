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
use std::ops::Range;
use std::str::{self, FromStr};

use {Data, Timestamp};
use storage::{GapFillMethod, PoolingMethod, Retrieval, RetrievalDirection, RetrievalOptions, Storable, Storage};

// The number of bytes a timestamp occupies in the file
const TIMESTAMP_SIZE: u64 = 13;

// The number of additional bytes stored per item
const PADDING: u64 = TIMESTAMP_SIZE + 2; // 14 bytes for the timestamp and a space, and then a newline at the end

pub struct FileStorage<T> {
    file: RefCell<File>,
    item_size: usize,
    items: usize,
    first_time: Timestamp,
    last_time: Timestamp,
    end_offset: u64,
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
            return Err(io::Error::new(io::ErrorKind::InvalidData, "FileStorage file is an invalid size"));
        };

        // If the file is bigger than a single element,
        let (first_time, last_time, end_offset) = if end >= item_size as u64 {
            let mut buffer = vec![0u8; item_size];

            // Seek to the beginning of the first item
            file.seek(SeekFrom::Start(0))?;
            let first_time = read_record::<T, File>(&mut file, &mut buffer)?.0;

            // Seek to the beginning of the last item
            let end_offset = file.seek(SeekFrom::End(-(item_size as i64)))?;
            let last_time = read_record::<T, File>(&mut file, &mut buffer)?.0;

            (first_time, last_time, end_offset)
        } else {
            (0, 0, 0)
        };

        Ok(Self {
            file: RefCell::new(file),
            item_size: item_size,
            items: items,
            first_time: first_time,
            last_time: last_time,
            end_offset: end_offset,
            _phantom: PhantomData,
        })
    }

    /// Finds the time and offset of the first record that occurs on or before the timestamp.
    /// If the timestamp is before the first record, it returns the time and offset of the first record.
    fn find_from(&self, timestamp: Timestamp) -> io::Result<(Timestamp, u64)> {
        // Scratch buffer into which we'll read new timestamps for parsing
        let mut read_buffer = vec![0u8; TIMESTAMP_SIZE as usize];

        let from_offset = if timestamp >= self.first_time {
            binary_search_for_timestamp::<T, File>(&mut *self.file.borrow_mut(), &mut read_buffer, Some(RetrievalDirection::Backward), timestamp, 0, self.end_offset)?
        } else {
            0
        };

        self.file.borrow_mut().seek(SeekFrom::Start(from_offset))?;
        let from_timestamp = cmp::max(read_timestamp::<File>(&mut *self.file.borrow_mut(), &mut read_buffer)?, timestamp);

        Ok((from_timestamp, from_offset))
    }

    /// Finds the offset of the first record that occurs before the timestamp.
    fn find_to(&self, timestamp: Timestamp) -> io::Result<u64> {
        // Scratch buffer into which we'll read new timestamps for parsing
        let mut read_buffer = vec![0u8; TIMESTAMP_SIZE as usize];

        let to_offset = binary_search_for_timestamp::<T, File>(&mut *self.file.borrow_mut(), &mut read_buffer, Some(RetrievalDirection::Backward), timestamp, 0, self.end_offset)?;

        self.file.borrow_mut().seek(SeekFrom::Start(to_offset))?;
        let to_timestamp = read_timestamp::<File>(&mut *self.file.borrow_mut(), &mut read_buffer)?;

        // find_to is exclusive.  If the bounding timestamp is found exactly, exclude that record from the result.
        Ok(if to_timestamp != timestamp {
            to_offset
        } else if to_offset > 0 {
            to_offset - self.item_size as u64
        } else {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "find_to timestamp was equal to the first record"));
        })
    }
}

impl<T> Storage for FileStorage<T> where T: Storable<FileStorage<T>> {
    fn store(&mut self, timestamp: Timestamp, data: Box<Data>) -> io::Result<()> {
        if self.items > 0 && timestamp <= self.last_time {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "Passed timestamp was equal to or before the last recorded timestamp"));
        }

        if let Some(&data) = data.downcast_ref::<T>() {
            self.file.borrow_mut().seek(SeekFrom::End(0))?;

            write_record(&mut self.file.borrow_mut(), timestamp, data)?;

            if self.items == 0 {
                self.first_time = timestamp;
            } else {
                self.end_offset += self.item_size as u64;
            }

            self.items += 1;
            self.last_time = timestamp;

            Ok(())
        } else {
            Err(io::Error::new(io::ErrorKind::InvalidInput, "FileStorage was passed the wrong kind of data"))
        }
    }

    fn retrieve(&self, timestamp: Timestamp, retrieval_direction: Option<RetrievalDirection>) -> io::Result<Retrieval> {
        let record_offset = {
            // Scratch buffer into which we'll read new timestamps for parsing
            let mut read_buffer = vec![0u8; TIMESTAMP_SIZE as usize];

            binary_search_for_timestamp::<T, File>(&mut *self.file.borrow_mut(), &mut read_buffer, retrieval_direction, timestamp, 0, self.end_offset)?
        };

        self.file.borrow_mut().seek(SeekFrom::Start(record_offset))?;

        Ok(Retrieval::new(Box::new(
            read_record::<T, File>(&mut self.file.borrow_mut(), &mut vec![0u8; self.item_size])?
        )))
    }

    fn retrieve_all(&self, retrieval_options: RetrievalOptions) -> io::Result<Retrieval> {
        // Reset the file to the beginning
        self.file.borrow_mut().seek(SeekFrom::Start(0))?;

        // Buffer the file to reduce the number of disk reads
        let file = &mut *self.file.borrow_mut();
        let mut file_buffer = BufReader::new(file);

        // Scratch buffer into which we'll read new records for parsing
        let mut read_buffer = vec![0u8; self.item_size];

        // Gather all buckets between the beginning and end of the file
        let values = gather_buckets::<T, BufReader<&mut File>>(
            &mut file_buffer,
            &mut read_buffer,
            retrieval_options,
            self.first_time,
            0,
            self.end_offset,
        )?;

        Ok(Retrieval::new(Box::new(values)))
    }

    fn retrieve_from(&self, timestamp: Timestamp, retrieval_options: RetrievalOptions) -> io::Result<Retrieval> {
        let (from_timestamp, from_offset) = self.find_from(timestamp)?;
        self.file.borrow_mut().seek(SeekFrom::Start(from_offset))?;

        // Buffer the file to reduce the number of disk reads
        let file = &mut *self.file.borrow_mut();
        let mut file_buffer = BufReader::new(file);

        // Scratch buffer into which we'll read new records for parsing
        let mut read_buffer = vec![0u8; self.item_size];

        // Gather all buckets between the beginning and end of the file
        let values = gather_buckets::<T, BufReader<&mut File>>(
            &mut file_buffer,
            &mut read_buffer,
            retrieval_options,
            from_timestamp,
            from_offset,
            self.end_offset,
        )?;

        Ok(Retrieval::new(Box::new(values)))
    }

    fn retrieve_to(&self, timestamp: Timestamp, retrieval_options: RetrievalOptions) -> io::Result<Retrieval> {
        let to_offset = match self.find_to(timestamp) {
            Ok(offset) => offset,
            Err(error) => return if error.kind() == io::ErrorKind::InvalidInput && format!("{}", error) == "find_to timestamp was equal to the first record" {
                Ok(Retrieval::new(Box::new(Vec::<(Timestamp, T)>::new())))
            } else {
                Err(error)
            },
        };

        self.file.borrow_mut().seek(SeekFrom::Start(0))?;

        // Buffer the file to reduce the number of disk reads
        let file = &mut *self.file.borrow_mut();
        let mut file_buffer = BufReader::new(file);

        // Scratch buffer into which we'll read new records for parsing
        let mut read_buffer = vec![0u8; self.item_size];

        // Gather all buckets between the beginning and end of the file
        let values = gather_buckets::<T, BufReader<&mut File>>(
            &mut file_buffer,
            &mut read_buffer,
            retrieval_options,
            self.first_time,
            0,
            to_offset,
        )?;

        Ok(Retrieval::new(Box::new(values)))
    }

    fn retrieve_range(&self, range: Range<Timestamp>, retrieval_options: RetrievalOptions) -> io::Result<Retrieval> {
        let (from_timestamp, from_offset) = self.find_from(range.start)?;

        let to_offset = match self.find_to(range.end) {
            Ok(offset) => offset,
            Err(error) => return if error.kind() == io::ErrorKind::InvalidInput && format!("{}", error) == "find_to timestamp was equal to the first record" {
                Ok(Retrieval::new(Box::new(Vec::<(Timestamp, T)>::new())))
            } else {
                Err(error)
            },
        };

        // Since the range is exclusive of the end, if the from and to offsets are the same record, there are no records to return.
        // Also no records to return if the from is after the to, obviously.
        if (to_offset as i64 - from_offset as i64) < self.item_size as i64 {
            return Ok(Retrieval::new(Box::new(Vec::<(Timestamp, T)>::new())));
        }

        self.file.borrow_mut().seek(SeekFrom::Start(from_offset))?;

        // Buffer the file to reduce the number of disk reads
        let file = &mut *self.file.borrow_mut();
        let mut file_buffer = BufReader::new(file);

        // Scratch buffer into which we'll read new records for parsing
        let mut read_buffer = vec![0u8; self.item_size];

        // Gather all buckets between the beginning and end of the file
        let values = gather_buckets::<T, BufReader<&mut File>>(
            &mut file_buffer,
            &mut read_buffer,
            retrieval_options,
            from_timestamp,
            from_offset,
            to_offset,
        )?;

        Ok(Retrieval::new(Box::new(values)))
    }

    fn len(&self) -> usize {
        self.items
    }
}

fn binary_search_for_timestamp<T: Storable<FileStorage<T>>, F: Read + Seek>(
    file: &mut F,
    buffer: &mut [u8],
    retrieval_direction: Option<RetrievalDirection>,
    timestamp: Timestamp,
    start_offset: u64,
    end_offset: u64,
) -> io::Result<u64> {
    if start_offset == end_offset {
        return Err(io::Error::new(io::ErrorKind::NotFound, "No items in search range"));
    }

    // Check the beginning of the range
    file.seek(SeekFrom::Start(start_offset))?;
    let start_timestamp = read_timestamp(file, buffer)?;

    if timestamp < start_timestamp {
        // If the timestamp is before the range, but we want to retrieve forward, return the beginning
        return if retrieval_direction != Some(RetrievalDirection::Forward) {
            Err(io::Error::new(io::ErrorKind::NotFound, "Search timestamp is before the search range"))
        } else {
            Ok(start_offset)
        };
    } else if timestamp == start_timestamp {
        return Ok(start_offset);
    }

    // Check the end of the range
    file.seek(SeekFrom::Start(end_offset))?;
    let end_timestamp = read_timestamp(file, buffer)?;

    if timestamp > end_timestamp {
        // If the timestamp is after the range, but we want to retrieve backward, return the end
        return if retrieval_direction != Some(RetrievalDirection::Backward) {
            Err(io::Error::new(io::ErrorKind::NotFound, "Search timestamp is after the search range"))
        } else {
            Ok(end_offset)
        };
    } else if timestamp == end_timestamp {
        return Ok(end_offset);
    }

    fn bisect_and_descend<T: Storable<FileStorage<T>>, F: Read + Seek>(
        file: &mut F,
        buffer: &mut [u8],
        retrieval_direction: Option<RetrievalDirection>,
        timestamp: Timestamp,
        start_offset: u64,
        end_offset: u64,
    ) -> io::Result<u64> {
        let range = end_offset - start_offset;
        let range_items = range / (PADDING + T::size() as u64);

        // If we've narrowed it down to just one item, the timestamp must occur between it and the next item.
        // Depending on the direction we want to retrieve, return it, the next item, or neither.
        if range_items == 1 {
            return match retrieval_direction {
                Some(RetrievalDirection::Forward) => Ok(end_offset),
                Some(RetrievalDirection::Backward) => Ok(start_offset),
                None => Err(io::Error::new(io::ErrorKind::NotFound, "Search timestamp was not found")),
            };
        }

        let center_offset = start_offset + range_items / 2 * (PADDING + T::size() as u64);

        // Check the center of the range (rounded down)
        file.seek(SeekFrom::Start(center_offset))?;
        let center_timestamp = read_timestamp(file, buffer)?;

        // Descend into whichever half contains the timestamp
        if timestamp < center_timestamp {
            bisect_and_descend::<T, F>(file, buffer, retrieval_direction, timestamp, start_offset, center_offset)
        } else if timestamp > center_timestamp {
            bisect_and_descend::<T, F>(file, buffer, retrieval_direction, timestamp, center_offset, end_offset)
        } else {
            Ok(center_offset)
        }
    }

    bisect_and_descend::<T, F>(file, buffer, retrieval_direction, timestamp, start_offset, end_offset)
}

fn gather_buckets<T: Storable<FileStorage<T>>, F: Read>(
    file: &mut F,
    buffer: &mut [u8],
    retrieval_options: RetrievalOptions,
    start_time: Timestamp,
    start_offset: u64,
    end_offset: u64,
) -> io::Result<Vec<(Timestamp, T)>> {
    let mut values: Vec<(Timestamp, T)> = Vec::new();

    let record_count = (end_offset - start_offset) / (PADDING + T::size() as u64) + 1;

    struct Bucket<T> {
        pub records: Vec<(Timestamp, T)>,
        pub start: Timestamp,
        pub end: Timestamp,
    }

    let first_record = read_record::<T, F>(file, buffer)?;

    // Start off the first bucket with the first record if it belongs there
    let mut bucket = Bucket {
        records: if first_record.0 == start_time {
            vec![first_record]
        } else {
            Vec::new()
        },
        start: start_time,
        end: start_time + retrieval_options.interval,
    };

    // Add the final bucket value onto the list, depending on the type of pooling
    fn conclude_bucket<T: Storable<FileStorage<T>>>(bucket: &Bucket<T>, values: &mut Vec<(Timestamp, T)>, last_record: (Timestamp, T), retrieval_options: RetrievalOptions) {
        if !bucket.records.is_empty() {
            values.push((bucket.start, match retrieval_options.pooling_method {
                PoolingMethod::End => bucket.records.last().unwrap().1,
                PoolingMethod::High => bucket.records.iter().max_by_key(|r| r.1).unwrap().1,
                PoolingMethod::Low => bucket.records.iter().min_by_key(|r| r.1).unwrap().1,
                PoolingMethod::Mean => T::mean(&bucket.records.iter().map(|r| r.1).collect::<Vec<T>>()),
                PoolingMethod::Start => if bucket.records.first().unwrap().0 == bucket.start || retrieval_options.gap_fill_method == Some(GapFillMethod::Default) {
                    bucket.records.first().unwrap().1
                } else {
                    last_record.1
                },
                PoolingMethod::Sum => T::sum(&bucket.records.iter().map(|r| r.1).collect::<Vec<T>>()),
            }));
        } else if let Some(gap_fill_method) = retrieval_options.gap_fill_method {
            let value = match gap_fill_method {
                GapFillMethod::Default => T::default(),
                GapFillMethod::Previous => last_record.1,
            };

            values.push((bucket.start, value));
        }
    }

    let mut last_record = first_record;

    // For the rest of the records
    for _ in 1..record_count {
        let record = read_record::<T, F>(file, buffer)?;

        // If the record we just read doesn't fit in this bucket,
        if record.0 >= bucket.end {
            // end the current bucket and start new ones until the record fits.
            conclude_bucket(&bucket, &mut values, last_record, retrieval_options);

            if !bucket.records.is_empty() {
                last_record = *bucket.records.last().unwrap();

                bucket.records.clear();
            }

            bucket.start = bucket.end;
            bucket.end += retrieval_options.interval;

            while bucket.end <= record.0 {
                conclude_bucket(&bucket, &mut values, last_record, retrieval_options);

                bucket.start = bucket.end;
                bucket.end += retrieval_options.interval;
            }
        }

        bucket.records.push(record);
    }

    conclude_bucket(&bucket, &mut values, last_record, retrieval_options);

    Ok(values)
}

fn read_record<T: Storable<FileStorage<T>>, F: Read>(file: &mut F, buffer: &mut [u8]) -> io::Result<(Timestamp, T)> {
    debug_assert_eq!(buffer.len(), PADDING as usize + T::size(), "read_record was passed a buffer of the wrong size");

    file.read_exact(buffer)?;

    if let Ok(str_buffer) = str::from_utf8(buffer) {
        // Parse the string into chunks
        let mut parts = str_buffer.split_whitespace();

        Ok((
            Timestamp::from_str(parts.next().unwrap()).unwrap(), // The first chunk is the timestamp
            T::from_bytes(parts.next().unwrap().as_bytes())?,    // The second chunk is the data
        ))
    } else {
        Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid data"))
    }
}

fn read_timestamp<F: Read>(file: &mut F, buffer: &mut [u8]) -> io::Result<Timestamp> {
    debug_assert_eq!(buffer.len(), TIMESTAMP_SIZE as usize, "read_timestamp was passed a buffer of the wrong size");

    file.read_exact(buffer)?;

    if let Ok(str_buffer) = str::from_utf8(buffer) {
        Ok(Timestamp::from_str(str_buffer).unwrap())
    } else {
        Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid data"))
    }
}

fn write_record<T: Storable<FileStorage<T>>>(file: &mut File, timestamp: Timestamp, data: T) -> io::Result<()> {
    // We don't want to incur a write per part of the data
    let mut buffer = BufWriter::with_capacity(PADDING as usize + T::size(), file);

    // Format the data and write it
    buffer.write(&format!("{:0size$} ", timestamp, size = TIMESTAMP_SIZE as usize).into_bytes())?;
    buffer.write(&data.into_bytes())?;
    buffer.write(b"\n")?;
    buffer.flush()
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

        fn from_bytes(buffer: &[u8]) -> io::Result<i32> {
            if let Ok(string) = String::from_utf8(buffer.to_vec()) {
                if let Ok(value) = i32::from_str(&string) {
                    return Ok(value);
                }
            }

            Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid data"))
        }

        fn mean(values: &[i32]) -> i32 {
            (values.iter().sum::<i32>() as f32 / values.len() as f32) as i32
        }

        fn sum(values: &[i32]) -> i32 {
            values.iter().sum()
        }
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
    fn test_file_storage_gap_fill_method() {
        let _setup_file = SetupFile::new("test_file_storage_gap_fill_method");

        let mut fs = FileStorage::<i32>::new("test_file_storage_gap_fill_method").unwrap();

        fs.store(10, Box::new(1)).unwrap();
        fs.store(14, Box::new(2)).unwrap();
        fs.store(15, Box::new(3)).unwrap();
        fs.store(20, Box::new(4)).unwrap();
        fs.store(26, Box::new(5)).unwrap();

        let retrieval_options = RetrievalOptions { interval: 3, pooling_method: PoolingMethod::Start, gap_fill_method: Some(GapFillMethod::Previous) };
        let retrieval = fs.retrieve_all(retrieval_options).unwrap();
        assert_eq!(retrieval.as_vec::<i32, FileStorage<i32>>(), Some(&vec![(10, 1), (13, 1), (16, 3), (19, 3), (22, 4), (25, 4)]));

        let retrieval_options = RetrievalOptions { interval: 3, pooling_method: PoolingMethod::Start, gap_fill_method: Some(GapFillMethod::Default) };
        let retrieval = fs.retrieve_all(retrieval_options).unwrap();
        assert_eq!(retrieval.as_vec::<i32, FileStorage<i32>>(), Some(&vec![(10, 1), (13, 2), (16, 0), (19, 4), (22, 0), (25, 5)]));

        let retrieval_options = RetrievalOptions { interval: 3, pooling_method: PoolingMethod::Start, gap_fill_method: None };
        let retrieval = fs.retrieve_all(retrieval_options).unwrap();
        assert_eq!(retrieval.as_vec::<i32, FileStorage<i32>>(), Some(&vec![(10, 1), (13, 1), (19, 3), (25, 4)]));
    }

    #[test]
    fn test_file_storage_pooling_method() {
        let _setup_file = SetupFile::new("test_file_storage_pooling_method");

        let mut fs = FileStorage::<i32>::new("test_file_storage_pooling_method").unwrap();

        fs.store(10, Box::new(1)).unwrap();
        fs.store(14, Box::new(2)).unwrap();
        fs.store(15, Box::new(3)).unwrap();
        fs.store(19, Box::new(5)).unwrap();
        fs.store(20, Box::new(4)).unwrap();
        fs.store(21, Box::new(6)).unwrap();
        fs.store(26, Box::new(7)).unwrap();

        let retrieval_options = RetrievalOptions { interval: 3, pooling_method: PoolingMethod::End, gap_fill_method: Some(GapFillMethod::Previous) };
        let retrieval = fs.retrieve_from(12, retrieval_options).unwrap();
        assert_eq!(retrieval.as_vec::<i32, FileStorage<i32>>(), Some(&vec![(12, 2), (15, 3), (18, 4), (21, 6), (24, 7)]));

        let retrieval_options = RetrievalOptions { interval: 3, pooling_method: PoolingMethod::High, gap_fill_method: Some(GapFillMethod::Previous) };
        let retrieval = fs.retrieve_from(12, retrieval_options).unwrap();
        assert_eq!(retrieval.as_vec::<i32, FileStorage<i32>>(), Some(&vec![(12, 2), (15, 3), (18, 5), (21, 6), (24, 7)]));

        let retrieval_options = RetrievalOptions { interval: 3, pooling_method: PoolingMethod::Low, gap_fill_method: Some(GapFillMethod::Previous) };
        let retrieval = fs.retrieve_from(12, retrieval_options).unwrap();
        assert_eq!(retrieval.as_vec::<i32, FileStorage<i32>>(), Some(&vec![(12, 2), (15, 3), (18, 4), (21, 6), (24, 7)]));

        let retrieval_options = RetrievalOptions { interval: 3, pooling_method: PoolingMethod::Mean, gap_fill_method: Some(GapFillMethod::Previous) };
        let retrieval = fs.retrieve_from(12, retrieval_options).unwrap();
        assert_eq!(retrieval.as_vec::<i32, FileStorage<i32>>(), Some(&vec![(12, 2), (15, 3), (18, 4), (21, 6), (24, 7)]));

        let retrieval_options = RetrievalOptions { interval: 3, pooling_method: PoolingMethod::Start, gap_fill_method: Some(GapFillMethod::Previous) };
        let retrieval = fs.retrieve_from(12, retrieval_options).unwrap();
        assert_eq!(retrieval.as_vec::<i32, FileStorage<i32>>(), Some(&vec![(12, 1), (15, 3), (18, 3), (21, 6), (24, 6)]));

        let retrieval_options = RetrievalOptions { interval: 3, pooling_method: PoolingMethod::Sum, gap_fill_method: Some(GapFillMethod::Previous) };
        let retrieval = fs.retrieve_from(12, retrieval_options).unwrap();
        assert_eq!(retrieval.as_vec::<i32, FileStorage<i32>>(), Some(&vec![(12, 2), (15, 3), (18, 9), (21, 6), (24, 7)]));
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

    #[test]
    fn test_file_storage_retrieval_direction() {
        let _setup_file = SetupFile::new("test_file_storage_retrieval_direction");

        let mut fs = FileStorage::<i32>::new("test_file_storage_retrieval_direction").unwrap();

        fs.store(10, Box::new(1)).unwrap();
        fs.store(15, Box::new(1)).unwrap();
        fs.store(20, Box::new(2)).unwrap();
        fs.store(30, Box::new(3)).unwrap();

        let retrieval = fs.retrieve(22, Some(RetrievalDirection::Forward)).unwrap();
        assert_eq!(retrieval.as_single::<i32, FileStorage<i32>>(), Some(&(30, 3)));

        let retrieval = fs.retrieve(17, Some(RetrievalDirection::Backward)).unwrap();
        assert_eq!(retrieval.as_single::<i32, FileStorage<i32>>(), Some(&(15, 1)));
    }

    #[test]
    fn test_file_storage_retrieve() {
        let _setup_file = SetupFile::new("test_file_storage_retrieve");

        let mut fs = FileStorage::<i32>::new("test_file_storage_retrieve").unwrap();

        fs.store(1, Box::new(1)).unwrap();
        fs.store(2, Box::new(2)).unwrap();
        fs.store(3, Box::new(3)).unwrap();

        let retrieval = fs.retrieve(2, None).unwrap();
        assert_eq!(retrieval.as_single::<i32, FileStorage<i32>>(), Some(&(2, 2)));
    }

    #[test]
    fn test_file_storage_retrieve_all() {
        let _setup_file = SetupFile::new("test_file_storage_retrieve_all");

        let mut fs = FileStorage::<i32>::new("test_file_storage_retrieve_all").unwrap();

        fs.store(1, Box::new(1)).unwrap();
        fs.store(2, Box::new(2)).unwrap();
        fs.store(3, Box::new(3)).unwrap();

        let retrieval_options = RetrievalOptions { interval: 1, ..RetrievalOptions::default() };
        let retrieval = fs.retrieve_all(retrieval_options).unwrap();
        assert_eq!(retrieval.as_vec::<i32, FileStorage<i32>>(), Some(&vec![(1, 1), (2, 2), (3, 3)]));
    }

    #[test]
    fn test_file_storage_retrieve_from() {
        let _setup_file = SetupFile::new("test_file_storage_retrieve_from");

        let mut fs = FileStorage::<i32>::new("test_file_storage_retrieve_from").unwrap();

        fs.store(10, Box::new(1)).unwrap();
        fs.store(20, Box::new(2)).unwrap();
        fs.store(30, Box::new(3)).unwrap();
        fs.store(40, Box::new(4)).unwrap();

        let retrieval_options = RetrievalOptions { interval: 10, ..RetrievalOptions::default() };
        let retrieval = fs.retrieve_from(17, retrieval_options).unwrap();
        assert_eq!(retrieval.as_vec::<i32, FileStorage<i32>>(), Some(&vec![(17, 2), (27, 3), (37, 4)]));

        let retrieval = fs.retrieve_from(7, retrieval_options).unwrap();
        assert_eq!(retrieval.as_vec::<i32, FileStorage<i32>>(), Some(&vec![(10, 1), (20, 2), (30, 3), (40, 4)]));
    }

    #[test]
    fn test_file_storage_retrieve_range() {
        let _setup_file = SetupFile::new("test_file_storage_retrieve_range");

        let mut fs = FileStorage::<i32>::new("test_file_storage_retrieve_range").unwrap();

        fs.store(10, Box::new(1)).unwrap();
        fs.store(20, Box::new(2)).unwrap();
        fs.store(30, Box::new(3)).unwrap();
        fs.store(40, Box::new(4)).unwrap();

        let retrieval_options = RetrievalOptions { interval: 10, ..RetrievalOptions::default() };
        let retrieval = fs.retrieve_range(10..33, retrieval_options).unwrap();
        assert_eq!(retrieval.as_vec::<i32, FileStorage<i32>>(), Some(&vec![(10, 1), (20, 2), (30, 3)]));

        let retrieval = fs.retrieve_range(31..33, retrieval_options).unwrap();
        assert_eq!(retrieval.as_vec::<i32, FileStorage<i32>>(), Some(&vec![]));

        let retrieval = fs.retrieve_range(7..43, retrieval_options).unwrap();
        assert_eq!(retrieval.as_vec::<i32, FileStorage<i32>>(), Some(&vec![(10, 1), (20, 2), (30, 3), (40, 4)]));
    }

    #[test]
    fn test_file_storage_retrieve_range_is_exclusive() {
        let _setup_file = SetupFile::new("test_file_storage_retrieve_range_is_exclusive");

        let mut fs = FileStorage::<i32>::new("test_file_storage_retrieve_range_is_exclusive").unwrap();

        fs.store(10, Box::new(1)).unwrap();
        fs.store(20, Box::new(2)).unwrap();
        fs.store(30, Box::new(3)).unwrap();
        fs.store(40, Box::new(4)).unwrap();

        let retrieval_options = RetrievalOptions { interval: 10, ..RetrievalOptions::default() };
        let retrieval = fs.retrieve_range(10..30, retrieval_options).unwrap();
        assert_eq!(retrieval.as_vec::<i32, FileStorage<i32>>(), Some(&vec![(10, 1), (20, 2)]));

        let retrieval = fs.retrieve_range(30..30, retrieval_options).unwrap();
        assert_eq!(retrieval.as_vec::<i32, FileStorage<i32>>(), Some(&vec![]));
    }

    #[test]
    fn test_file_storage_retrieve_to() {
        let _setup_file = SetupFile::new("test_file_storage_retrieve_to");

        let mut fs = FileStorage::<i32>::new("test_file_storage_retrieve_to").unwrap();

        fs.store(10, Box::new(1)).unwrap();
        fs.store(20, Box::new(2)).unwrap();
        fs.store(30, Box::new(3)).unwrap();
        fs.store(40, Box::new(4)).unwrap();

        let retrieval_options = RetrievalOptions { interval: 10, ..RetrievalOptions::default() };
        let retrieval = fs.retrieve_to(33, retrieval_options).unwrap();
        assert_eq!(retrieval.as_vec::<i32, FileStorage<i32>>(), Some(&vec![(10, 1), (20, 2), (30, 3)]));
    }

    #[test]
    fn test_file_storage_retrieve_to_is_exclusive() {
        let _setup_file = SetupFile::new("test_file_storage_retrieve_to_is_exclusive");

        let mut fs = FileStorage::<i32>::new("test_file_storage_retrieve_to_is_exclusive").unwrap();

        fs.store(10, Box::new(1)).unwrap();
        fs.store(20, Box::new(2)).unwrap();
        fs.store(30, Box::new(3)).unwrap();
        fs.store(40, Box::new(4)).unwrap();

        let retrieval_options = RetrievalOptions { interval: 10, ..RetrievalOptions::default() };
        let retrieval = fs.retrieve_to(30, retrieval_options).unwrap();
        assert_eq!(retrieval.as_vec::<i32, FileStorage<i32>>(), Some(&vec![(10, 1), (20, 2)]));

        let retrieval = fs.retrieve_to(10, retrieval_options).unwrap();
        assert_eq!(retrieval.as_vec::<i32, FileStorage<i32>>(), Some(&vec![]));
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
}
