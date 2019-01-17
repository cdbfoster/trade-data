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
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::ops::{Add, Range};

use key_value_store::{Data, KeyValueStore, Retrieval, Storable};
use pooled_time_series::{GapFillMethod, Poolable, PooledTimeSeries, PoolingMethod, PoolingOptions};
use storage::file::{FileStorage, read_record};
use time_series::{RetrievalDirection, TimeSeries, Timestamp};

impl<V> PooledTimeSeries for FileStorage<Timestamp, V> where V: Storable<FileStorage<Timestamp, V>> + Poolable {
    fn pool_all(&self, pooling_options: PoolingOptions) -> io::Result<Retrieval> {
        // Reset the file to the beginning
        self.file.borrow_mut().seek(SeekFrom::Start(0))?;

        // Buffer the file to reduce the number of disk reads
        let file = &mut *self.file.borrow_mut();
        let mut file_buffer = BufReader::new(file);

        // Scratch buffer into which we'll read new records for parsing
        let mut read_buffer = vec![0u8; self.item_size];

        // Gather all buckets between the beginning and end of the file
        let values = gather_buckets::<V, BufReader<&mut File>>(
            &mut file_buffer,
            &mut read_buffer,
            pooling_options,
            self.first_key,
            0,
            self.end_offset,
        )?;

        Ok(Retrieval::new(Box::new(values)))
    }

    fn pool_from(&self, timestamp: Timestamp, pooling_options: PoolingOptions) -> io::Result<Retrieval> {
        let (from_timestamp, from_offset) = self.find_from(timestamp)?;
        self.file.borrow_mut().seek(SeekFrom::Start(from_offset))?;

        // Buffer the file to reduce the number of disk reads
        let file = &mut *self.file.borrow_mut();
        let mut file_buffer = BufReader::new(file);

        // Scratch buffer into which we'll read new records for parsing
        let mut read_buffer = vec![0u8; self.item_size];

        // Gather all buckets between the beginning and end of the file
        let values = gather_buckets::<V, BufReader<&mut File>>(
            &mut file_buffer,
            &mut read_buffer,
            pooling_options,
            from_timestamp,
            from_offset,
            self.end_offset,
        )?;

        Ok(Retrieval::new(Box::new(values)))
    }

    fn pool_to(&self, timestamp: Timestamp, pooling_options: PoolingOptions) -> io::Result<Retrieval> {
        let to_offset = match self.find_to(timestamp) {
            Ok(offset) => offset,
            Err(error) => return if error.kind() == io::ErrorKind::InvalidInput && format!("{}", error) == "find_to search key was equal to the first record" {
                Ok(Retrieval::new(Box::new(Vec::<(Timestamp, V)>::new())))
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
        let values = gather_buckets::<V, BufReader<&mut File>>(
            &mut file_buffer,
            &mut read_buffer,
            pooling_options,
            self.first_key,
            0,
            to_offset,
        )?;

        Ok(Retrieval::new(Box::new(values)))
    }

    fn pool_range(&self, range: Range<Timestamp>, pooling_options: PoolingOptions) -> io::Result<Retrieval> {
        let (from_timestamp, from_offset) = self.find_from(range.start)?;

        let to_offset = match self.find_to(range.end) {
            Ok(offset) => offset,
            Err(error) => return if error.kind() == io::ErrorKind::InvalidInput && format!("{}", error) == "find_to search key was equal to the first record" {
                Ok(Retrieval::new(Box::new(Vec::<(Timestamp, V)>::new())))
            } else {
                Err(error)
            },
        };

        // Since the range is exclusive of the end, if the from and to offsets are the same record, there are no records to return.
        // Also no records to return if the from is after the to, obviously.
        if (to_offset as i64 - from_offset as i64) < self.item_size as i64 {
            return Ok(Retrieval::new(Box::new(Vec::<(Timestamp, V)>::new())));
        }

        self.file.borrow_mut().seek(SeekFrom::Start(from_offset))?;

        // Buffer the file to reduce the number of disk reads
        let file = &mut *self.file.borrow_mut();
        let mut file_buffer = BufReader::new(file);

        // Scratch buffer into which we'll read new records for parsing
        let mut read_buffer = vec![0u8; self.item_size];

        // Gather all buckets between the beginning and end of the file
        let values = gather_buckets::<V, BufReader<&mut File>>(
            &mut file_buffer,
            &mut read_buffer,
            pooling_options,
            from_timestamp,
            from_offset,
            to_offset,
        )?;

        Ok(Retrieval::new(Box::new(values)))
    }
}

fn gather_buckets<V, F>(
    file: &mut F,
    buffer: &mut [u8],
    pooling_options: PoolingOptions,
    start_time: Timestamp,
    start_offset: u64,
    end_offset: u64,
) -> io::Result<Vec<(Timestamp, V)>> where V: Storable<FileStorage<Timestamp, V>> + Poolable, F: Read {
    let mut values: Vec<(Timestamp, V)> = Vec::new();

    let record_count = (end_offset - start_offset) / (<Timestamp as Storable<FileStorage<Timestamp, V>>>::size() + 1 + V::size() + 1) as u64 + 1;

    struct Bucket<V> {
        pub records: Vec<(Timestamp, V)>,
        pub start: Timestamp,
        pub end: Timestamp,
    }

    let first_record = read_record::<Timestamp, V, F>(file, buffer)?;

    // Start off the first bucket with the first record if it belongs there
    let mut bucket = Bucket {
        records: if first_record.0 == start_time {
            vec![first_record]
        } else {
            Vec::new()
        },
        start: start_time,
        end: start_time + pooling_options.interval,
    };

    // Add the final bucket value onto the list, depending on the type of pooling
    fn conclude_bucket<V>(
        bucket: &Bucket<V>,
        values: &mut Vec<(Timestamp, V)>,
        last_record: (Timestamp, V),
        pooling_options: PoolingOptions
    ) where V: Poolable {
        if !bucket.records.is_empty() {
            values.push((bucket.start, match pooling_options.pooling {
                PoolingMethod::End => bucket.records.last().unwrap().1,
                PoolingMethod::High => bucket.records.iter().max_by_key(|r| r.1).unwrap().1,
                PoolingMethod::Low => bucket.records.iter().min_by_key(|r| r.1).unwrap().1,
                PoolingMethod::Mean => V::mean(&bucket.records.iter().map(|r| r.1).collect::<Vec<V>>()),
                PoolingMethod::Start => if bucket.records.first().unwrap().0 == bucket.start || pooling_options.gap_fill == Some(GapFillMethod::Default) {
                    bucket.records.first().unwrap().1
                } else {
                    last_record.1
                },
                PoolingMethod::Sum => V::sum(&bucket.records.iter().map(|r| r.1).collect::<Vec<V>>()),
            }));
        } else if let Some(gap_fill_method) = pooling_options.gap_fill {
            let value = match gap_fill_method {
                GapFillMethod::Default => V::default(),
                GapFillMethod::Previous => last_record.1,
            };

            values.push((bucket.start, value));
        }
    }

    let mut last_record = first_record;

    // For the rest of the records
    for _ in 1..record_count {
        let record = read_record::<Timestamp, V, F>(file, buffer)?;

        // If the record we just read doesn't fit in this bucket,
        if record.0 >= bucket.end {
            // end the current bucket and start new ones until the record fits.
            conclude_bucket(&bucket, &mut values, last_record, pooling_options);

            if !bucket.records.is_empty() {
                last_record = *bucket.records.last().unwrap();

                bucket.records.clear();
            }

            bucket.start = bucket.end;
            bucket.end += pooling_options.interval;

            while bucket.end <= record.0 {
                conclude_bucket(&bucket, &mut values, last_record, pooling_options);

                bucket.start = bucket.end;
                bucket.end += pooling_options.interval;
            }
        }

        bucket.records.push(record);
    }

    conclude_bucket(&bucket, &mut values, last_record, pooling_options);

    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::fs::File;
    use std::mem;

    use time_series::Timestamp;
    use util::SetupFile;

    #[test]
    fn test_gap_fill_method() {
        let _setup_file = SetupFile::new("test_gap_fill_method");

        let mut fs = FileStorage::<Timestamp, i32>::new("test_gap_fill_method").unwrap();

        fs.store(Box::new(10 as Timestamp), Box::new(1 as i32)).unwrap();
        fs.store(Box::new(14 as Timestamp), Box::new(2 as i32)).unwrap();
        fs.store(Box::new(15 as Timestamp), Box::new(3 as i32)).unwrap();
        fs.store(Box::new(20 as Timestamp), Box::new(4 as i32)).unwrap();
        fs.store(Box::new(26 as Timestamp), Box::new(5 as i32)).unwrap();

        let pooling_options = PoolingOptions { interval: 3, pooling: PoolingMethod::Start, gap_fill: Some(GapFillMethod::Previous) };
        let retrieval = fs.pool_all(pooling_options).unwrap();
        assert_eq!(retrieval.as_vec::<Timestamp, i32>(), Some(&vec![(10, 1), (13, 1), (16, 3), (19, 3), (22, 4), (25, 4)]));

        let pooling_options = PoolingOptions { interval: 3, pooling: PoolingMethod::Start, gap_fill: Some(GapFillMethod::Default) };
        let retrieval = fs.pool_all(pooling_options).unwrap();
        assert_eq!(retrieval.as_vec::<Timestamp, i32>(), Some(&vec![(10, 1), (13, 2), (16, 0), (19, 4), (22, 0), (25, 5)]));

        let pooling_options = PoolingOptions { interval: 3, pooling: PoolingMethod::Start, gap_fill: None };
        let retrieval = fs.pool_all(pooling_options).unwrap();
        assert_eq!(retrieval.as_vec::<Timestamp, i32>(), Some(&vec![(10, 1), (13, 1), (19, 3), (25, 4)]));
    }

    #[test]
    fn test_pool_all() {
        let _setup_file = SetupFile::new("test_pool_all");

        let mut fs = FileStorage::<Timestamp, i32>::new("test_pool_all").unwrap();

        fs.store(Box::new(1 as Timestamp), Box::new(1 as i32)).unwrap();
        fs.store(Box::new(2 as Timestamp), Box::new(2 as i32)).unwrap();
        fs.store(Box::new(3 as Timestamp), Box::new(3 as i32)).unwrap();

        let pooling_options = PoolingOptions { interval: 1, ..PoolingOptions::default() };
        let retrieval = fs.pool_all(pooling_options).unwrap();
        assert_eq!(retrieval.as_vec::<Timestamp, i32>(), Some(&vec![(1, 1), (2, 2), (3, 3)]));
    }

    #[test]
    fn test_pool_from() {
        let _setup_file = SetupFile::new("test_pool_from");

        let mut fs = FileStorage::<Timestamp, i32>::new("test_pool_from").unwrap();

        fs.store(Box::new(10 as Timestamp), Box::new(1 as i32)).unwrap();
        fs.store(Box::new(20 as Timestamp), Box::new(2 as i32)).unwrap();
        fs.store(Box::new(30 as Timestamp), Box::new(3 as i32)).unwrap();
        fs.store(Box::new(40 as Timestamp), Box::new(4 as i32)).unwrap();

        let pooling_options = PoolingOptions { interval: 10, ..PoolingOptions::default() };
        let retrieval = fs.pool_from(17, pooling_options).unwrap();
        assert_eq!(retrieval.as_vec::<Timestamp, i32>(), Some(&vec![(17, 2), (27, 3), (37, 4)]));

        let retrieval = fs.pool_from(7, pooling_options).unwrap();
        assert_eq!(retrieval.as_vec::<Timestamp, i32>(), Some(&vec![(10, 1), (20, 2), (30, 3), (40, 4)]));
    }

    #[test]
    fn test_pooling_method() {
        let _setup_file = SetupFile::new("test_pooling_method");

        let mut fs = FileStorage::<Timestamp, i32>::new("test_pooling_method").unwrap();

        fs.store(Box::new(10 as Timestamp), Box::new(1 as i32)).unwrap();
        fs.store(Box::new(14 as Timestamp), Box::new(2 as i32)).unwrap();
        fs.store(Box::new(15 as Timestamp), Box::new(3 as i32)).unwrap();
        fs.store(Box::new(19 as Timestamp), Box::new(5 as i32)).unwrap();
        fs.store(Box::new(20 as Timestamp), Box::new(4 as i32)).unwrap();
        fs.store(Box::new(21 as Timestamp), Box::new(6 as i32)).unwrap();
        fs.store(Box::new(26 as Timestamp), Box::new(7 as i32)).unwrap();

        let pooling_options = PoolingOptions { interval: 3, pooling: PoolingMethod::End, gap_fill: Some(GapFillMethod::Previous) };
        let retrieval = fs.pool_from(12, pooling_options).unwrap();
        assert_eq!(retrieval.as_vec::<Timestamp, i32>(), Some(&vec![(12, 2), (15, 3), (18, 4), (21, 6), (24, 7)]));

        let pooling_options = PoolingOptions { interval: 3, pooling: PoolingMethod::High, gap_fill: Some(GapFillMethod::Previous) };
        let retrieval = fs.pool_from(12, pooling_options).unwrap();
        assert_eq!(retrieval.as_vec::<Timestamp, i32>(), Some(&vec![(12, 2), (15, 3), (18, 5), (21, 6), (24, 7)]));

        let pooling_options = PoolingOptions { interval: 3, pooling: PoolingMethod::Low, gap_fill: Some(GapFillMethod::Previous) };
        let retrieval = fs.pool_from(12, pooling_options).unwrap();
        assert_eq!(retrieval.as_vec::<Timestamp, i32>(), Some(&vec![(12, 2), (15, 3), (18, 4), (21, 6), (24, 7)]));

        let pooling_options = PoolingOptions { interval: 3, pooling: PoolingMethod::Mean, gap_fill: Some(GapFillMethod::Previous) };
        let retrieval = fs.pool_from(12, pooling_options).unwrap();
        assert_eq!(retrieval.as_vec::<Timestamp, i32>(), Some(&vec![(12, 2), (15, 3), (18, 4), (21, 6), (24, 7)]));

        let pooling_options = PoolingOptions { interval: 3, pooling: PoolingMethod::Start, gap_fill: Some(GapFillMethod::Previous) };
        let retrieval = fs.pool_from(12, pooling_options).unwrap();
        assert_eq!(retrieval.as_vec::<Timestamp, i32>(), Some(&vec![(12, 1), (15, 3), (18, 3), (21, 6), (24, 6)]));

        let pooling_options = PoolingOptions { interval: 3, pooling: PoolingMethod::Sum, gap_fill: Some(GapFillMethod::Previous) };
        let retrieval = fs.pool_from(12, pooling_options).unwrap();
        assert_eq!(retrieval.as_vec::<Timestamp, i32>(), Some(&vec![(12, 2), (15, 3), (18, 9), (21, 6), (24, 7)]));
    }

    #[test]
    fn test_pool_range() {
        let _setup_file = SetupFile::new("test_pool_range");

        let mut fs = FileStorage::<Timestamp, i32>::new("test_pool_range").unwrap();

        fs.store(Box::new(10 as Timestamp), Box::new(1 as i32)).unwrap();
        fs.store(Box::new(20 as Timestamp), Box::new(2 as i32)).unwrap();
        fs.store(Box::new(30 as Timestamp), Box::new(3 as i32)).unwrap();
        fs.store(Box::new(40 as Timestamp), Box::new(4 as i32)).unwrap();

        let pooling_options = PoolingOptions { interval: 10, ..PoolingOptions::default() };
        let retrieval = fs.pool_range(10..33, pooling_options).unwrap();
        assert_eq!(retrieval.as_vec::<Timestamp, i32>(), Some(&vec![(10, 1), (20, 2), (30, 3)]));

        let retrieval = fs.pool_range(31..33, pooling_options).unwrap();
        assert_eq!(retrieval.as_vec::<Timestamp, i32>(), Some(&vec![]));

        let retrieval = fs.pool_range(7..43, pooling_options).unwrap();
        assert_eq!(retrieval.as_vec::<Timestamp, i32>(), Some(&vec![(10, 1), (20, 2), (30, 3), (40, 4)]));
    }

    #[test]
    fn test_retrieve_range_is_exclusive() {
        let _setup_file = SetupFile::new("test_pool_range_is_exclusive");

        let mut fs = FileStorage::<Timestamp, i32>::new("test_pool_range_is_exclusive").unwrap();

        fs.store(Box::new(10 as Timestamp), Box::new(1 as i32)).unwrap();
        fs.store(Box::new(20 as Timestamp), Box::new(2 as i32)).unwrap();
        fs.store(Box::new(30 as Timestamp), Box::new(3 as i32)).unwrap();
        fs.store(Box::new(40 as Timestamp), Box::new(4 as i32)).unwrap();

        let pooling_options = PoolingOptions { interval: 10, ..PoolingOptions::default() };
        let retrieval = fs.pool_range(10..30, pooling_options).unwrap();
        assert_eq!(retrieval.as_vec::<Timestamp, i32>(), Some(&vec![(10, 1), (20, 2)]));

        let retrieval = fs.pool_range(30..30, pooling_options).unwrap();
        assert_eq!(retrieval.as_vec::<Timestamp, i32>(), Some(&vec![]));
    }

    #[test]
    fn test_pool_to() {
        let _setup_file = SetupFile::new("test_pool_to");

        let mut fs = FileStorage::<Timestamp, i32>::new("test_pool_to").unwrap();

        fs.store(Box::new(10 as Timestamp), Box::new(1 as i32)).unwrap();
        fs.store(Box::new(20 as Timestamp), Box::new(2 as i32)).unwrap();
        fs.store(Box::new(30 as Timestamp), Box::new(3 as i32)).unwrap();
        fs.store(Box::new(40 as Timestamp), Box::new(4 as i32)).unwrap();

        let pooling_options = PoolingOptions { interval: 10, ..PoolingOptions::default() };
        let retrieval = fs.pool_to(33, pooling_options).unwrap();
        assert_eq!(retrieval.as_vec::<Timestamp, i32>(), Some(&vec![(10, 1), (20, 2), (30, 3)]));
    }

    #[test]
    fn test_pool_to_is_exclusive() {
        let _setup_file = SetupFile::new("test_pool_to_is_exclusive");

        let mut fs = FileStorage::<Timestamp, i32>::new("test_pool_to_is_exclusive").unwrap();

        fs.store(Box::new(10 as Timestamp), Box::new(1 as i32)).unwrap();
        fs.store(Box::new(20 as Timestamp), Box::new(2 as i32)).unwrap();
        fs.store(Box::new(30 as Timestamp), Box::new(3 as i32)).unwrap();
        fs.store(Box::new(40 as Timestamp), Box::new(4 as i32)).unwrap();

        let pooling_options = PoolingOptions { interval: 10, ..PoolingOptions::default() };
        let retrieval = fs.pool_to(30, pooling_options).unwrap();
        assert_eq!(retrieval.as_vec::<Timestamp, i32>(), Some(&vec![(10, 1), (20, 2)]));

        let retrieval = fs.pool_to(10, pooling_options).unwrap();
        assert_eq!(retrieval.as_vec::<Timestamp, i32>(), Some(&vec![]));
    }
}
