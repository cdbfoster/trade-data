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

use std::io;
use std::ops::Range;

use key_value_store::Retrieval;
use time_series::{TimeSeries, Timestamp};

pub type Interval = Timestamp;

/// The value to return during gaps in the record
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum GapFillMethod {
    /// Buckets with no records will receive the data type's default value
    Default,
    /// Buckets with no records will receive the value of the last bucket
    Previous,
}

/// The value to return for each bucket
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum PoolingMethod {
    End,
    High,
    Low,
    Mean,
    /// When gap_fill is Some(Default), the bucket value is the first record in the bucket.
    /// Otherwise, the bucket value is the most recent record upon bucket start.
    Start,
    Sum,
}

#[derive(Clone, Copy, Debug)]
pub struct PoolingOptions {
    /// The size of each bucket
    pub interval: Interval,
    /// Which value to return for each bucket
    pub pooling: PoolingMethod,
    /// Whether and how to fill gaps
    pub gap_fill: Option<GapFillMethod>,
}

impl Default for PoolingOptions {
    fn default() -> Self {
        Self {
            interval: 0,
            pooling: PoolingMethod::End,
            gap_fill: None,
        }
    }
}

pub trait PooledTimeSeries: TimeSeries {
    fn pool_all(&self, pooling_options: PoolingOptions) -> io::Result<Retrieval>;
    fn pool_from(&self, timestamp: Timestamp, pooling_options: PoolingOptions) -> io::Result<Retrieval>;
    fn pool_to(&self, timestamp: Timestamp, pooling_options: PoolingOptions) -> io::Result<Retrieval>;
    fn pool_range(&self, range: Range<Timestamp>, pooling_options: PoolingOptions) -> io::Result<Retrieval>;

    fn as_time_series(&self) -> &dyn TimeSeries;
    fn as_mut_time_series(&mut self) -> &mut dyn TimeSeries;
}

pub trait Poolable: 'static + Copy + Default + Ord + Sized {
    fn mean(values: &[Self]) -> Self;
    fn sum(values: &[Self]) -> Self;
}

#[cfg(test)]
mod tests {
    use super::*;

    impl Poolable for i32 {
        fn mean(values: &[Self]) -> Self {
            (values.iter().sum::<Self>() as f32 / values.len() as f32) as Self
        }

        fn sum(values: &[Self]) -> Self {
            values.iter().sum()
        }
    }
}
