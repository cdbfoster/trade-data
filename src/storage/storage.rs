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

use {Data, Interval, Timestamp};

/// The value to return during gaps in the record
#[derive(Clone, Copy, PartialEq)]
pub enum GapFillMethod {
    /// Buckets with no records will receive the data type's default value
    Default,
    /// Buckets with no records will receive the value of the last bucket
    Previous,
}

/// The value to return for each bucket
#[derive(Clone, Copy, PartialEq)]
pub enum PoolingMethod {
    End,
    High,
    Low,
    Mean,
    /// When gap_fill_method is Some(Default), the bucket value is the first record in the bucket.
    /// Otherwise, the bucket value is the most recent record upon bucket start.
    Start,
    Sum,
}

pub struct Retrieval {
    data: Box<Data>,
}

impl Retrieval {
    pub fn new(data: Box<Data>) -> Self {
        Self {
            data: data,
        }
    }

    pub fn as_single<T: Storable<U>, U: Storage>(&self) -> Option<&(Timestamp, T)> {
        self.data.downcast_ref::<(Timestamp, T)>()
    }

    pub fn as_vec<T: Storable<U>, U: Storage>(&self) -> Option<&Vec<(Timestamp, T)>> {
        self.data.downcast_ref::<Vec<(Timestamp, T)>>()
    }
}

#[derive(Clone, Copy, PartialEq)]
pub enum RetrievalDirection {
    Forward,
    Backward,
}

#[derive(Clone, Copy)]
pub struct RetrievalOptions {
    /// The size of each bucket
    pub interval: Interval,
    /// Which value to return for each bucket
    pub pooling_method: PoolingMethod,
    /// Whether and how to fill gaps
    pub gap_fill_method: Option<GapFillMethod>,
}

impl Default for RetrievalOptions {
    fn default() -> Self {
        Self {
            interval: 10_000,
            pooling_method: PoolingMethod::End,
            gap_fill_method: Some(GapFillMethod::Previous),
        }
    }
}

pub trait Storage {
    fn store(&mut self, timestamp: Timestamp, data: Box<Data>) -> io::Result<()>;

    fn retrieve(&self, timestamp: Timestamp, retrieval_direction: Option<RetrievalDirection>) -> io::Result<Retrieval>;
    fn retrieve_all(&self, retrieval_options: RetrievalOptions) -> io::Result<Retrieval>;
    fn retrieve_from(&self, timestamp: Timestamp, retrieval_options: RetrievalOptions) -> io::Result<Retrieval>;
    fn retrieve_to(&self, timestamp: Timestamp, retrieval_options: RetrievalOptions) -> io::Result<Retrieval>;
    fn retrieve_range(&self, range: Range<Timestamp>, retrieval_options: RetrievalOptions) -> io::Result<Retrieval>;

    fn len(&self) -> usize;
}

pub trait Storable<T: Storage>: 'static + Copy + Default + Ord {
    fn size() -> usize;
    fn into_bytes(self) -> Vec<u8>;
    fn from_bytes(buffer: &[u8]) -> io::Result<Self> where Self: Sized;
    fn mean(values: &[Self]) -> Self where Self: Sized;
    fn sum(value: &[Self]) -> Self where Self: Sized;
}
