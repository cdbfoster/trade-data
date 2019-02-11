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

use key_value_store::{KeyValueStore, Retrieval};

pub type Timestamp = u64;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum RetrievalDirection {
    Forward,
    Backward,
}

pub trait TimeSeries: KeyValueStore {
    fn retrieve_nearest(&self, timestamp: Timestamp, retrieval_direction: Option<RetrievalDirection>) -> io::Result<Retrieval>;
    fn retrieve_all(&self) -> io::Result<Retrieval>;
    fn retrieve_from(&self, timestamp: Timestamp) -> io::Result<Retrieval>;
    fn retrieve_to(&self, timestamp: Timestamp) -> io::Result<Retrieval>;
    fn retrieve_range(&self, range: Range<Timestamp>) -> io::Result<Retrieval>;

    fn as_key_value_store(&self) -> &dyn KeyValueStore;
    fn as_mut_key_value_store(&mut self) -> &mut dyn KeyValueStore;
}

mod storage;
