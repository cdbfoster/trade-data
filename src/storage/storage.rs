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

use {Data, Timestamp};

pub trait Storage {
    fn store(&mut self, timestamp: Timestamp, data: Box<Data>) -> io::Result<()>;

    fn retrieve(&self, timestamp: Timestamp) -> io::Result<Box<Data>>;
    fn retrieve_all(&self) -> io::Result<Box<Data>>;
    fn retrieve_from(&self, timestamp: Timestamp) -> io::Result<Box<Data>>;
    fn retrieve_to(&self, timestamp: Timestamp) -> io::Result<Box<Data>>;
    fn retrieve_range(&self, range: Range<Timestamp>) -> io::Result<Box<Data>>;

    fn len(&self) -> usize;
}

pub trait Storable<T: Storage> {
    fn size() -> usize;
    fn into_bytes(self) -> Vec<u8>;
    fn from_bytes(buffer: &[u8]) -> io::Result<Self> where Self: Sized;
}
