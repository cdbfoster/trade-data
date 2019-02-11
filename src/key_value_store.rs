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

use std::any::Any;
use std::io;

pub type Data = dyn Any;

pub struct Retrieval {
    data: Box<Data>,
}

impl Retrieval {
    pub fn new(data: Box<Data>) -> Self {
        Self {
            data: data,
        }
    }

    pub fn as_single<K: 'static, V: 'static>(&self) -> Option<&(K, V)> {
        self.data.downcast_ref::<(K, V)>()
    }

    pub fn as_vec<K: 'static, V: 'static>(&self) -> Option<&Vec<(K, V)>> {
        self.data.downcast_ref::<Vec<(K, V)>>()
    }

    pub fn into_single<K: 'static, V: 'static>(self) -> (K, V) {
        if let Ok(cast) = self.data.downcast::<(K, V)>() {
            *cast
        } else {
            panic!("into_single called on a Retrieval of the wrong type");
        }
    }

    pub fn into_vec<K: 'static, V: 'static>(self) -> Vec<(K, V)> {
        if let Ok(cast) = self.data.downcast::<Vec<(K, V)>>() {
            *cast
        } else {
            panic!("into_vec called on a Retrieval of the wrong type");
        }
    }
}

pub trait KeyValueStore: Send {
    fn len(&self) -> usize;

    fn store(&mut self, key: Box<Data>, value: Box<Data>) -> io::Result<()>;
    //fn retrieve(&self, key: Box<Data>) -> io::Result<Retrieval>;
}

pub trait Storable<T: KeyValueStore>: 'static + Copy + Default + Sized + Send {
    fn size() -> usize;
    fn into_bytes(self) -> Vec<u8>;
    fn from_bytes(buffer: &[u8]) -> io::Result<Self>;
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::str::FromStr;

    use storage::FileStorage;
    use time_series::Timestamp;

    impl Storable<FileStorage<Timestamp, i32>> for i32 {
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
    }
}
