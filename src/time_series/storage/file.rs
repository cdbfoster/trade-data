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
use std::str::FromStr;

use key_value_store::Storable;
use storage::FileStorage;
use time_series::Timestamp;

const SIGNIFICANT_DIGITS: usize = 13;

impl<V> Storable<FileStorage<Timestamp, V>> for Timestamp where V: Storable<FileStorage<Timestamp, V>> {
    fn size() -> usize {
        SIGNIFICANT_DIGITS
    }

    fn into_bytes(self) -> Vec<u8> {
        format!("{:0size$}", self, size = SIGNIFICANT_DIGITS).into_bytes()
    }

    fn from_bytes(buffer: &[u8]) -> io::Result<Self> {
        if let Ok(string) = String::from_utf8(buffer.to_vec()) {
            if let Ok(value) = Timestamp::from_str(&string) {
                return Ok(value);
            }
        }

        Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid data"))
    }
}
