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

use std::str::FromStr;
use std::io;

use storage::{FileStorage, Storable};
use value::Btc;
use value::btc::{MAJOR_DIGITS, MINOR_DIGITS};

impl Storable<FileStorage<Btc>> for Btc {
    fn size() -> usize {
        MAJOR_DIGITS + 1 + MINOR_DIGITS
    }

    fn into_bytes(self) -> Vec<u8> {
        format!("{:>size$}", self, size = Self::size()).into_bytes()
    }

    fn from_bytes(buffer: &[u8]) -> io::Result<Self> {
        let len = buffer.len();
        if len >= MINOR_DIGITS + 2 {
            let whole = if let Ok(string) = String::from_utf8(buffer[..len - MINOR_DIGITS - 1].to_vec()) {
                if let Ok(value) = i64::from_str(string.trim()) {
                    value
                } else {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid data"));
                }
            } else {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid data"));
            };

            let fractional = if let Ok(string) = String::from_utf8(buffer[len - MINOR_DIGITS..].to_vec()) {
                if let Ok(value) = i64::from_str(&string) {
                    value
                } else {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid data"));
                }
            } else {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid data"));
            };

            return Ok(Btc::new(whole * 10i64.pow(MINOR_DIGITS as u32) + fractional));
        }

        Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid data"))
    }

    fn mean(values: &[Self]) -> Self {
        Btc::new(values.iter().map(|v| v.value).sum::<i64>() / values.len() as i64)
    }

    fn sum(values: &[Self]) -> Self {
        Btc::new(values.iter().map(|v| v.value).sum())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_btc_storable_file_storage_into_bytes() {
        let value = Btc::new(1234567890);

        assert_eq!(value.into_bytes(), format!("    12.34567890").into_bytes());
    }

    #[test]
    fn test_btc_storable_file_storage_from_bytes() {
        let value = Btc::from_bytes(&Btc::new(1234567890).into_bytes());

        assert!(value.is_ok());
        assert!(value.unwrap() == Btc::new(1234567890));
    }

    #[test]
    fn test_btc_storable_file_storage_mean() {
        let values = vec![Btc::new(1234567890), Btc::new(4793323), Btc::new(498432214)];

        assert_eq!(Btc::mean(&values), Btc::new(579264475));
    }

    #[test]
    fn test_btc_storable_file_storage_sum() {
        let values = vec![Btc::new(1234567890), Btc::new(4793323), Btc::new(498432214)];

        assert_eq!(Btc::sum(&values), Btc::new(1737793427));
    }
}
