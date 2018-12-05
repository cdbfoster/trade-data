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
use value::Usd;
use value::usd::{MAJOR_DIGITS, MINOR_DIGITS};

impl Storable<FileStorage<Usd>> for Usd {
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

            return Ok(Usd::new(whole * 10i64.pow(MINOR_DIGITS as u32) + fractional));
        }

        Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid data"))
    }

    fn mean(values: &[Self]) -> Self {
        Usd::new(values.iter().map(|v| v.value).sum::<i64>() / values.len() as i64)
    }

    fn sum(values: &[Self]) -> Self {
        Usd::new(values.iter().map(|v| v.value).sum())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_usd_storable_file_storage_into_bytes() {
        let value = Usd::new(12345);

        assert_eq!(value.into_bytes(), format!("   123.45").into_bytes());
    }

    #[test]
    fn test_usd_storable_file_storage_from_bytes() {
        let value = Usd::from_bytes(&Usd::new(12345).into_bytes());

        assert!(value.is_ok());
        assert!(value.unwrap() == Usd::new(12345));
    }

    #[test]
    fn test_usd_storable_file_storage_mean() {
        let values = vec![Usd::new(12345), Usd::new(479), Usd::new(9467)];

        assert_eq!(Usd::mean(&values), Usd::new(7430));
    }

    #[test]
    fn test_usd_storable_file_storage_sum() {
        let values = vec![Usd::new(12345), Usd::new(479), Usd::new(9467)];

        assert_eq!(Usd::sum(&values), Usd::new(22291));
    }
}
