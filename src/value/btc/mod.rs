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

use std::fmt;

use value::Value;

const MAJOR_DIGITS: usize = 6;
const MINOR_DIGITS: usize = 8;

#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
pub struct Btc {
    value: i64,
}

impl Btc {
    pub fn new(value: i64) -> Self {
        Self {
            value: value,
        }
    }
}

impl Value for Btc {
    fn whole(&self) -> i64 {
        self.value / 10i64.pow(MINOR_DIGITS as u32)
    }

    fn fractional(&self) -> i64 {
        self.value % 10i64.pow(MINOR_DIGITS as u32)
    }
}

impl fmt::Display for Btc {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.pad(&format!("{}.{:0minor_digits$}", self.whole(), self.fractional(), minor_digits = MINOR_DIGITS))
    }
}

mod storable;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_btc_whole() {
        let value = Btc::new(1234567890);

        assert_eq!(value.whole(), 12i64);
    }

    #[test]
    fn test_btc_fractional() {
        let value = Btc::new(1234567890);

        assert_eq!(value.fractional(), 34567890i64);
    }

    #[test]
    fn test_btc_display() {
        let value = Btc::new(1234567890);

        assert_eq!(&format!("{}", value), "12.34567890");
    }
}
