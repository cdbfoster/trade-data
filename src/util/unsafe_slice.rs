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

use std::slice;

pub struct UnsafeSlice<T> {
    ptr: *const T,
    len: usize,
}

impl<T> UnsafeSlice<T> {
    pub fn new(ptr: *const T, len: usize) -> Self {
        Self {
            ptr: ptr,
            len: len,
        }
    }

    pub unsafe fn as_slice(&self) -> &[T] {
        slice::from_raw_parts(self.ptr, self.len)
    }
}
