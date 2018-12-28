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

use std::fs;

pub struct SetupFile {
    filename: &'static str,
}

impl SetupFile {
    pub fn new(filename: &'static str) -> Self {
        fs::remove_file(filename).ok();
        Self {
            filename: filename,
        }
    }
}

impl Drop for SetupFile {
    fn drop(&mut self) {
        fs::remove_file(self.filename).ok();
    }
}
