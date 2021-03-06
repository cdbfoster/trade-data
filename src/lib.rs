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

pub use key_value_store::{KeyValueStore, Retrieval};
pub use pooled_time_series::{Interval, GapFillMethod, Poolable, PooledTimeSeries, PoolingOptions};
pub use time_series::{TimeSeries, Timestamp};

pub mod storage;
//pub mod value;

mod key_value_store;
mod pooled_time_series;
mod time_series;
mod util;
