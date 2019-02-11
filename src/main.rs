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

#![feature(proc_macro_hygiene, decl_macro)]

#[macro_use] extern crate lazy_static;
#[macro_use] extern crate rocket;
extern crate rocket_contrib;
#[macro_use] extern crate serde_derive;

extern crate trade_data;

use rocket::Rocket;
use rocket_contrib::json::Json;

mod market {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use trade_data::{KeyValueStore, PooledTimeSeries, TimeSeries, Timestamp};
    use trade_data::storage::FileStorage;

    lazy_static! {
        pub static ref MARKETS: HashMap<String, Market> = {
            let mut markets = HashMap::new();

            markets.insert("gemini".to_string(), Market({
                let mut symbols = HashMap::new();

                symbols.insert("btcusd".to_string(), Symbol({
                    let mut channels = HashMap::new();

                    channels.insert("trades".to_string(), Mutex::new(Channel::TimeSeries(Box::new(FileStorage::<Timestamp, Timestamp>::new("gemini_btcusd_trades").unwrap()))));
                    channels
                }));
                symbols
            }));
            markets
        };
    }

    pub struct Market(HashMap<String, Symbol>);

    pub struct Symbol(HashMap<String, Mutex<Channel>>);

    pub enum Channel {
        KeyValueStore(Box<dyn KeyValueStore>),
        TimeSeries(Box<dyn TimeSeries>),
        PooledTimeSeries(Box<dyn PooledTimeSeries>),
    }

    impl Channel {
        fn as_key_value_store(&self) -> Option<&dyn KeyValueStore> {
            match self {
                Channel::KeyValueStore(x) => Some(&**x),
                Channel::TimeSeries(x) => Some(x.as_key_value_store()),
                Channel::PooledTimeSeries(x) => Some(x.as_key_value_store()),
            }
        }

        fn as_time_series(&self) -> Option<&dyn TimeSeries> {
            match self {
                Channel::KeyValueStore(_) => None,
                Channel::TimeSeries(x) => Some(&**x),
                Channel::PooledTimeSeries(x) => Some(x.as_time_series()),
            }
        }

        fn as_pooled_time_series(&self) -> Option<&dyn PooledTimeSeries> {
            match self {
                Channel::KeyValueStore(_) => None,
                Channel::TimeSeries(_) => None,
                Channel::PooledTimeSeries(x) => Some(&**x),
            }
        }

        fn as_mut_key_value_store(&mut self) -> Option<&mut dyn KeyValueStore> {
            match self {
                Channel::KeyValueStore(x) => Some(&mut **x),
                Channel::TimeSeries(x) => Some(x.as_mut_key_value_store()),
                Channel::PooledTimeSeries(x) => Some(x.as_mut_key_value_store()),
            }
        }

        fn as_mut_time_series(&mut self) -> Option<&mut dyn TimeSeries> {
            match self {
                Channel::KeyValueStore(_) => None,
                Channel::TimeSeries(x) => Some(&mut **x),
                Channel::PooledTimeSeries(x) => Some(x.as_mut_time_series()),
            }
        }

        fn as_mut_pooled_time_series(&mut self) -> Option<&mut dyn PooledTimeSeries> {
            match self {
                Channel::KeyValueStore(_) => None,
                Channel::TimeSeries(_) => None,
                Channel::PooledTimeSeries(x) => Some(&mut **x),
            }
        }
    }
}

#[get("/")]
fn index() -> &'static str {
    "Hello world!"
}

#[derive(Serialize)]
struct DataThing {
    value: String,
}

#[get("/<market>/<symbol>/<channel>")]
fn get_data(market: String, symbol: String, channel: String) -> Json<DataThing> {
    Json(DataThing { value: format!("You asked for the {} market, and the {} symbol, and the {} channel.", market, symbol, channel) })
}

fn create_http_server() -> Rocket {
    rocket::ignite()
        .mount("/", routes![index])
        .mount("/", routes![get_data])
}

fn main() {
    create_http_server().launch();
}

#[cfg(test)]
mod tests {
    use super::*;

    use rocket::local::Client;
    use rocket::http::Status;

    #[test]
    fn test_client_hello_world() {
        let client = Client::new(create_http_server()).expect("create server");
        let request = client.get("/");
        let mut response = request.dispatch();

        assert_eq!(response.status(), Status::Ok);
        assert_eq!(response.body_string(), Some("Hello world!".into()));
    }
}
