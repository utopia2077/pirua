#![cfg(feature = "search-sqlite")]
#[path = "composition/histories.rs"]
mod histories;
#[path = "composition/legacy.rs"]
mod legacy;
#[cfg(feature = "sqlite")]
#[path = "composition/sqlite.rs"]
mod sqlite;
#[path = "composition/support.rs"]
mod support;
