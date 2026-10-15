//! Shared SQLite connection and atomic schema initialization.
//!
//! Components own their tables. This crate never migrates an existing schema.

use std::{
    fmt,
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};

pub use rusqlite::{Connection, OptionalExtension, Transaction, params};

/// Database failures, including an explicit incompatible component format.
#[derive(Debug)]
pub enum Error {
    Sqlite(rusqlite::Error),
    Poisoned,
    SchemaVersion {
        component: String,
        expected: u32,
        actual: i64,
    },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sqlite(error) => error.fmt(f),
            Self::Poisoned => f.write_str("SQLite connection lock is poisoned"),
            Self::SchemaVersion {
                component,
                expected,
                actual,
            } => write!(
                f,
                "unsupported {component} schema version {actual}; expected {expected}"
            ),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Sqlite(error) => Some(error),
            _ => None,
        }
    }
}

impl From<rusqlite::Error> for Error {
    fn from(error: rusqlite::Error) -> Self {
        Self::Sqlite(error)
    }
}

/// Cloneable connection handle. A transaction holds the connection until commit/rollback.
#[derive(Clone)]
pub struct Database(Arc<Mutex<Connection>>);

impl Database {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, Error> {
        Self::from_connection(Connection::open(path)?)
    }

    pub fn memory() -> Result<Self, Error> {
        Self::from_connection(Connection::open_in_memory()?)
    }

    pub fn from_connection(connection: Connection) -> Result<Self, Error> {
        connection.busy_timeout(Duration::from_secs(5))?;
        connection.execute_batch("PRAGMA foreign_keys = ON; PRAGMA journal_mode = WAL;")?;
        Ok(Self(Arc::new(Mutex::new(connection))))
    }

    /// Runs synchronous database work. Do not re-enter this handle from the callback.
    pub fn read<T, E>(&self, operation: impl FnOnce(&Connection) -> Result<T, E>) -> Result<T, E>
    where
        E: From<Error>,
    {
        let connection = self.0.lock().map_err(|_| E::from(Error::Poisoned))?;
        operation(&connection)
    }

    /// Reads one consistent SQLite snapshot across multiple queries, including when
    /// another connection commits to the same WAL database between those queries.
    pub fn snapshot<T, E>(
        &self,
        operation: impl FnOnce(&Transaction<'_>) -> Result<T, E>,
    ) -> Result<T, E>
    where
        E: From<Error>,
    {
        let mut connection = self.0.lock().map_err(|_| E::from(Error::Poisoned))?;
        let transaction = connection
            .transaction_with_behavior(rusqlite::TransactionBehavior::Deferred)
            .map_err(|error| E::from(Error::from(error)))?;
        let result = operation(&transaction)?;
        transaction
            .commit()
            .map_err(|error| E::from(Error::from(error)))?;
        Ok(result)
    }

    /// Acquires the write reservation before executing the callback.
    /// All callback work must use the supplied transaction; an error rolls it back.
    pub fn transaction<T, E>(
        &self,
        operation: impl FnOnce(&Transaction<'_>) -> Result<T, E>,
    ) -> Result<T, E>
    where
        E: From<Error>,
    {
        let mut connection = self.0.lock().map_err(|_| E::from(Error::Poisoned))?;
        let transaction = connection
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(|error| E::from(Error::from(error)))?;
        let result = operation(&transaction)?;
        transaction
            .commit()
            .map_err(|error| E::from(Error::from(error)))?;
        Ok(result)
    }

    /// Creates a component's current schema atomically, or checks the existing version.
    /// No old-version upgrade or downgrade is attempted.
    pub fn initialize(&self, component: &str, version: u32, schema: &str) -> Result<(), Error> {
        self.transaction(|tx| {
            tx.execute_batch(
                "CREATE TABLE IF NOT EXISTS pirua_schema (
                component TEXT PRIMARY KEY NOT NULL, version INTEGER NOT NULL
            ) STRICT;",
            )?;
            let actual: Option<i64> = tx
                .query_row(
                    "SELECT version FROM pirua_schema WHERE component = ?1",
                    [component],
                    |row| row.get(0),
                )
                .optional()?;
            match actual {
                Some(actual) if actual != i64::from(version) => Err(Error::SchemaVersion {
                    component: component.to_owned(),
                    expected: version,
                    actual,
                }),
                Some(_) => Ok(()),
                None => {
                    tx.execute_batch(schema)?;
                    tx.execute(
                        "INSERT INTO pirua_schema (component, version) VALUES (?1, ?2)",
                        params![component, version],
                    )?;
                    Ok(())
                }
            }
        })
    }
}
