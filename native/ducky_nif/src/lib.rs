//! DuckDB Native Implemented Function for Erlang/Elixir/Gleam.
//!
//! Provides native bindings to DuckDB through Rustler.

use duckdb::Connection as DuckDBConnection;
use rustler::{Encoder, Env, ResourceArc, Term};
use std::sync::Mutex;

mod atoms {
    rustler::atoms! {
        ok,
        error,
        // Error atoms
        connection_failed,
        query_syntax_error,
        database_error,
        nil,
    }
}

/// Error type that can be returned to Erlang.
#[derive(Debug)]
pub enum DuckyError {
    ConnectionFailed(String),
    QuerySyntaxError(String),
    DatabaseError(String),
}

impl Encoder for DuckyError {
    fn encode<'a>(&self, env: Env<'a>) -> Term<'a> {
        let reason = match self {
            DuckyError::ConnectionFailed(msg) => {
                (atoms::connection_failed(), msg.as_str()).encode(env)
            }
            DuckyError::QuerySyntaxError(msg) => {
                (atoms::query_syntax_error(), msg.as_str()).encode(env)
            }
            DuckyError::DatabaseError(msg) => (atoms::database_error(), msg.as_str()).encode(env),
        };
        (atoms::error(), reason).encode(env)
    }
}

impl From<duckdb::Error> for DuckyError {
    fn from(err: duckdb::Error) -> Self {
        DuckyError::DatabaseError(err.to_string())
    }
}

/// Resource wrapper for DuckDB connection with thread-safe access.
pub struct ConnectionResource {
    #[allow(dead_code)]
    connection: Mutex<DuckDBConnection>,
}

impl ConnectionResource {
    fn new(connection: DuckDBConnection) -> Self {
        Self {
            connection: Mutex::new(connection),
        }
    }
}

/// Opens a connection to a DuckDB database.
///
/// ## Arguments
/// - `path`: Database file path or `:memory:` for in-memory database
///
/// ## Returns
/// - `Ok(ResourceArc<ConnectionResource>)` on success
/// - `Err(DuckyError)` on failure
#[rustler::nif]
fn connect(path: String) -> Result<ResourceArc<ConnectionResource>, DuckyError> {
    let connection = if path == ":memory:" {
        DuckDBConnection::open_in_memory()
    } else {
        DuckDBConnection::open(&path)
    }
    .map_err(|e| DuckyError::ConnectionFailed(e.to_string()))?;

    Ok(ResourceArc::new(ConnectionResource::new(connection)))
}

/// Closes a database connection.
///
/// ## Arguments
/// - `conn`: Connection resource to close
///
/// ## Returns
/// - `Ok(())` on success
/// - `Err(DuckyError)` if close fails
#[rustler::nif]
fn close(conn: ResourceArc<ConnectionResource>) -> Result<rustler::Atom, DuckyError> {
    // Attempt to lock the connection
    let _guard = conn
        .connection
        .lock()
        .map_err(|e| DuckyError::DatabaseError(format!("Connection mutex poisoned: {}", e)))?;

    // DuckDB connections are closed via Drop when ResourceArc is dropped
    // We just validate the connection is accessible before allowing the drop
    drop(_guard);
    Ok(atoms::nil())
}

/// Health check NIF to verify the library loads correctly.
#[rustler::nif]
fn test() -> String {
    "DuckDB NIF loaded successfully!".to_string()
}

/// Initialize the NIF module and register all functions.
fn on_load(env: Env, _: Term) -> bool {
    #[allow(non_local_definitions)]
    {
        let _ = rustler::resource!(ConnectionResource, env);
    }
    true
}

rustler::init!("ducky_nif", load = on_load);
