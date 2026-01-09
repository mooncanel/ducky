//// Foreign Function Interface to the Rust NIF layer.
////
//// This module provides low-level bindings to the native DuckDB implementation.
//// These functions should not be used directly; use the public API instead.

import gleam/dynamic.{type Dynamic}

/// Opaque reference to a native connection resource.
pub type NativeConnection

/// Opens a connection to a DuckDB database.
///
/// Returns the raw NIF result which must be decoded.
@external(erlang, "ducky_nif", "connect")
pub fn connect(path: String) -> Result(NativeConnection, Dynamic)

/// Closes a database connection.
///
/// Returns nil atom on success.
@external(erlang, "ducky_nif", "close")
pub fn close(conn: NativeConnection) -> Dynamic

/// Health check function to verify NIF is loaded.
@external(erlang, "ducky_nif", "test")
pub fn health_check() -> String
