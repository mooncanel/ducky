//// Database connection management.

import ducky/error.{type Error}
import ducky/internal/error_decoder
import ducky/internal/ffi
import gleam/result

/// An opaque connection to a DuckDB database.
pub opaque type Connection {
  Connection(native: ffi.NativeConnection, path: String)
}

/// Opens a connection to a DuckDB database.
///
/// ## Examples
///
/// ```gleam
/// connect(":memory:")
/// // => Ok(Connection(...))
///
/// connect("data.duckdb")
/// // => Ok(Connection(...))
/// ```
pub fn connect(path: String) -> Result(Connection, Error) {
  case path {
    "" -> Error(error.ConnectionFailed("path cannot be empty"))
    _ -> {
      ffi.connect(path)
      |> result.map(fn(native) { Connection(native: native, path: path) })
      |> result.map_error(error_decoder.decode_nif_error)
    }
  }
}

/// Closes a database connection.
///
/// ## Examples
///
/// ```gleam
/// let assert Ok(conn) = connect(":memory:")
/// let assert Ok(_) = close(conn)
/// ```
///
/// ## Errors
///
/// Returns an error if the connection cannot be closed.
pub fn close(connection: Connection) -> Result(Nil, Error) {
  ffi.close(connection.native)
  |> result.map(fn(_) { Nil })
  |> result.map_error(error_decoder.decode_nif_error)
}

/// Returns the database path for a connection.
pub fn path(connection: Connection) -> String {
  connection.path
}

/// Returns the native connection handle for FFI calls.
///
/// This is an internal function for use by other modules in the ducky package.
pub fn native(connection: Connection) -> ffi.NativeConnection {
  connection.native
}
