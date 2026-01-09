//// Database connection management.

import ducky/error.{type Error}
import ducky/internal/ffi
import gleam/dynamic
import gleam/result
import gleam/string

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
      |> result.map_error(decode_error)
    }
  }
}

/// Closes a database connection.
pub fn close(connection: Connection) -> Nil {
  ffi.close(connection.native)
  Nil
}

/// Returns the database path for a connection.
pub fn path(connection: Connection) -> String {
  connection.path
}

/// Decodes an error from the NIF layer.
fn decode_error(err: dynamic.Dynamic) -> Error {
  // NIF returns errors as strings for now
  let err_string = string.inspect(err)

  case { string.contains(err_string, "connection_failed") } {
    True -> error.ConnectionFailed(err_string)
    False ->
      case string.contains(err_string, "query_syntax_error") {
        True -> error.QuerySyntaxError(err_string)
        False -> error.DatabaseError(err_string)
      }
  }
}
