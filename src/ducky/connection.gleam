//// Database connection management.

import ducky/error.{type Error}

/// An opaque connection to a DuckDB database.
pub opaque type Connection {
  Connection(handle: Int, path: String)
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
      // TODO: Call FFI to open connection
      Ok(Connection(handle: 0, path: path))
    }
  }
}

/// Closes a database connection.
pub fn close(connection: Connection) -> Result(Nil, Error) {
  case connection {
    Connection(..) -> {
      // TODO: Call FFI to close connection
      Ok(Nil)
    }
  }
}

/// Returns the database path for a connection.
pub fn path(connection: Connection) -> String {
  connection.path
}
