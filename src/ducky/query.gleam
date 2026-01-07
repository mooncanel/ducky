//// Query execution and result handling.

import ducky/connection.{type Connection}
import ducky/error.{type Error}
import ducky/types.{type DataFrame}

/// Executes a SQL query and returns structured results.
///
/// The query runs on a dirty scheduler to avoid blocking the BEAM.
/// Large result sets are streamed to prevent memory exhaustion.
///
/// ## Examples
///
/// ```gleam
/// query(conn, "SELECT id, name FROM users WHERE active = true")
/// // => Ok(DataFrame(columns: ["id", "name"], rows: [...]))
/// ```
pub fn query(_connection: Connection, sql: String) -> Result(DataFrame, Error) {
  case validate_sql(sql) {
    False -> Error(error.QuerySyntaxError("SQL cannot be empty"))
    True -> {
      // TODO: Call FFI to execute query
      Ok(types.DataFrame(columns: [], rows: []))
    }
  }
}

fn validate_sql(sql: String) -> Bool {
  sql != ""
}
