//// Internal error decoding utilities.

import ducky/error.{type Error}
import gleam/dynamic
import gleam/string

/// Decodes an error from the NIF layer.
///
/// The NIF returns errors as tuples like {:error, {:connection_failed, "msg"}},
/// but for now we parse the string representation.
/// TODO: Use proper dynamic decoders for structured error atoms.
pub fn decode_nif_error(err: dynamic.Dynamic) -> Error {
  let err_string = string.inspect(err)

  case string.contains(err_string, "connection_failed") {
    True -> error.ConnectionFailed(err_string)
    False ->
      case string.contains(err_string, "query_syntax_error") {
        True -> error.QuerySyntaxError(err_string)
        False ->
          case string.contains(err_string, "database_error") {
            True -> error.DatabaseError(err_string)
            False -> error.DatabaseError(err_string)
          }
      }
  }
}
