//// Type mappings between DuckDB and Gleam.

import gleam/option.{type Option}

/// A value from a DuckDB result set.
pub type Value {
  Null
  Boolean(Bool)
  TinyInt(Int)
  SmallInt(Int)
  Integer(Int)
  BigInt(Int)
  Float(Float)
  Double(Float)
  Text(String)
  Blob(BitArray)
  Timestamp(Int)
  Date(Int)
  Time(Int)
  Interval(Int)
  List(List(Value))
}

/// A single row from a query result.
pub type Row {
  Row(values: List(Value))
}

/// A complete query result with column metadata.
pub type DataFrame {
  DataFrame(columns: List(String), rows: List(Row))
}

/// Get a value from a row by column index.
pub fn get(row: Row, index: Int) -> Option(Value) {
  case row {
    Row(values) -> list_at(values, index)
  }
}

fn list_at(list: List(a), index: Int) -> Option(a) {
  case list, index {
    [], _ -> option.None
    [first, ..], 0 -> option.Some(first)
    [_, ..rest], n if n > 0 -> list_at(rest, n - 1)
    _, _ -> option.None
  }
}
