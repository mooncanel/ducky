import ducky
import gleeunit
import gleeunit/should

pub fn main() {
  gleeunit.main()
}

pub fn connect_memory_database_test() {
  ducky.connect(":memory:")
  |> should.be_ok
}

pub fn connect_empty_path_test() {
  ducky.connect("")
  |> should.be_error
}

pub fn query_empty_sql_test() {
  let assert Ok(conn) = ducky.connect(":memory:")
  ducky.query(conn, "")
  |> should.be_error
}
