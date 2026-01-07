# ducky

Native DuckDB driver for Gleam with support for Erlang and JavaScript runtimes.

[![Package Version](https://img.shields.io/hexpm/v/ducky)](https://hex.pm/packages/ducky)
[![Hex Docs](https://img.shields.io/badge/hex-docs-ffaff3)](https://hexdocs.pm/ducky/)

## Installation

```sh
gleam add ducky
```

## Usage

```gleam
import ducky

pub fn main() {
  let assert Ok(conn) = ducky.connect(":memory:")
  let assert Ok(result) = ducky.query(conn, "SELECT 42 as answer")

  // Process results...
}
```

## Development Status

This package is in early development. Core features are being implemented.

## Development

```sh
gleam test  # Run the tests
gleam build # Build the project
```

## License

Apache-2.0
