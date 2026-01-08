//! DuckDB Native Implemented Function for Erlang/Elixir/Gleam.
//!
//! Provides native bindings to DuckDB through Rustler.

use rustler::{Env, Term};

mod atoms {
    rustler::atoms! {
        ok,
        error,
        // Error atoms
        connection_failed,
        query_syntax_error,
        database_error,
    }
}

/// Health check NIF to verify the library loads correctly.
#[rustler::nif]
fn test() -> String {
    "DuckDB NIF loaded successfully!".to_string()
}

/// Initialize the NIF module and register all functions.
fn load(_env: Env, _: Term) -> bool {
    true
}

rustler::init!("ducky_nif", load = load);
