# Build a sqlite clone with C and port it to Rust
This is a sandbox on converting an existing C codebase to Rust.

## Background
Originally, it was a pure copy of https://cstack.github.io/db_tutorial/. Eventually it used cmake to build the C
executable with bits and pieces migrated over to a static rust lib with C bindings. Over time, all C was removed and it
moved to 100% Rust (albeit very unsafe). It is slowly moving over to idiomatic and safe rust.

## Running tests
Since some tests depend on knowing where the executable is, you need to run tests by pointing the `CSTACK_PATH`
environment variable to the executable.
```shell
CSTACK_PATH=target/debug/cstack cargo test
```