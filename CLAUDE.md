# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Extension Overview

This is a DuckDB Rust extension for IP address operations, specifically implementing IP prefix less/more specific functionality. The extension downloads and parses RIS-Whois data to build an IP trie for lookups.

## Build Commands

Configure the build environment (required first):
```bash
make configure
```

Build debug version:
```bash
make debug
```

Build release version:
```bash
make release
```

## Testing

Run tests with debug build:
```bash
make test_debug
```

Run tests with release build:
```bash
make test_release
```

Test files are in SQLLogicTest format at `test/sql/ip_more_less_specific.test`.

## Code Quality

Format Rust code:
```bash
cargo fmt
```

Run Clippy linter:
```bash
cargo clippy
```

## Extension Loading

Start DuckDB with unsigned extensions enabled:
```bash
duckdb -unsigned
```

Load the extension:
```sql
LOAD './build/debug/extension/ip_more_less_specific/ip_more_less_specific.duckdb_extension';
```

## Architecture

The extension is structured as follows:

- `src/lib.rs` - Main extension entry point with DuckDB C API integration
- `src/lib/ris_whois.rs` - Module for downloading and parsing RIS-Whois data into IP trie
- `src/wasm_lib.rs` - WASM wrapper (forwards to lib.rs)

The core functionality uses:
- `FirstLessSpecific` struct implementing `VArrowScalar` for DuckDB Arrow integration
- `FirstLessSpecificState` with `LazyLock<IpnetTrie<()>>` for thread-safe lazy initialization
- `build_ipnet_trie()` downloads RIS-Whois data and builds the IP prefix trie on first access

## Version Management

The extension targets DuckDB v1.3.2. To test with different versions:

```bash
make clean_all
DUCKDB_TEST_VERSION=v1.3.2 make configure
make debug && make test_debug
```

## Dependencies

Key dependencies include:
- `duckdb` v1.3.2 with Arrow and VScalar features
- `ipnet-trie` v0.3.0 for IP prefix operations
- `polars` v0.50.0 for CSV processing
- `reqwest` v0.12.23 for HTTP downloads

## Known Issues

- Extensions may fail to load on Windows with Python 3.11 - use Python 3.12 if encountered
