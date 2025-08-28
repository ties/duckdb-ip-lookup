extern crate duckdb;
extern crate duckdb_loadable_macros;
extern crate libduckdb_sys;

use duckdb::ffi;

use env_logger::Env;

use duckdb::{
    arrow::{self, array::Array, datatypes::DataType},
    vscalar::VArrowScalar,
    Connection, Result,
};
use duckdb_loadable_macros::duckdb_entrypoint_c_api;
use std::sync::Arc;
use std::{env, error::Error};

#[path = "ris_whois.rs"]
mod ris_whois;
use ris_whois::{build_ipnet_trie, LookupTrie};

pub struct FirstLessSpecificState {
    trie: LookupTrie<String>,
}

impl Default for FirstLessSpecificState {
    fn default() -> Self {
        // There is no recovery if building the trie fails.
        let trie = build_ipnet_trie().unwrap();
        FirstLessSpecificState { trie }
    }
}

pub struct FirstLessSpecific;

impl VArrowScalar for FirstLessSpecific {
    type State = FirstLessSpecificState;

    fn invoke(
        info: &Self::State,
        input: duckdb::arrow::array::RecordBatch,
    ) -> std::result::Result<
        std::sync::Arc<dyn duckdb::arrow::array::Array>,
        Box<dyn std::error::Error>,
    > {
        let len = input.num_rows();

        let ip_array = input
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();

        // 13.48 as average length on a testset
        let mut builder = arrow::array::StringBuilder::with_capacity(len, len * 15);

        for i in ip_array {
            match i {
                Some(ip_str) => {
                    let res = info.trie.lookup(ip_str).map(|r| r.1.as_str());

                    match res {
                        Some(pfx) => builder.append_value(pfx),
                        None => builder.append_null(),
                    }
                }
                None => builder.append_null(),
            }
        }

        Ok(Arc::new(builder.finish()))
    }

    fn signatures() -> Vec<duckdb::vscalar::ArrowFunctionSignature> {
        vec![duckdb::vscalar::ArrowFunctionSignature::exact(
            vec![arrow::datatypes::DataType::Utf8],
            DataType::Utf8,
        )]
    }
}

/// Initialize env_logger with log level based on DUCKDB_LOG_LEVEL environment variable
fn init_logging() {
    // Map DuckDB log levels to env_logger levels
    let log_level = match env::var("DUCKDB_LOG_LEVEL").as_deref() {
        Ok("ERROR") => "error",
        Ok("WARN") => "warn",
        Ok("INFO") => "info",
        Ok("DEBUG") => "debug",
        Ok("TRACE") => "trace",
        _ => "info", // Default to info if not set or unrecognized
    };

    // Initialize env_logger with the determined log level
    let _ = env_logger::Builder::from_env(Env::default().default_filter_or(format!(
        "{}={}",
        env!("CARGO_PKG_NAME"),
        log_level
    )))
    .try_init();
}

/// # Safety
/// This function is called by DuckDB when the extension is loaded.
/// It registers the scalar function with DuckDB.
#[duckdb_entrypoint_c_api()]
pub unsafe fn extension_entrypoint(con: Connection) -> Result<(), Box<dyn Error>> {
    // Initialize logging with DuckDB log level
    init_logging();

    con.register_scalar_function::<FirstLessSpecific>("riswhois_longest_prefix")
        .expect("Failed to register function");
    Ok(())
}
