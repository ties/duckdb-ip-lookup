extern crate duckdb;
extern crate duckdb_loadable_macros;
extern crate libduckdb_sys;

use duckdb::ffi;

use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{fmt, EnvFilter};

use duckdb::{
    arrow::{self, array::Array, datatypes::DataType},
    vscalar::VArrowScalar,
    Connection, Result,
};
use duckdb_loadable_macros::duckdb_entrypoint_c_api;
use std::sync::Arc;
use std::{env, error::Error};

mod lib {
    pub mod ris_whois;
}
use crate::lib::ris_whois::{build_ipnet_trie, LookupTrie};

struct FirstLessSpecificState {
    trie: LookupTrie<()>,
}

impl Default for FirstLessSpecificState {
    fn default() -> Self {
        // There is no recovery if building the trie fails.
        let trie = build_ipnet_trie().unwrap();
        FirstLessSpecificState { trie }
    }
}

struct FirstLessSpecific;

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
                    let res = info
                        .trie
                        .lookup(ip_str)
                        .map(|r| r.0)
                        .map(|ipnet| ipnet.to_string());

                    builder.append_option(res);
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

/// Initialize tracing subscriber with log level based on DUCKDB_LOG_LEVEL environment variable
fn init_tracing() {
    // Map DuckDB log levels to tracing levels
    let log_level = match env::var("DUCKDB_LOG_LEVEL").as_deref() {
        Ok("ERROR") => "error",
        Ok("WARN") => "warn",
        Ok("INFO") => "info",
        Ok("DEBUG") => "debug",
        Ok("TRACE") => "trace",
        _ => "info", // Default to info if not set or unrecognized
    };

    // Create env filter with the determined log level for this crate
    let filter = EnvFilter::new(format!("{}={}", env!("CARGO_PKG_NAME"), log_level));

    // Initialize subscriber only if not already initialized
    let _ = tracing_subscriber::registry()
        .with(fmt::layer())
        .with(filter)
        .try_init();
}

/// # Safety
/// This function is called by DuckDB when the extension is loaded.
/// It registers the scalar function with DuckDB.
#[duckdb_entrypoint_c_api()]
pub unsafe fn extension_entrypoint(con: Connection) -> Result<(), Box<dyn Error>> {
    // Initialize tracing with DuckDB log level
    init_tracing();

    con.register_scalar_function::<FirstLessSpecific>("riswhois_longest_prefix")
        .expect("Failed to register function");
    Ok(())
}
