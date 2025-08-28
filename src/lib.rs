extern crate duckdb;
extern crate duckdb_loadable_macros;
extern crate libduckdb_sys;

use ipnet_trie::IpnetTrie;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{fmt, EnvFilter};

use duckdb::{
    arrow::{self, array::Array, datatypes::DataType},
    vscalar::VArrowScalar,
    Connection, Result,
};
use duckdb_loadable_macros::duckdb_entrypoint_c_api;
use ipnet::IpNet;
use libduckdb_sys as ffi;
use std::sync::{Arc, LazyLock};
use std::{env, error::Error, net::IpAddr};

mod lib {
    pub mod ris_whois;
}
use crate::lib::ris_whois::build_ipnet_trie;

static IP_TRIE: LazyLock<IpnetTrie<()>> = LazyLock::new(|| {
    build_ipnet_trie().unwrap_or_else(|e| {
        eprintln!("Failed to build IP trie: {}", e);
        IpnetTrie::new()
    })
});

#[derive(Default)]
struct FirstLessSpecificState;

struct FirstLessSpecific;

impl VArrowScalar for FirstLessSpecific {
    type State = FirstLessSpecificState;

    fn invoke(
        _info: &Self::State,
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

        let mut builder = arrow::array::StringBuilder::with_capacity(len, len * 15);

        let mut last_value: Option<(String, Option<String>)> = None;
        for i in 0..len {
            if ip_array.is_valid(i) {
                let ip_str = ip_array.value(i);

                match &last_value {
                    Some((last_ip_str, last_result)) if last_ip_str == ip_str => {
                        builder.append_option(last_result.clone());
                        continue;
                    }
                    _ => {}
                }

                // Parse the input as either an IP network (with CIDR) or IP address
                let ipnet = match ip_str {
                    s if s.contains('/') => s.parse::<IpNet>().ok(),
                    s => s.parse::<IpAddr>().ok().map(IpNet::from),
                };

                let result = match ipnet {
                    Some(net) => {
                        // Find the longest matching prefix in the trie
                        IP_TRIE
                            .longest_match(&net)
                            .map(|(matched_net, _)| format!("{}", matched_net))
                    }
                    None => None,
                };

                match result {
                    Some(ref r) => builder.append_value(r),
                    None => builder.append_null(),
                }

                last_value = Some((ip_str.to_string(), result));
            } else {
                builder.append_null();
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
