extern crate duckdb;
extern crate duckdb_loadable_macros;
extern crate libduckdb_sys;

use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{fmt, EnvFilter};

use duckdb::{
    arrow::{self, array::Array, datatypes::DataType}, vscalar::VArrowScalar, Connection, Result
};
use duckdb_loadable_macros::duckdb_entrypoint_c_api;
use libduckdb_sys as ffi;
use std::{
    env, error::Error, sync::Arc
};

#[derive(Default)]
struct FirstLessSpecificState {

}


struct FirstLessSpecific;

impl VArrowScalar for FirstLessSpecific {
    type State = FirstLessSpecificState;

    fn invoke(info: &Self::State, input: duckdb::arrow::array::RecordBatch) -> std::result::Result<std::sync::Arc<dyn duckdb::arrow::array::Array>, Box<dyn std::error::Error>> {
        let len = input.num_rows();

        println!("ArrowFunc invoked with {} rows - pass", len);

        let name_array = input
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();

        let mut results = Vec::with_capacity(len);
        for i in 0..len {
            if name_array.is_valid(i) {
                let name = name_array.value(i);
                let result = format!("Hello, {}!", name);
                results.push(Some(result));
            } else {
                results.push(None);
            }
        }

        Ok(Arc::new(arrow::array::StringArray::from(results)))
    }

    fn signatures() -> Vec<duckdb::vscalar::ArrowFunctionSignature> {
        vec![duckdb::vscalar::ArrowFunctionSignature::exact(
            vec![arrow::datatypes::DataType::Utf8], DataType::Utf8
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

const EXTENSION_NAME: &str = env!("CARGO_PKG_NAME");

#[duckdb_entrypoint_c_api()]
pub unsafe fn extension_entrypoint(con: Connection) -> Result<(), Box<dyn Error>> {
    // Initialize tracing with DuckDB log level
    init_tracing();

    con.register_scalar_function::<FirstLessSpecific>(EXTENSION_NAME).expect("Failed to register function");
    Ok(())
}