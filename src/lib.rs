extern crate duckdb;
extern crate duckdb_loadable_macros;
extern crate libduckdb_sys;

pub(crate) mod ris_whois;

use duckdb::{
    arrow::{self, array::Array, datatypes::DataType}, vscalar::VArrowScalar, Connection, Result
};
use duckdb_loadable_macros::duckdb_entrypoint_c_api;
use libduckdb_sys as ffi;
use std::{
    error::Error, sync::Arc,
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

const EXTENSION_NAME: &str = env!("CARGO_PKG_NAME");

#[duckdb_entrypoint_c_api()]
pub unsafe fn extension_entrypoint(con: Connection) -> Result<(), Box<dyn Error>> {
    con.register_scalar_function::<FirstLessSpecific>(EXTENSION_NAME).expect("Failed to register function");
    Ok(())
}