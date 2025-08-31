use std::{fs::File, sync::Arc};

use arrow::{
    array::{Array, StringArray},
    compute::concat,
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

const CHUNK_SIZE: usize = 2048;
const TEST_DATA_PATH: &str = "test/data/openintel-radar-2025-08-28.parquet";

pub fn load_benchmarking_data() -> (Arc<Schema>, StringArray) {
    // Open the parquet file
    let file = File::open(TEST_DATA_PATH).expect("Failed to open test data file");
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();

    // Verify the parquet file structure
    let schema = builder.schema();
    assert_eq!(schema.fields().len(), 1);
    assert_eq!(schema.field(0).name(), "ip");
    assert!(matches!(schema.field(0).data_type(), DataType::Utf8));

    // Build the reader and collect all batches.
    let reader = builder.with_batch_size(CHUNK_SIZE).build().unwrap();
    let batches: Vec<RecordBatch> = reader.into_iter().map(|r| r.unwrap()).collect();

    let schema = batches[0].schema();

    // Concatenate all batches into one big array
    let all_arrays: Vec<&dyn Array> = batches
        .iter()
        .map(|batch| batch.column(0).as_ref())
        .collect();
    let concatenated_array = concat(&all_arrays).unwrap();
    let string_array = concatenated_array
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    (schema, string_array.to_owned())
}

pub fn create_chunks(batch: &RecordBatch, chunk_size: usize) -> Vec<RecordBatch> {
    let mut chunks = Vec::new();
    let total_rows = batch.num_rows();

    for start in (0..total_rows).step_by(chunk_size) {
        let end = std::cmp::min(start + chunk_size, total_rows);
        let chunk = batch.slice(start, end - start);
        chunks.push(chunk);
    }

    chunks
}