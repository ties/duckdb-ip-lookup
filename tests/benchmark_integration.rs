use std::fs::File;
use std::time::Instant;

use arrow::datatypes::DataType;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

// Import the necessary types from the main library
use duckdb::vscalar::VArrowScalar;
use ip_more_less_specific::{FirstLessSpecific, FirstLessSpecificState};

const CHUNK_SIZE: usize = 2048;
const TEST_DATA_PATH: &str = "test/data/openintel-radar-2025-08-28.parquet";

#[test]
fn test_first_less_specific_benchmark() {
    println!("Starting FirstLessSpecific benchmark test...");

    // Initialize the FirstLessSpecific state (this will build the trie)
    let start_init = Instant::now();
    let state = FirstLessSpecificState::default();
    let init_duration = start_init.elapsed();
    println!(
        "FirstLessSpecific state initialization took: {:?}",
        init_duration
    );

    // Open the parquet file
    let file = File::open(TEST_DATA_PATH).unwrap();

    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();

    // Assert that the parquet file has only the expected 'ip' column
    let schema = builder.schema();
    assert_eq!(schema.fields().len(), 1, "Expected exactly one column");
    assert_eq!(
        schema.field(0).name(),
        "ip",
        "Expected column to be named 'ip'"
    );
    assert!(
        matches!(
            schema.field(0).data_type(),
            DataType::Utf8 | DataType::LargeUtf8
        ),
        "Expected 'ip' column to be a string type"
    );

    // Build the reader with our chunk size
    let mut reader = builder.with_batch_size(CHUNK_SIZE).build().unwrap();

    let mut total_rows = 0;
    let mut total_processing_time = std::time::Duration::new(0, 0);
    let mut chunk_count = 0;

    // Process each chunk
    while let Some(batch) = reader.next() {
        let batch = batch.unwrap();
        let rows_in_batch = batch.num_rows();

        if rows_in_batch == 0 {
            continue;
        }

        // Time the processing - use the batch directly since it only has the ip column
        let start_chunk = Instant::now();
        let _result = FirstLessSpecific::invoke(&state, batch).unwrap();
        let chunk_duration = start_chunk.elapsed();

        total_processing_time += chunk_duration;
        total_rows += rows_in_batch;
        chunk_count += 1;
    }

    println!("\n=== BENCHMARK RESULTS ===");
    println!("Total chunks processed: {}", chunk_count);
    println!("Total rows processed: {}", total_rows);
    println!("Total processing time: {:?}", total_processing_time);
    println!(
        "Average processing time per chunk: {:?}",
        total_processing_time / chunk_count as u32
    );
    println!(
        "Average rows per second: {:.2}",
        total_rows as f64 / total_processing_time.as_secs_f64()
    );
    println!(
        "Average milliseconds per 1000 rows: {:.2}",
        total_processing_time.as_millis() as f64 / (total_rows as f64 / 1000.0)
    );
}
