use std::{fs::File, sync::Arc};

use arrow::{
    array::{Array, StringArray, UInt32Array},
    compute::{concat, sort_to_indices},
    datatypes::DataType,
    record_batch::RecordBatch,
};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use rand::distributions::Distribution;
use rand::rngs::StdRng;
use rand::{seq::SliceRandom, SeedableRng};
use rand_distr::Zipf;

// Import the necessary types from the main library
use duckdb::vscalar::VArrowScalar;
use ip_more_less_specific::{FirstLessSpecific, FirstLessSpecificState};

const CHUNK_SIZE: usize = 2048;
const TEST_DATA_PATH: &str = "test/data/openintel-radar-2025-08-28.parquet";

fn benchmark_first_less_specific(c: &mut Criterion) {
    // Initialize the FirstLessSpecific state (this will build the trie)
    let state = FirstLessSpecificState::default();

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

    let mut group = c.benchmark_group("first_less_specific");
    group.measurement_time(std::time::Duration::from_secs(60));

    let batches = create_chunks(
        &RecordBatch::try_new(schema.clone(), vec![Arc::new(string_array.clone())]).unwrap(),
        CHUNK_SIZE,
    );

    group.bench_function("original_order_openintel_radar_data", |b| {
        b.iter(|| {
            for batch in &batches {
                let _result = black_box(FirstLessSpecific::invoke(&state, batch.clone()).unwrap());
            }
        })
    });

    // Random order benchmark with seed=42
    let mut rng = StdRng::seed_from_u64(42);
    let mut indices: Vec<u32> = (0..string_array.len() as u32).collect();
    indices.shuffle(&mut rng);

    let indices_array = UInt32Array::from(indices);
    let random_array = arrow::compute::take(string_array, &indices_array, None).unwrap();
    let random_batch = RecordBatch::try_new(batches[0].schema(), vec![random_array]).unwrap();

    // Split back into chunks for processing
    let random_batches = create_chunks(&random_batch, CHUNK_SIZE);

    group.bench_function("random_order_seed42_openintel_radar_data", |b| {
        b.iter(|| {
            for batch in &random_batches {
                let _result = black_box(FirstLessSpecific::invoke(&state, batch.clone()).unwrap());
            }
        })
    });

    // Alphabetical order benchmark
    let sort_indices = sort_to_indices(string_array, None, None).unwrap();
    let sorted_array = arrow::compute::take(string_array, &sort_indices, None).unwrap();
    let sorted_batch = RecordBatch::try_new(batches[0].schema(), vec![sorted_array]).unwrap();

    // Split back into chunks for processing
    let sorted_batches = create_chunks(&sorted_batch, CHUNK_SIZE);

    group.bench_function("alphabetical_order_openintel_radar_data", |b| {
        b.iter(|| {
            for batch in &sorted_batches {
                let _result = black_box(FirstLessSpecific::invoke(&state, batch.clone()).unwrap());
            }
        })
    });

    // Zipf-distributed sample benchmark (1M elements)
    let zipf_samples = create_zipf_sample(string_array, 1_000_000, 1.5, 42);
    let zipf_batches = create_chunks(&zipf_samples, CHUNK_SIZE);

    group.bench_function("zipf_distributed_1m_openintel_radar_data", |b| {
        b.iter(|| {
            for batch in &zipf_batches {
                let _result = black_box(FirstLessSpecific::invoke(&state, batch.clone()).unwrap());
            }
        })
    });

    group.finish();
}

fn create_zipf_sample(
    string_array: &StringArray,
    sample_size: usize,
    exponent: f64,
    seed: u64,
) -> RecordBatch {
    let mut rng = StdRng::seed_from_u64(seed);
    let n = string_array.len();

    // Create Zipf distribution
    let zipf = Zipf::new(n as u64, exponent).unwrap();

    // Generate Zipf-distributed indices
    let mut zipf_indices = Vec::with_capacity(sample_size);
    for _ in 0..sample_size {
        // zipf.sample() returns 1-based index, convert to 0-based
        let index = (zipf.sample(&mut rng) as u64 - 1) as u32;
        zipf_indices.push(index);
    }

    // Create array from Zipf-distributed indices
    let indices_array = UInt32Array::from(zipf_indices);
    let zipf_array = arrow::compute::take(string_array, &indices_array, None).unwrap();

    // Create record batch
    let schema = string_array.data_type().clone();
    RecordBatch::try_new(
        Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("ip", schema, false),
        ])),
        vec![zipf_array],
    )
    .unwrap()
}

fn create_chunks(batch: &RecordBatch, chunk_size: usize) -> Vec<RecordBatch> {
    let mut chunks = Vec::new();
    let total_rows = batch.num_rows();

    for start in (0..total_rows).step_by(chunk_size) {
        let end = std::cmp::min(start + chunk_size, total_rows);
        let chunk = batch.slice(start, end - start);
        chunks.push(chunk);
    }

    chunks
}

criterion_group!(benches, benchmark_first_less_specific);
criterion_main!(benches);
