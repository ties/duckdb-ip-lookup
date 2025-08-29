use std::{fs::File, sync::Arc};

use arrow::{
    array::{Array, StringArray, UInt32Array},
    compute::{concat, sort_to_indices},
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use rand::distributions::Distribution;
use rand::rngs::StdRng;
use rand::{seq::SliceRandom, SeedableRng};
use rand_distr::{Exp, Zipf};

// Import the necessary types from the main library
use duckdb::vscalar::VArrowScalar;
use ip_more_less_specific::{FirstLessSpecific, FirstLessSpecificState};

const CHUNK_SIZE: usize = 2048;
const TEST_DATA_PATH: &str = "test/data/openintel-radar-2025-08-28.parquet";

fn benchmarking_data() -> (Arc<Schema>, StringArray) {
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

fn benchmark_first_less_specific(c: &mut Criterion) {
    // Initialize the FirstLessSpecific state (this will build the trie)
    let state = FirstLessSpecificState::default();

    // Get the benchmarking data
    let (schema, string_array) = benchmarking_data();

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
    let random_array = arrow::compute::take(&string_array, &indices_array, None).unwrap();
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
    let sort_indices = sort_to_indices(&string_array, None, None).unwrap();
    let sorted_array = arrow::compute::take(&string_array, &sort_indices, None).unwrap();
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

    group.finish();
}

fn benchmark_zipf_distributed(c: &mut Criterion) {
    // Initialize the FirstLessSpecific state (this will build the trie)
    let state = FirstLessSpecificState::default();

    // Get the benchmarking data
    let (schema, string_array) = benchmarking_data();

    let mut group = c.benchmark_group("zipf_distributed_1m");
    group.measurement_time(std::time::Duration::from_secs(60));

    // Standard zipf-distributed sample benchmark (1M elements)
    let n = string_array.len();
    let zipf_distribution = ZipfU64Wrapper::new(n as u64, 1.5).unwrap();
    let zipf_array =
        create_sample_from_distribution(&string_array, 1_000_000, zipf_distribution, 42);

    // Random order zipf benchmark with different seed (43) to avoid correlation
    let mut rng = StdRng::seed_from_u64(43);
    let mut indices: Vec<u32> = (0..zipf_array.len() as u32).collect();
    indices.shuffle(&mut rng);

    let indices_array = UInt32Array::from(indices);
    let random_zipf_array = arrow::compute::take(&zipf_array, &indices_array, None).unwrap();
    let random_zipf_batch = RecordBatch::try_new(schema.clone(), vec![random_zipf_array]).unwrap();
    let random_zipf_batches = create_chunks(&random_zipf_batch, CHUNK_SIZE);

    group.bench_function("random_order_seed43_zipf_data", |b| {
        b.iter(|| {
            for batch in &random_zipf_batches {
                let _result = black_box(FirstLessSpecific::invoke(&state, batch.clone()).unwrap());
            }
        })
    });

    // Sorted order zipf benchmark
    let sort_indices = sort_to_indices(&zipf_array, None, None).unwrap();
    let sorted_zipf_array = arrow::compute::take(&zipf_array, &sort_indices, None).unwrap();
    let sorted_zipf_batch = RecordBatch::try_new(schema.clone(), vec![sorted_zipf_array]).unwrap();
    let sorted_zipf_batches = create_chunks(&sorted_zipf_batch, CHUNK_SIZE);

    group.bench_function("alphabetical_order_zipf_data", |b| {
        b.iter(|| {
            for batch in &sorted_zipf_batches {
                let _result = black_box(FirstLessSpecific::invoke(&state, batch.clone()).unwrap());
            }
        })
    });

    group.finish();
}

fn benchmark_exponential_distributed(c: &mut Criterion) {
    // Initialize the FirstLessSpecific state (this will build the trie)
    let state = FirstLessSpecificState::default();

    // Get the benchmarking data
    let (schema, string_array) = benchmarking_data();
    let n = string_array.len() as u64;

    // Test different lambda values for temporal locality patterns
    let lambda_values = [
        (0.5, "strong_temporal_locality"),
        (0.05, "moderate_temporal_locality"),
        (0.005, "weak_temporal_locality"),
    ];

    for (lambda, label) in lambda_values {
        let mut group = c.benchmark_group(&format!(
            "exponential_distributed_lambda_{}_sample_1m",
            label
        ));
        group.measurement_time(std::time::Duration::from_secs(60));

        // Create exponential distributed sample (1M elements)
        let exp_distribution = ExpU64Wrapper::new(lambda, n);
        let exp_array =
            create_sample_from_distribution(&string_array, 1_000_000, exp_distribution, 42);

        // Random order exponential benchmark
        let mut rng = StdRng::seed_from_u64(43);
        let mut indices: Vec<u32> = (0..exp_array.len() as u32).collect();
        indices.shuffle(&mut rng);

        let indices_array = UInt32Array::from(indices);
        let random_exp_array = arrow::compute::take(&exp_array, &indices_array, None).unwrap();
        let random_exp_batch =
            RecordBatch::try_new(schema.clone(), vec![random_exp_array]).unwrap();
        let random_exp_batches = create_chunks(&random_exp_batch, CHUNK_SIZE);

        group.bench_function("random_order_exp_data", |b| {
            b.iter(|| {
                for batch in &random_exp_batches {
                    let _result =
                        black_box(FirstLessSpecific::invoke(&state, batch.clone()).unwrap());
                }
            })
        });

        // Sorted order exponential benchmark
        let sort_indices = sort_to_indices(&exp_array, None, None).unwrap();
        let sorted_exp_array = arrow::compute::take(&exp_array, &sort_indices, None).unwrap();
        let sorted_exp_batch =
            RecordBatch::try_new(schema.clone(), vec![sorted_exp_array]).unwrap();
        let sorted_exp_batches = create_chunks(&sorted_exp_batch, CHUNK_SIZE);

        group.bench_function("alphabetical_order_exp_data", |b| {
            b.iter(|| {
                for batch in &sorted_exp_batches {
                    let _result =
                        black_box(FirstLessSpecific::invoke(&state, batch.clone()).unwrap());
                }
            })
        });

        group.finish();
    }
}

fn create_sample_from_distribution<T>(
    string_array: &StringArray,
    sample_size: usize,
    distribution: T,
    seed: u64,
) -> StringArray
where
    T: Distribution<u64>,
{
    let mut rng = StdRng::seed_from_u64(seed);

    // Generate distribution-based indices
    let mut indices = Vec::with_capacity(sample_size);
    for _ in 0..sample_size {
        // Convert 1-based to 0-based index for distributions like Zipf
        let sample = distribution.sample(&mut rng);
        let index = (sample - 1) as u32;
        indices.push(index);
    }

    // Create array from distribution-based indices
    let indices_array = UInt32Array::from(indices);
    let sampled_array = arrow::compute::take(string_array, &indices_array, None).unwrap();

    sampled_array
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .clone()
}

// Wrapper for Zipf distribution to convert f64 to u64
struct ZipfU64Wrapper {
    zipf: Zipf<f64>,
}

impl ZipfU64Wrapper {
    fn new(n: u64, exponent: f64) -> Result<Self, rand_distr::ZipfError> {
        Ok(Self {
            zipf: Zipf::new(n, exponent)?,
        })
    }
}

impl Distribution<u64> for ZipfU64Wrapper {
    fn sample<R: rand::Rng + ?Sized>(&self, rng: &mut R) -> u64 {
        self.zipf.sample(rng) as u64
    }
}

// Wrapper for Exponential distribution to convert to bounded u64 indices
struct ExpU64Wrapper {
    exp: Exp<f64>,
    max_index: u64,
}

impl ExpU64Wrapper {
    fn new(lambda: f64, max_index: u64) -> Self {
        Self {
            exp: Exp::new(lambda).unwrap(),
            max_index,
        }
    }
}

impl Distribution<u64> for ExpU64Wrapper {
    fn sample<R: rand::Rng + ?Sized>(&self, rng: &mut R) -> u64 {
        // Sample from exponential distribution and map to index range [1, max_index]
        let exp_sample = self.exp.sample(rng);
        // Use modulo to wrap around and add 1 to make it 1-based
        ((exp_sample as u64) % self.max_index) + 1
    }
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

criterion_group!(
    benches,
    benchmark_first_less_specific,
    benchmark_zipf_distributed,
    benchmark_exponential_distributed
);
criterion_main!(benches);
