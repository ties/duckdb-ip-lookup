mod data;
mod distributions;

use std::sync::Arc;

use arrow::{
    array::{Array, StringArray, UInt32Array},
    compute::sort_to_indices,
    record_batch::RecordBatch,
};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rand::rngs::StdRng;
use rand::{seq::SliceRandom, SeedableRng};

use duckdb::vscalar::VArrowScalar;
use ip_more_less_specific::{FirstLessSpecific, FirstLessSpecificState};

use data::{
    create_chunks, create_dataset_with_nulls_uniform, create_dataset_with_nulls_zipf,
    load_benchmarking_data,
};
use distributions::{create_sample_from_distribution, ExpU64Wrapper, ZipfU64Wrapper};

const CHUNK_SIZE: usize = 2048;

fn benchmark_first_less_specific(c: &mut Criterion) {
    let state = FirstLessSpecificState::default();
    let (schema, string_array) = load_benchmarking_data();

    let mut group = c.benchmark_group("first_less_specific");
    group.measurement_time(std::time::Duration::from_secs(60));

    let original_batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(string_array.clone())]).unwrap();
    let original_batches = create_chunks(&original_batch, CHUNK_SIZE);

    benchmark_order_variant(
        &mut group,
        &state,
        &original_batches,
        "original_order_openintel_radar_data",
    );

    let random_batches = create_random_order_batches(&schema, &string_array, 42);
    benchmark_order_variant(
        &mut group,
        &state,
        &random_batches,
        "random_order_seed42_openintel_radar_data",
    );

    let sorted_batches = create_sorted_batches(&schema, &string_array);
    benchmark_order_variant(
        &mut group,
        &state,
        &sorted_batches,
        "alphabetical_order_openintel_radar_data",
    );

    group.finish();
}

fn benchmark_zipf_distributed(c: &mut Criterion) {
    let state = FirstLessSpecificState::default();
    let (schema, string_array) = load_benchmarking_data();

    let mut group = c.benchmark_group("zipf_distributed_1m");
    group.measurement_time(std::time::Duration::from_secs(60));

    let zipf_distribution = ZipfU64Wrapper::new(string_array.len() as u64, 1.5).unwrap();
    let zipf_array = Arc::new(create_sample_from_distribution(
        &string_array,
        1_000_000,
        zipf_distribution,
        42,
    ));

    let zipf_batches = create_chunks(
        &RecordBatch::try_new(schema.clone(), vec![zipf_array.clone()]).unwrap(),
        CHUNK_SIZE,
    );
    benchmark_order_variant(&mut group, &state, &zipf_batches, "original");

    let sorted_zipf_batches = create_sorted_batches(&schema, &zipf_array);
    benchmark_order_variant(
        &mut group,
        &state,
        &sorted_zipf_batches,
        "alphabetical_order_zipf_data",
    );

    group.finish();
}

fn benchmark_exponential_distributed(c: &mut Criterion) {
    let state = FirstLessSpecificState::default();
    let (schema, string_array) = load_benchmarking_data();

    let lambda_values = [0.5, 0.05, 0.005]; // Strong, moderate, weak temporal locality
    let mut group = c.benchmark_group("exponential_distributed");
    group.measurement_time(std::time::Duration::from_secs(60));

    for lambda in lambda_values {
        let exp_distribution = ExpU64Wrapper::new(lambda, string_array.len() as u64);
        let exp_array =
            create_sample_from_distribution(&string_array, 1_000_000, exp_distribution, 42);
        let exp_batches = create_chunks(
            &RecordBatch::try_new(schema.clone(), vec![Arc::new(exp_array)]).unwrap(),
            CHUNK_SIZE,
        );

        benchmark_order_variant(
            &mut group,
            &state,
            &exp_batches,
            &format!("lambda_{lambda}"),
        );
    }
    group.finish();
}

fn benchmark_null_data_uniform(c: &mut Criterion) {
    let state = FirstLessSpecificState::default();
    let (schema, string_array) = load_benchmarking_data();

    let mut group = c.benchmark_group("null_data_uniform");
    group.measurement_time(std::time::Duration::from_secs(60));

    let null_percentages = [0.2, 0.8]; // 20% and 80% nulls
    let seeds = [42, 123]; // Different seeds for variety

    for &null_percentage in &null_percentages {
        for &seed in &seeds {
            let array_with_nulls =
                create_dataset_with_nulls_uniform(&string_array, null_percentage, seed);
            let batch_with_nulls =
                RecordBatch::try_new(schema.clone(), vec![Arc::new(array_with_nulls)]).unwrap();
            let batches_with_nulls = create_chunks(&batch_with_nulls, CHUNK_SIZE);

            let bench_name = format!(
                "uniform_{}pct_nulls_seed{}",
                (null_percentage * 100.0) as u32,
                seed
            );
            benchmark_order_variant(&mut group, &state, &batches_with_nulls, &bench_name);
        }
    }

    group.finish();
}

fn benchmark_null_data_zipf(c: &mut Criterion) {
    let state = FirstLessSpecificState::default();
    let (schema, string_array) = load_benchmarking_data();

    let mut group = c.benchmark_group("null_data_zipf");
    group.measurement_time(std::time::Duration::from_secs(60));

    let null_percentages = [0.2, 0.8]; // 20% and 80% nulls
    let zipf_exponents = [1.0, 1.5]; // Different zipf exponents
    let seed = 42;

    for &null_percentage in &null_percentages {
        for &exponent in &zipf_exponents {
            let array_with_nulls =
                create_dataset_with_nulls_zipf(&string_array, null_percentage, exponent, seed);
            let batch_with_nulls =
                RecordBatch::try_new(schema.clone(), vec![Arc::new(array_with_nulls)]).unwrap();
            let batches_with_nulls = create_chunks(&batch_with_nulls, CHUNK_SIZE);

            let bench_name = format!(
                "zipf_{}pct_nulls_exp{}",
                (null_percentage * 100.0) as u32,
                exponent
            );
            benchmark_order_variant(&mut group, &state, &batches_with_nulls, &bench_name);
        }
    }

    group.finish();
}

fn benchmark_null_data_combined_distributions(c: &mut Criterion) {
    let state = FirstLessSpecificState::default();
    let (schema, string_array) = load_benchmarking_data();

    let mut group = c.benchmark_group("null_data_combined_distributions");
    group.measurement_time(std::time::Duration::from_secs(60));

    let null_percentage = 0.2; // 20% nulls
    let seed = 42;

    // Test with original data distribution + uniform nulls
    let original_with_nulls =
        create_dataset_with_nulls_uniform(&string_array, null_percentage, seed);
    let original_batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(original_with_nulls)]).unwrap();
    let original_batches = create_chunks(&original_batch, CHUNK_SIZE);
    benchmark_order_variant(
        &mut group,
        &state,
        &original_batches,
        "original_data_uniform_nulls",
    );

    // Test with zipf data distribution + uniform nulls
    let zipf_distribution = ZipfU64Wrapper::new(string_array.len() as u64, 1.5).unwrap();
    let zipf_array =
        create_sample_from_distribution(&string_array, 100_000, zipf_distribution, seed);
    let zipf_with_nulls = create_dataset_with_nulls_uniform(&zipf_array, null_percentage, seed + 1);
    let zipf_batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(zipf_with_nulls)]).unwrap();
    let zipf_batches = create_chunks(&zipf_batch, CHUNK_SIZE);
    benchmark_order_variant(&mut group, &state, &zipf_batches, "zipf_data_uniform_nulls");

    // Test with exponential data distribution + uniform nulls
    let exp_distribution = ExpU64Wrapper::new(0.05, string_array.len() as u64);
    let exp_array = create_sample_from_distribution(&string_array, 100_000, exp_distribution, seed);
    let exp_with_nulls = create_dataset_with_nulls_uniform(&exp_array, null_percentage, seed + 2);
    let exp_batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(exp_with_nulls)]).unwrap();
    let exp_batches = create_chunks(&exp_batch, CHUNK_SIZE);
    benchmark_order_variant(&mut group, &state, &exp_batches, "exp_data_uniform_nulls");

    group.finish();
}

fn benchmark_order_variant(
    group: &mut criterion::BenchmarkGroup<criterion::measurement::WallTime>,
    state: &FirstLessSpecificState,
    batches: &[RecordBatch],
    name: &str,
) {
    group.bench_function(name, |b| {
        b.iter(|| {
            for batch in batches {
                let _result = black_box(FirstLessSpecific::invoke(state, batch.clone()).unwrap());
            }
        })
    });
}

fn create_random_order_batches(
    schema: &Arc<arrow::datatypes::Schema>,
    string_array: &StringArray,
    seed: u64,
) -> Vec<RecordBatch> {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut indices: Vec<u32> = (0..string_array.len() as u32).collect();
    indices.shuffle(&mut rng);

    let indices_array = UInt32Array::from(indices);
    let random_array = arrow::compute::take(string_array, &indices_array, None).unwrap();
    let random_batch = RecordBatch::try_new(schema.clone(), vec![random_array]).unwrap();

    create_chunks(&random_batch, CHUNK_SIZE)
}

fn create_sorted_batches(
    schema: &Arc<arrow::datatypes::Schema>,
    string_array: &StringArray,
) -> Vec<RecordBatch> {
    let sort_indices = sort_to_indices(string_array, None, None).unwrap();
    let sorted_array = arrow::compute::take(string_array, &sort_indices, None).unwrap();
    let sorted_batch = RecordBatch::try_new(schema.clone(), vec![sorted_array]).unwrap();

    create_chunks(&sorted_batch, CHUNK_SIZE)
}

criterion_group!(
    benches,
    benchmark_first_less_specific,
    benchmark_zipf_distributed,
    benchmark_exponential_distributed,
    benchmark_null_data_uniform,
    benchmark_null_data_zipf,
    benchmark_null_data_combined_distributions,
);
criterion_main!(benches);
