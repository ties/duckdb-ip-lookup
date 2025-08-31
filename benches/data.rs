use std::{fs::File, sync::Arc};

use arrow::{
    array::{Array, StringArray},
    compute::concat,
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
    array::builder::StringBuilder,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;
use rand::distributions::Distribution;

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

pub fn inject_nulls_with_linear_probing(
    string_array: &StringArray,
    null_positions: &[usize],
) -> StringArray {
    let mut builder = StringBuilder::new();
    let array_len = string_array.len();
    
    // Create a boolean vector to track positions to make null (true = available for null injection)
    let mut available_positions = vec![true; array_len];
    
    // Mark existing null positions as unavailable
    for i in 0..array_len {
        if string_array.is_null(i) {
            available_positions[i] = false;
        }
    }
    
    // Track which positions will be made null
    let mut positions_to_null = vec![false; array_len];
    let mut nulls_injected = 0;
    
    for &target_pos in null_positions {
        let mut current_pos = target_pos % array_len;
        
        // Use linear probing to find an available position
        while !available_positions[current_pos] {
            current_pos = (current_pos + 1) % array_len;
            
            // Prevent infinite loop if all positions are unavailable
            if nulls_injected >= array_len {
                break;
            }
        }
        
        // Mark this position for null injection if available
        if available_positions[current_pos] && nulls_injected < array_len {
            positions_to_null[current_pos] = true;
            available_positions[current_pos] = false; // Mark as unavailable for future injections
            nulls_injected += 1;
        }
    }
    
    // Build the new array with nulls in the determined positions
    for i in 0..array_len {
        if positions_to_null[i] {
            builder.append_null();
        } else {
            builder.append_value(string_array.value(i));
        }
    }
    
    builder.finish()
}

pub fn create_null_positions_uniform(array_len: usize, null_percentage: f64, seed: u64) -> Vec<usize> {
    let mut rng = StdRng::seed_from_u64(seed);
    let num_nulls = (array_len as f64 * null_percentage) as usize;
    
    let mut positions = Vec::new();
    for _ in 0..num_nulls {
        let pos = rng.gen_range(0..array_len);
        positions.push(pos);
    }
    
    positions
}

pub fn create_null_positions_zipf(array_len: usize, null_percentage: f64, exponent: f64, seed: u64) -> Vec<usize> {
    use rand_distr::Zipf;
    
    let mut rng = StdRng::seed_from_u64(seed);
    let num_nulls = (array_len as f64 * null_percentage) as usize;
    let zipf = Zipf::new(array_len as u64, exponent).unwrap();
    
    let mut positions = Vec::new();
    for _ in 0..num_nulls {
        // Convert 1-based Zipf to 0-based array index
        let pos = (zipf.sample(&mut rng) as usize) - 1;
        positions.push(pos);
    }
    
    positions
}

pub fn create_dataset_with_nulls_uniform(
    string_array: &StringArray,
    null_percentage: f64,
    seed: u64,
) -> StringArray {
    let null_positions = create_null_positions_uniform(string_array.len(), null_percentage, seed);
    inject_nulls_with_linear_probing(string_array, &null_positions)
}

pub fn create_dataset_with_nulls_zipf(
    string_array: &StringArray,
    null_percentage: f64,
    exponent: f64,
    seed: u64,
) -> StringArray {
    let null_positions = create_null_positions_zipf(string_array.len(), null_percentage, exponent, seed);
    inject_nulls_with_linear_probing(string_array, &null_positions)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;

    #[test]
    fn test_inject_nulls_basic() {
        let array = StringArray::from(vec!["a", "b", "c", "d", "e"]);
        let null_positions = vec![1, 3];
        
        let result = inject_nulls_with_linear_probing(&array, &null_positions);
        
        assert_eq!(result.len(), 5);
        assert_eq!(result.value(0), "a");
        assert!(result.is_null(1));
        assert_eq!(result.value(2), "c");
        assert!(result.is_null(3));
        assert_eq!(result.value(4), "e");
    }

    #[test]
    fn test_inject_nulls_linear_probing() {
        // Create array with existing null at position 1
        let mut builder = StringBuilder::new();
        builder.append_value("a");
        builder.append_null(); // position 1 is already null
        builder.append_value("c");
        builder.append_value("d");
        builder.append_value("e");
        let array = builder.finish();
        
        // Try to inject null at position 1, should probe to next available position
        let null_positions = vec![1];
        
        let result = inject_nulls_with_linear_probing(&array, &null_positions);
        
        assert_eq!(result.len(), 5);
        assert_eq!(result.value(0), "a");
        assert!(result.is_null(1)); // Original null
        assert!(result.is_null(2)); // New null after probing
        assert_eq!(result.value(3), "d");
        assert_eq!(result.value(4), "e");
    }

    #[test]
    fn test_inject_nulls_wraparound() {
        let array = StringArray::from(vec!["a", "b", "c"]);
        // Try to inject at position 5, should wrap to position 2
        let null_positions = vec![5];
        
        let result = inject_nulls_with_linear_probing(&array, &null_positions);
        
        assert_eq!(result.len(), 3);
        assert_eq!(result.value(0), "a");
        assert_eq!(result.value(1), "b");
        assert!(result.is_null(2));
    }

    #[test]
    fn test_inject_nulls_no_duplicates() {
        let array = StringArray::from(vec!["a", "b", "c", "d", "e"]);
        // Try to inject at same position twice
        let null_positions = vec![1, 1, 1];
        
        let result = inject_nulls_with_linear_probing(&array, &null_positions);
        
        assert_eq!(result.len(), 5);
        assert_eq!(result.value(0), "a");
        assert!(result.is_null(1)); // First injection at position 1
        assert!(result.is_null(2)); // Second injection probes to position 2
        assert!(result.is_null(3)); // Third injection probes to position 3
        assert_eq!(result.value(4), "e");
    }

    #[test]
    fn test_inject_nulls_empty_positions() {
        let array = StringArray::from(vec!["a", "b", "c"]);
        let null_positions = vec![];
        
        let result = inject_nulls_with_linear_probing(&array, &null_positions);
        
        assert_eq!(result.len(), 3);
        assert_eq!(result.value(0), "a");
        assert_eq!(result.value(1), "b");
        assert_eq!(result.value(2), "c");
        // No nulls should be injected
        for i in 0..3 {
            assert!(!result.is_null(i));
        }
    }

    #[test]
    fn test_inject_nulls_all_positions_already_null() {
        let mut builder = StringBuilder::new();
        builder.append_null();
        builder.append_null();
        builder.append_null();
        let array = builder.finish();
        
        let null_positions = vec![0, 1, 2];
        
        let result = inject_nulls_with_linear_probing(&array, &null_positions);
        
        assert_eq!(result.len(), 3);
        // All positions should remain null, no new nulls should be added
        for i in 0..3 {
            assert!(result.is_null(i));
        }
    }

    #[test]
    fn test_create_null_positions_uniform() {
        let positions = create_null_positions_uniform(100, 0.2, 42);
        assert_eq!(positions.len(), 20); // 20% of 100
        
        // All positions should be within bounds
        for &pos in &positions {
            assert!(pos < 100);
        }
    }

    #[test]
    fn test_create_null_positions_zipf() {
        let positions = create_null_positions_zipf(100, 0.1, 1.5, 42);
        assert_eq!(positions.len(), 10); // 10% of 100
        
        // All positions should be within bounds
        for &pos in &positions {
            assert!(pos < 100);
        }
    }
}