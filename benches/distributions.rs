use arrow::{
    array::{StringArray, UInt32Array},
    compute,
};
use rand::distributions::Distribution;
use rand::rngs::StdRng;
use rand::SeedableRng;
use rand_distr::{Exp, Zipf};

pub struct ZipfU64Wrapper {
    zipf: Zipf<f64>,
}

impl ZipfU64Wrapper {
    pub fn new(n: u64, exponent: f64) -> Result<Self, rand_distr::ZipfError> {
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

pub struct ExpU64Wrapper {
    exp: Exp<f64>,
    max_index: u64,
}

impl ExpU64Wrapper {
    pub fn new(lambda: f64, max_index: u64) -> Self {
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

pub fn create_sample_from_distribution<T>(
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
    let sampled_array = compute::take(string_array, &indices_array, None).unwrap();

    sampled_array
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .clone()
}
