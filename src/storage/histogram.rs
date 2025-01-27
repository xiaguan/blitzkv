//! Copied from my historian crate. - Tyler Neely
//!
//! A zero-config simple histogram collector
//!
//! for use in instrumented optimization.
//! Uses logarithmic bucketing rather than sampling,
//! and has bounded (generally <0.5%) error on percentiles.
//! Performs no allocations after initial creation.
//! Uses Relaxed atomics during collection.
//!
//! When you create it, it allocates 65k `AtomicU64`'s
//! that it uses for incrementing. Generating reports
//! after running workloads on dozens of `Histogram`'s
//! does not result in a perceptible delay, but it
//! might not be acceptable for use in low-latency
//! reporting paths.
//!
//! The trade-offs taken in this are to minimize latency
//! during collection, while initial allocation and
//! postprocessing delays are acceptable.
//!
//! Future work to further reduce collection latency
//! may include using thread-local caches that perform
//! no atomic operations until they are dropped, when
//! they may atomically aggregate their measurements
//! into the shared collector that will be used for
//! reporting.
#![allow(unused)]
#![allow(unused_results)]
#![allow(clippy::print_stdout)]

use std::convert::TryFrom;
use std::fmt::{self, Debug};
use std::sync::atomic::{AtomicU64, Ordering};

const PRECISION: f64 = 100.;
const BUCKETS: usize = 1 << 16;

/// A histogram collector that uses zero-configuration logarithmic buckets.
pub struct Histogram {
    vals: Vec<AtomicU64>,
    sum: AtomicU64,
    count: AtomicU64,
}

impl Default for Histogram {
    fn default() -> Histogram {
        let mut vals = Vec::with_capacity(BUCKETS);
        vals.resize_with(BUCKETS, Default::default);

        Histogram {
            vals,
            sum: AtomicU64::new(0),
            count: AtomicU64::new(0),
        }
    }
}

#[allow(unsafe_code)]
unsafe impl Send for Histogram {}

impl Debug for Histogram {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        const PS: [f64; 10] = [0., 50., 75., 90., 95., 97.5, 99., 99.9, 99.99, 100.];
        f.write_str("Histogramgram[")?;

        for p in &PS {
            let res = self.percentile(*p).round();
            let line = format!("({} -> {}) ", p, res);
            f.write_str(&*line)?;
        }

        f.write_str("]")
    }
}

impl Histogram {
    /// Record a value.
    #[inline]
    pub fn measure<T: Copy + Into<f64>>(&self, raw_value: T) -> u64 {
        {
            let value_float: f64 = raw_value.into();
            self.sum
                .fetch_add(value_float.round() as u64, Ordering::Relaxed);

            self.count.fetch_add(1, Ordering::Relaxed);

            // compress the value to one of 2**16 values
            // using logarithmic bucketing
            let compressed: u16 = compress(value_float);

            // increment the counter for this compressed value
            self.vals[compressed as usize].fetch_add(1, Ordering::Relaxed) + 1
        }
    }

    /// Retrieve a percentile [0-100]. Returns NAN if no metrics have been
    /// collected yet.
    pub fn percentile(&self, p: f64) -> f64 {
        {
            assert!(p <= 100., "percentiles must not exceed 100.0");

            let count = self.count.load(Ordering::Acquire);

            if count == 0 {
                return std::f64::NAN;
            }

            let mut target = count as f64 * (p / 100.);
            if target == 0. {
                target = 1.;
            }

            let mut sum = 0.;

            for (idx, val) in self.vals.iter().enumerate() {
                let count = val.load(Ordering::Acquire);
                sum += count as f64;

                if sum >= target {
                    return decompress(idx as u16);
                }
            }
        }

        std::f64::NAN
    }

    /// Dump out some common percentiles.
    pub fn print_percentiles(&self) {
        println!("{:?}", self);
    }

    /// Return the sum of all observations in this histogram.
    pub fn sum(&self) -> u64 {
        self.sum.load(Ordering::Acquire)
    }

    /// Return the count of observations in this histogram.
    pub fn count(&self) -> u64 {
        self.count.load(Ordering::Acquire)
    }
}

// compress takes a value and lossily shrinks it to an u16 to facilitate
// bucketing of histogram values, staying roughly within 1% of the true
// value. This fails for large values of 1e142 and above, and is
// inaccurate for values closer to 0 than +/- 0.51 or +/- math.Inf.
#[allow(clippy::cast_sign_loss)]
#[allow(clippy::cast_possible_truncation)]
#[inline]
fn compress<T: Into<f64>>(input_value: T) -> u16 {
    let value: f64 = input_value.into();
    let abs = value.abs();
    let boosted = 1. + abs;
    let ln = boosted.ln();
    let compressed = PRECISION.mul_add(ln, 0.5);
    assert!(compressed <= f64::from(u16::max_value()));

    compressed as u16
}

// decompress takes a lossily shrunken u16 and returns an f64 within 1% of
// the original passed to compress.
#[inline]
fn decompress(compressed: u16) -> f64 {
    let unboosted = f64::from(compressed) / PRECISION;
    (unboosted.exp() - 1.)
}
