#![allow(unused_results)]
#![allow(clippy::print_stdout)]

#[cfg(not(target_arch = "x86_64"))]
use std::time::{Duration, Instant};

use crate::storage::lazy::Lazy;

use super::histogram::Histogram;

/// A metric collector for all pagecache users running in this
/// process.
pub static M: Lazy<Metrics, fn() -> Metrics> = Lazy::new(Metrics::default);

#[allow(clippy::cast_precision_loss)]
pub(crate) fn clock() -> f64 {
    #[cfg(target_arch = "x86_64")]
    #[allow(unsafe_code)]
    unsafe {
        let mut aux = 0;
        core::arch::x86_64::__rdtscp(&mut aux) as f64
    }

    #[cfg(not(target_arch = "x86_64"))]
    {
        let u = uptime();
        (u.as_secs() * 1_000_000_000) as f64 + f64::from(u.subsec_nanos())
    }
}

// not correct, since it starts counting at the first observance...
#[cfg(not(target_arch = "x86_64"))]
pub(crate) fn uptime() -> Duration {
    static START: Lazy<Instant, fn() -> Instant> = Lazy::new(Instant::now);

    if cfg!(feature = "no_metrics") {
        Duration::new(0, 0)
    } else {
        START.elapsed()
    }
}

/// Measure the duration of an event, and call `Histogram::measure()`.
pub struct Measure<'h> {
    _start: f64,
    histo: &'h Histogram,
}

impl<'h> Measure<'h> {
    /// The time delta from ctor to dtor is recorded in `histo`.
    #[inline]
    pub fn new(_histo: &'h Histogram) -> Measure<'h> {
        Measure {
            histo: _histo,
            _start: clock(),
        }
    }
}

impl<'h> Drop for Measure<'h> {
    #[inline]
    fn drop(&mut self) {
        self.histo.measure(clock() - self._start);
    }
}

#[derive(Default, Debug)]
pub struct Metrics {
    pub sq_mu_wait: Histogram,
    pub sq_mu_hold: Histogram,
    pub cq_mu_wait: Histogram,
    pub cq_mu_hold: Histogram,
    pub enter_cqe: Histogram,
    pub enter_sqe: Histogram,
    pub get_sqe: Histogram,
    pub reap_ready: Histogram,
    pub wait: Histogram,
    pub ticket_queue_push: Histogram,
    pub ticket_queue_pop: Histogram,
}

impl Drop for Metrics {
    fn drop(&mut self) {
        self.print_profile()
    }
}

impl Metrics {
    pub fn print_profile(&self) {
        println!(
            "rio profile:\n\
             {0: >17} | {1: >10} | {2: >10} | {3: >10} | {4: >10} | {5: >10} | {6: >10} | {7: >10} | {8: >10} | {9: >10}",
            "op",
            "min (us)",
            "med (us)",
            "90 (us)",
            "99 (us)",
            "99.9 (us)",
            "99.99 (us)",
            "max (us)",
            "count",
            "sum (s)"
        );
        println!("{}", std::iter::repeat("-").take(134).collect::<String>());

        let p = |mut tuples: Vec<(String, _, _, _, _, _, _, _, _, _)>| {
            tuples.sort_by_key(|t| (t.9 * -1. * 1e3) as i64);
            for v in tuples {
                println!(
                    "{0: >17} | {1: >10.1} | {2: >10.1} | {3: >10.1} \
                     | {4: >10.1} | {5: >10.1} | {6: >10.1} | {7: >10.1} \
                     | {8: >10.1} | {9: >10.3}",
                    v.0, v.1, v.2, v.3, v.4, v.5, v.6, v.7, v.8, v.9,
                );
            }
        };

        let lat = |name: &str, histo: &Histogram| {
            (
                name.to_string(),
                histo.percentile(0.) / 1e3,
                histo.percentile(50.) / 1e3,
                histo.percentile(90.) / 1e3,
                histo.percentile(99.) / 1e3,
                histo.percentile(99.9) / 1e3,
                histo.percentile(99.99) / 1e3,
                histo.percentile(100.) / 1e3,
                histo.count(),
                histo.sum() as f64 / 1e9,
            )
        };

        println!("sq:");
        p(vec![
            lat("sq_mu_wait", &self.sq_mu_wait),
            lat("sq_mu_hold", &self.sq_mu_hold),
            lat("enter sqe", &self.enter_sqe),
            lat("ticket q pop", &self.ticket_queue_pop),
        ]);

        println!("{}", std::iter::repeat("-").take(134).collect::<String>());
        println!("cq:");
        p(vec![
            lat("cq_mu_wait", &self.cq_mu_wait),
            lat("cq_mu_hold", &self.cq_mu_hold),
            lat("enter cqe", &self.enter_cqe),
            lat("ticket q push", &self.ticket_queue_push),
        ]);

        println!("{}", std::iter::repeat("-").take(134).collect::<String>());
        println!("reaping and waiting:");
        p(vec![
            lat("reap_ready", &self.reap_ready),
            lat("wait", &self.wait),
        ]);

        println!("{}", std::iter::repeat("-").take(134).collect::<String>());
    }
}
