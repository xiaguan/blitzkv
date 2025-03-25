use blitzkv::database::{Database, DatabaseError};
use indicatif::{ProgressBar, ProgressStyle};
use serde::{Deserialize, Serialize};
use std::hash::Hasher;
use std::io::BufRead;
use std::path::PathBuf;
use std::time::Instant;
use std::{
    fs,
    hash::Hash,
    time::{SystemTime, UNIX_EPOCH},
};
use tracing::{info, instrument};
use tracing_subscriber;

// Operation types from trace
const GET_TEMP: u8 = 1;
const GET_PERM: u8 = 2;
const PUT_TEMP: u8 = 3;
const PUT_PERM: u8 = 4;
const GET_NOT_INIT: u8 = 5;
const PUT_NOT_INIT: u8 = 6;
const UNKNOWN: u8 = 100;

const PUT_OPS: [u8; 3] = [PUT_PERM, PUT_TEMP, PUT_NOT_INIT];
const GET_OPS: [u8; 3] = [GET_PERM, GET_TEMP, GET_NOT_INIT];

// Structure to store trace records
#[derive(Debug)]
struct TraceRecord {
    block_id: u64,
    io_offset: u64,
    io_size: u64,
    op_time: u64,
    op_name: u8,
    user_namespace: u64,
    user_name: u64,
    rs_shard_id: u64,
    op_count: u64,
    host_name: u64,
}

#[derive(Serialize, Deserialize)]
struct BenchmarkResult {
    variant: String,
    throughput: f64,    // ops/sec
    duration_secs: f64, // runtime in seconds
    hit_ratio: f64,
    read_ssd_ops: u64,
    write_ssd_ops: u64,
    freq_p50: f64,
    freq_p95: f64,
    freq_p99: f64,
    freq_max: f64,
}

// Structure to store test operations
#[derive(Serialize, Deserialize)]
struct TestOperation {
    op_type: u8,    // Operation type from trace
    key: Vec<u8>,   // Key derived from block_id
    value: Vec<u8>, // Value sized according to io_size
}

#[derive(Serialize, Deserialize)]
struct TestData {
    operations: Vec<TestOperation>,
}

impl TestData {
    fn load_from_trace(path: &std::path::Path) -> std::io::Result<Self> {
        let file = fs::File::open(path)?;
        let reader = std::io::BufReader::new(file);
        let mut operations = Vec::new();

        for line in reader.lines() {
            let line = line?;
            if line.starts_with('#') {
                continue;
            }

            let fields: Vec<&str> = line.split_whitespace().collect();
            if fields.len() != 10 {
                continue;
            }

            let record = TraceRecord {
                block_id: fields[0].parse().unwrap(),
                io_offset: fields[1].parse().unwrap(),
                io_size: fields[2].parse().unwrap(),
                op_time: fields[3].parse().unwrap(),
                op_name: fields[4].parse().unwrap(),
                user_namespace: fields[5].parse().unwrap(),
                user_name: fields[6].parse().unwrap(),
                rs_shard_id: fields[7].parse().unwrap(),
                op_count: fields[8].parse().unwrap(),
                host_name: fields[9].parse().unwrap(),
            };

            // Generate value with random size
            let size: usize = record.io_size as usize % 400 + 620;
            let mut value = Vec::with_capacity(size);
            value.extend(std::iter::repeat(0u8).take(size));

            // Hash the block_id using DefaultHasher
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            record.block_id.hash(&mut hasher);
            let key = hasher.finish() % 5000;

            operations.push(TestOperation {
                op_type: record.op_name,
                key: key.to_string().into_bytes(),
                value,
            });
        }

        Ok(TestData { operations })
    }
}

#[instrument(skip(db))]
fn run_benchmark_with_params(
    db: &mut Database,
    variant: &str,
) -> Result<BenchmarkResult, DatabaseError> {
    info!("Starting benchmark with variant={}", variant);

    // Load trace data
    let test_data = TestData::load_from_trace(std::path::Path::new("trace.csv")).unwrap();
    let total_ops = test_data.operations.len();

    let mut op_counts = [0u64; 7]; // Counts for each operation type (1-6 + unknown)

    // Run benchmark with progress bar
    info!("Starting benchmark ({} operations)...", total_ops);
    let pb = ProgressBar::new(total_ops as u64);
    pb.set_style(ProgressStyle::default_bar()
        .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({per_sec} ops/sec)")
        .unwrap()
        .progress_chars("#>-"));

    let start_time = Instant::now();

    for op in test_data.operations.iter() {
        pb.inc(1);

        if op.op_type <= 6 {
            op_counts[op.op_type as usize - 1] += 1;
        } else {
            op_counts[6] += 1; // Unknown operations
        }

        match op.op_type {
            // GET operations
            GET_TEMP | GET_PERM | GET_NOT_INIT => {
                let _ = db.get(&op.key);
            }
            // PUT operations
            PUT_TEMP | PUT_PERM | PUT_NOT_INIT => {
                db.set(&op.key, &op.value)?;
            }
            // Unknown operations
            _ => {
                info!("Unknown operation type: {}", op.op_type);
            }
        }
    }

    let duration = start_time.elapsed();
    let throughput = total_ops as f64 / duration.as_secs_f64();

    pb.finish_with_message(format!("Benchmark completed in {:.2?}", duration));
    info!("Throughput: {:.2} ops/sec", throughput);
    info!(
        "Operation distribution: GET_TEMP={}, GET_PERM={}, PUT_TEMP={}, PUT_PERM={}, GET_NOT_INIT={}, PUT_NOT_INIT={}, UNKNOWN={}",
        op_counts[0], op_counts[1], op_counts[2], op_counts[3], op_counts[4], op_counts[5], op_counts[6]
    );

    let hit_ratio = db.hit_ratio();
    let ssd_metrics = db.metrics();
    let freq_hist = db.freq_histogram();

    info!("Access Frequency Statistics:");
    info!("  p50: {:.2}", freq_hist.value_at_percentile(50.0) as f64);
    info!("  p95: {:.2}", freq_hist.value_at_percentile(95.0) as f64);
    info!("  p99: {:.2}", freq_hist.value_at_percentile(99.0) as f64);
    info!("  max: {:.2}", freq_hist.max() as f64);

    Ok(BenchmarkResult {
        variant: variant.to_string(),
        throughput,
        duration_secs: duration.as_secs_f64(),
        hit_ratio,
        read_ssd_ops: ssd_metrics.reads(),
        write_ssd_ops: ssd_metrics.writes(),
        freq_p50: freq_hist.value_at_percentile(50.0) as f64,
        freq_p95: freq_hist.value_at_percentile(95.0) as f64,
        freq_p99: freq_hist.value_at_percentile(99.0) as f64,
        freq_max: freq_hist.max() as f64,
    })
}

fn main() -> Result<(), DatabaseError> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let data_dir = PathBuf::from("data");
    std::fs::create_dir_all(&data_dir).unwrap();

    let variants = vec![("optimized", 3), ("baseline", 40000)];
    let mut all_results = Vec::new();

    // Run benchmark for each variant
    for &(variant_name, hot_threshold) in &variants {
        let db_path = data_dir.join(format!("bench_{}.db", variant_name));
        info!("Running {} (db: {:?})", variant_name, db_path);
        let mut db = Database::new(db_path, hot_threshold)?;
        let result = run_benchmark_with_params(&mut db, variant_name)?;
        all_results.push(result);
    }

    // Output as JSON file
    let json = serde_json::to_string_pretty(&all_results).unwrap();
    std::fs::write("results.json", json).unwrap();
    info!("All benchmark results written to results.json");

    // Export detailed metrics for visualization
    for &(variant_name, hot_threshold) in &variants {
        let db_path = data_dir.join(format!("bench_{}.db", variant_name));
        info!(
            "Exporting detailed metrics for {} (db: {:?})",
            variant_name, db_path
        );
        let mut db = Database::new(db_path, hot_threshold)?;

        // Run a small benchmark to populate metrics
        let _ = run_benchmark_with_params(&mut db, variant_name)?;

        // Export metrics to JSON file
        let metrics_json = serde_json::to_string_pretty(&db.export_metrics()).unwrap();
        let output_path = format!("{}_vis.json", variant_name);
        std::fs::write(&output_path, metrics_json).unwrap();
        info!("Detailed metrics written to {}", output_path);
    }

    Ok(())
}
