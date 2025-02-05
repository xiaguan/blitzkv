use blitzkv::database::{Database, DatabaseError};
use rand::prelude::*;
use std::path::PathBuf;
use std::time::Instant;
use tracing::{error, info, instrument};
use tracing_subscriber;

const NUM_KEYS: usize = 10_000;
const TOTAL_OPS: usize = 100_000;
const READ_RATIO: f64 = 0.8; // 80% 读操作
const UPDATE_RATIO: f64 = 0.15; // 15% 更新操作
const WRITE_RATIO: f64 = 0.05; // 5% 写入操作
const ZIPF_S: f64 = 1.2; // Zipf 分布参数
const VALUE_SIZE: std::ops::Range<usize> = 600..1100;

fn generate_kv_pairs<R: Rng>(rng: &mut R, num_keys: usize) -> (Vec<Vec<u8>>, Vec<Vec<u8>>) {
    let mut keys = Vec::with_capacity(num_keys);
    let mut values = Vec::with_capacity(num_keys);

    for i in 0..num_keys {
        keys.push(format!("key_{:08}", i).into_bytes());
        values.push(generate_value(rng));
    }

    (keys, values)
}

fn generate_value<R: Rng>(rng: &mut R) -> Vec<u8> {
    let value_len = rng.gen_range(VALUE_SIZE);
    (0..value_len).map(|_| rng.gen()).collect()
}

#[instrument(skip(db))]
fn run_benchmark(db: &mut Database) -> Result<(), DatabaseError> {
    let mut rng = rand::thread_rng();

    // 1. 生成测试数据
    info!("Generating {} key-value pairs...", NUM_KEYS);
    let (keys, mut values) = generate_kv_pairs(&mut rng, NUM_KEYS);

    // 2. 预填充数据库
    info!("Pre-populating database...");
    for (key, value) in keys.iter().zip(values.iter()) {
        db.set(key, value)?;
    }

    // 3. 初始化分布和统计
    let zipf = zipf::ZipfDistribution::new(NUM_KEYS, ZIPF_S).unwrap();
    let mut current_key_id = NUM_KEYS;
    let mut op_counts = [0; 3]; // [read, update, write]

    // 4. 运行基准测试
    info!("Starting benchmark ({} operations)...", TOTAL_OPS);
    let start_time = Instant::now();

    for _ in 0..TOTAL_OPS {
        let op = rng.gen::<f64>();

        match op {
            p if p < READ_RATIO => {
                let idx = zipf.sample(&mut rng) - 1;
                let stored = db.get(&keys[idx])?;
                assert_eq!(
                    stored,
                    values[idx],
                    "Data mismatch for key {}",
                    String::from_utf8_lossy(&keys[idx])
                );
                op_counts[0] += 1;
            }
            p if p < READ_RATIO + UPDATE_RATIO => {
                let idx = zipf.sample(&mut rng) - 1;
                let new_value = generate_value(&mut rng);
                db.set(&keys[idx], &new_value)?;
                values[idx] = new_value;
                op_counts[1] += 1;
            }
            _ => {
                let new_key = format!("key_{:08}", current_key_id).into_bytes();
                let new_value = generate_value(&mut rng);
                db.set(&new_key, &new_value)?;
                current_key_id += 1;
                op_counts[2] += 1;
            }
        }
    }

    let duration = start_time.elapsed();

    info!("Benchmark completed in {:.2?}", duration);
    info!(
        "Throughput: {:.2} ops/sec",
        TOTAL_OPS as f64 / duration.as_secs_f64()
    );
    info!("Operation distribution:");
    info!(
        "  Reads: {} ({:.1}%)",
        op_counts[0],
        op_counts[0] as f64 / TOTAL_OPS as f64 * 100.0
    );
    info!(
        "  Updates: {} ({:.1}%)",
        op_counts[1],
        op_counts[1] as f64 / TOTAL_OPS as f64 * 100.0
    );
    info!(
        "  Writes: {} ({:.1}%)",
        op_counts[2],
        op_counts[2] as f64 / TOTAL_OPS as f64 * 100.0
    );
    info!("Zipf parameter: s={}", ZIPF_S);
    info!("Total unique keys: {}", current_key_id);

    Ok(())
}

fn main() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let data_dir = PathBuf::from("data");
    if let Err(e) = std::fs::create_dir_all(&data_dir) {
        error!("Failed to create data directory: {}", e);
        std::process::exit(1);
    }

    let mut db = Database::new(data_dir.join("bench.db")).unwrap();

    run_benchmark(&mut db).unwrap();
}
