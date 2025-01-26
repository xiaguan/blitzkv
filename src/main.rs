use blitzkv::database::{Database, DatabaseError};
use rand::prelude::*;
use std::path::PathBuf;
use std::time::Instant;
use tracing::{error, info, instrument};
use tracing_subscriber;

const NUM_KEYS: usize = 10_000;
const TOTAL_OPS: usize = 100_000;
const WRITE_RATIO: f64 = 0.1; // 10% 写操作
const ZIPF_S: f64 = 1.01; // Zipf 分布参数

fn generate_kv_pairs<R: Rng>(rng: &mut R, num_keys: usize) -> (Vec<Vec<u8>>, Vec<Vec<u8>>) {
    let mut keys = Vec::with_capacity(num_keys);
    let mut values = Vec::with_capacity(num_keys);

    for i in 0..num_keys {
        // 生成键 (key_0000 格式)
        keys.push(format!("key_{:04}", i).into_bytes());

        // 生成值 (600-1100 字节随机数据)
        let value_len = rng.gen_range(600..=1100);
        let value: Vec<u8> = (0..value_len).map(|_| rng.gen()).collect();
        values.push(value);
    }

    (keys, values)
}

#[instrument]
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

    // 4. 运行基准测试
    info!("Starting benchmark ({} operations)...", TOTAL_OPS);
    let mut rng = rand::thread_rng();
    let mut zipf = zipf::ZipfDistribution::new(NUM_KEYS, 1.03).unwrap();

    let start_time = Instant::now();

    for _ in 0..TOTAL_OPS {
        let is_write = rng.gen_bool(WRITE_RATIO);
        let idx = zipf.sample(&mut rng) - 1;

        if is_write {
            // 写入操作：生成新值
            let new_len = rng.gen_range(600..=1100);
            let new_value: Vec<u8> = (0..new_len).map(|_| rng.gen()).collect();
            db.set(&keys[idx], &new_value)?;
            values[idx] = new_value;
        } else {
            // 读取操作：验证数据一致性
            let stored = db.get(&keys[idx])?;
            assert_eq!(
                stored,
                values[idx],
                "Data mismatch for key {}",
                String::from_utf8_lossy(&keys[idx])
            );
        }
    }

    let duration = start_time.elapsed();

    // 5. 输出结果
    info!("Benchmark completed in {:.2?}", duration);
    info!(
        "Throughput: {:.2} ops/sec",
        TOTAL_OPS as f64 / duration.as_secs_f64()
    );
    info!("Write ratio: {:.1}%", WRITE_RATIO * 100.0);
    info!("Zipf parameter: s={}", ZIPF_S);

    Ok(())
}

fn main() {
    // 初始化日志
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    // 初始化数据库
    let data_dir = PathBuf::from("data");
    if let Err(e) = std::fs::create_dir_all(&data_dir) {
        error!("Failed to create data directory: {}", e);
        std::process::exit(1);
    }

    let mut db = match Database::new(data_dir.join("bench.db")) {
        Ok(db) => db,
        Err(e) => {
            error!("Failed to create database");
            std::process::exit(1);
        }
    };

    // 运行基准测试
    if let Err(e) = run_benchmark(&mut db) {
        error!("Benchmark failed");
        std::process::exit(1);
    }
}
