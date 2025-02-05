use blitzkv::database::{Database, DatabaseError};
use rand::prelude::*;
use serde::Serialize;
use std::path::PathBuf;
use std::time::Instant;
use tracing::{info, instrument};
use tracing_subscriber;

const NUM_KEYS: usize = 10_000;
const TOTAL_OPS: usize = 100_000;
const UPDATE_RATIO: f64 = 0.15; // 固定15%
const WRITE_RATIO: f64 = 0.05; // 固定5%
const VALUE_SIZE: std::ops::Range<usize> = 600..1100;

#[derive(Serialize)]
struct BenchmarkResult {
    variant: String,    // "baseline" 或 "optimized"
    read_ratio: f64,    // 例如 0.6 或 0.8
    zipf: f64,          // 例如 1.1, 1.2, 1.3
    throughput: f64,    // ops/sec
    duration_secs: f64, // 运行时长，单位秒
    hit_ratio: f64,
    read_ssd_ops: u64,
    write_ssd_ops: u64,
}

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

/// 带参数的基准测试函数，返回一个 BenchmarkResult
#[instrument(skip(db))]
fn run_benchmark_with_params(
    db: &mut Database,
    read_ratio: f64,
    zipf_param: f64,
    variant: &str,
) -> Result<BenchmarkResult, DatabaseError> {
    info!(
        "Starting benchmark with read_ratio={:.2}, zipf={:.2}, variant={}",
        read_ratio, zipf_param, variant
    );
    let mut rng = rand::thread_rng();

    // 1. 生成测试数据
    info!("Generating {} key-value pairs...", NUM_KEYS);
    let (keys, mut values) = generate_kv_pairs(&mut rng, NUM_KEYS);

    // 2. 预填充数据库
    info!("Pre-populating database...");
    for (key, value) in keys.iter().zip(values.iter()) {
        db.set(key, value)?;
    }

    // 3. 初始化 Zipf 分布和统计数据
    let zipf = zipf::ZipfDistribution::new(NUM_KEYS, zipf_param).unwrap();
    let mut current_key_id = NUM_KEYS;
    let mut op_counts = [0; 3]; // [read, update, write]

    // 4. 运行基准测试
    info!("Starting benchmark ({} operations)...", TOTAL_OPS);
    let start_time = Instant::now();

    for _ in 0..TOTAL_OPS {
        let op = rng.gen::<f64>();

        if op < read_ratio {
            let idx = zipf.sample(&mut rng) - 1;
            let stored = db.get(&keys[idx])?;
            assert_eq!(
                stored,
                values[idx],
                "Data mismatch for key {}",
                String::from_utf8_lossy(&keys[idx])
            );
            op_counts[0] += 1;
        } else if op < read_ratio + UPDATE_RATIO {
            let idx = zipf.sample(&mut rng) - 1;
            let new_value = generate_value(&mut rng);
            db.set(&keys[idx], &new_value)?;
            values[idx] = new_value;
            op_counts[1] += 1;
        } else {
            let new_key = format!("key_{:08}", current_key_id).into_bytes();
            let new_value = generate_value(&mut rng);
            db.set(&new_key, &new_value)?;
            current_key_id += 1;
            op_counts[2] += 1;
        }
    }

    let duration = start_time.elapsed();
    let throughput = TOTAL_OPS as f64 / duration.as_secs_f64();

    info!("Benchmark completed in {:.2?}", duration);
    info!("Throughput: {:.2} ops/sec", throughput);
    info!(
        "Operation distribution: reads={}, updates={}, writes={}",
        op_counts[0], op_counts[1], op_counts[2]
    );
    info!("Total unique keys: {}", current_key_id);

    let hit_ratio = db.hit_ratio();
    let ssd_metrics = db.metrics();
    // 返回结构化结果
    Ok(BenchmarkResult {
        variant: variant.to_string(),
        read_ratio,
        zipf: zipf_param,
        throughput,
        duration_secs: duration.as_secs_f64(),
        hit_ratio,
        read_ssd_ops: ssd_metrics.reads(),
        write_ssd_ops: ssd_metrics.writes(),
    })
}

fn main() -> Result<(), DatabaseError> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let data_dir = PathBuf::from("data");
    std::fs::create_dir_all(&data_dir).unwrap();

    let read_ratios = vec![0.8, 0.7];
    let zipf_params = vec![1.1, 1.3];
    let variants = vec![("optimized", 2), ("baseline", 200)];

    let mut all_results = Vec::new();

    // 遍历每个配置
    for &(variant_name, hot_threshold) in &variants {
        for &read_ratio in &read_ratios {
            for &zipf_param in &zipf_params {
                // 为每个实验构造不同的数据库文件路径，避免冲突
                let db_path = data_dir.join(format!(
                    "bench_{}_r{:.0}_z{}.db",
                    variant_name,
                    read_ratio * 100.0,
                    zipf_param
                ));
                info!(
                    "Running {} with read_ratio={} zipf={} (db: {:?})",
                    variant_name, read_ratio, zipf_param, db_path
                );
                let mut db = Database::new(db_path, hot_threshold)?;
                let result =
                    run_benchmark_with_params(&mut db, read_ratio, zipf_param, variant_name)?;
                all_results.push(result);
            }
        }
    }

    // 输出为 JSON 文件（也可以改成 CSV）
    let json = serde_json::to_string_pretty(&all_results).unwrap();
    std::fs::write("results.json", json).unwrap();
    info!("All benchmark results written to results.json");

    Ok(())
}
