use blitzkv::database::{Database, DatabaseError};
use rand::prelude::*;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;
use std::time::Instant;
use tracing::{info, instrument};
use tracing_subscriber;

// 测试数据存放目录
const TEST_DATA_DIR: &str = "test_data";
const NUM_KEYS: usize = 50_000;
const TOTAL_OPS: usize = 100_000;
const UPDATE_RATIO: f64 = 0.15; // 固定15%
const WRITE_RATIO: f64 = 0.05; // 固定5%
const VALUE_SIZE: std::ops::Range<usize> = 600..1100;

#[derive(Serialize, Deserialize)]
struct BenchmarkResult {
    variant: String,    // "baseline" 或 "optimized"
    read_ratio: f64,    // 例如 0.6 或 0.8
    zipf: f64,          // 例如 1.1, 1.2, 1.3
    throughput: f64,    // ops/sec
    duration_secs: f64, // 运行时长,单位秒
    hit_ratio: f64,
    read_ssd_ops: u64,
    write_ssd_ops: u64,
}

// 保存测试操作的结构体
#[derive(Serialize, Deserialize)]
struct TestOperation {
    op_type: u8, // 0: read, 1: update, 2: write
    key_idx: usize,
    value: Option<Vec<u8>>, // 对于update和write操作需要新的value
}

#[derive(Serialize, Deserialize)]
struct TestData {
    keys: Vec<Vec<u8>>,
    values: Vec<Vec<u8>>,
    operations: Vec<TestOperation>,
}

impl TestData {
    fn save_to_file(&self, path: &std::path::Path) -> std::io::Result<()> {
        let json = serde_json::to_string(self)?;
        fs::write(path, json)
    }

    fn load_from_file(path: &std::path::Path) -> std::io::Result<Self> {
        let json = fs::read_to_string(path)?;
        Ok(serde_json::from_str(&json)?)
    }

    fn generate<R: Rng>(rng: &mut R, read_ratio: f64, zipf_param: f64) -> Self {
        // 1. 生成初始键值对
        let (keys, values) = generate_kv_pairs(rng, NUM_KEYS);

        // 2. 生成操作序列
        let zipf = zipf::ZipfDistribution::new(NUM_KEYS, zipf_param).unwrap();
        let mut operations = Vec::with_capacity(TOTAL_OPS);
        let mut current_key_id = NUM_KEYS;

        for _ in 0..TOTAL_OPS {
            let op = rng.gen::<f64>();
            let operation = if op < read_ratio {
                // 读操作
                TestOperation {
                    op_type: 0,
                    key_idx: zipf.sample(rng) - 1,
                    value: None,
                }
            } else if op < read_ratio + UPDATE_RATIO {
                // 更新操作
                TestOperation {
                    op_type: 1,
                    key_idx: zipf.sample(rng) - 1,
                    value: Some(generate_value(rng)),
                }
            } else {
                // 写入新键
                TestOperation {
                    op_type: 2,
                    key_idx: current_key_id,
                    value: Some(generate_value(rng)),
                }
            };

            if operation.op_type == 2 {
                current_key_id += 1;
            }
            operations.push(operation);
        }

        TestData {
            keys,
            values,
            operations,
        }
    }
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

fn get_test_data_path(read_ratio: f64, zipf_param: f64) -> PathBuf {
    PathBuf::from(TEST_DATA_DIR).join(format!(
        "test_data_r{:.0}_z{}.json",
        read_ratio * 100.0,
        zipf_param
    ))
}

/// 带参数的基准测试函数,返回一个 BenchmarkResult
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

    // 尝试加载或生成测试数据
    let test_data_path = get_test_data_path(read_ratio, zipf_param);
    let test_data = if test_data_path.exists() {
        info!("Loading test data from {:?}", test_data_path);
        TestData::load_from_file(&test_data_path).unwrap()
    } else {
        info!("Generating new test data...");
        let mut rng = rand::thread_rng();
        let data = TestData::generate(&mut rng, read_ratio, zipf_param);

        // 确保目录存在
        if let Some(parent) = test_data_path.parent() {
            fs::create_dir_all(parent).unwrap();
        }

        // 保存测试数据
        info!("Saving test data to {:?}", test_data_path);
        data.save_to_file(&test_data_path).unwrap();
        data
    };

    // 预填充数据库
    info!("Pre-populating database...");
    for (key, value) in test_data.keys.iter().zip(test_data.values.iter()) {
        db.set(key, value)?;
    }

    let mut values = test_data.values;
    let mut current_key_id = NUM_KEYS;
    let mut op_counts = [0; 3]; // [read, update, write]

    // 运行基准测试
    info!("Starting benchmark ({} operations)...", TOTAL_OPS);
    let start_time = Instant::now();

    for op in test_data.operations.iter() {
        match op.op_type {
            0 => {
                // 读操作
                let stored = db.get(&test_data.keys[op.key_idx])?;
                assert_eq!(
                    stored,
                    values[op.key_idx],
                    "Data mismatch for key {}",
                    String::from_utf8_lossy(&test_data.keys[op.key_idx])
                );
                op_counts[0] += 1;
            }
            1 => {
                // 更新操作
                let new_value = op.value.as_ref().unwrap();
                db.set(&test_data.keys[op.key_idx], new_value)?;
                values[op.key_idx] = new_value.clone();
                op_counts[1] += 1;
            }
            2 => {
                // 写入新键
                let new_key = format!("key_{:08}", current_key_id).into_bytes();
                let new_value = op.value.as_ref().unwrap();
                db.set(&new_key, new_value)?;
                current_key_id += 1;
                op_counts[2] += 1;
            }
            _ => unreachable!(),
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
    // remove test_data dir
    std::fs::remove_dir_all(TEST_DATA_DIR).unwrap();
    std::fs::create_dir_all(&data_dir).unwrap();
    std::fs::create_dir_all(TEST_DATA_DIR).unwrap();

    let read_ratios = vec![0.7, 0.8];
    let zipf_params = vec![1.1, 1.2];
    let variants = vec![("optimized", 2), ("baseline", 300)];

    let mut all_results = Vec::new();

    // 遍历每个配置
    for &(variant_name, hot_threshold) in &variants {
        for &read_ratio in &read_ratios {
            for &zipf_param in &zipf_params {
                // 为每个实验构造不同的数据库文件路径,避免冲突
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

    // 输出为 JSON 文件
    let json = serde_json::to_string_pretty(&all_results).unwrap();
    std::fs::write("results.json", json).unwrap();
    info!("All benchmark results written to results.json");

    Ok(())
}
