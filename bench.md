2025-02-05T06:06:34.246502Z  INFO new{page_size=4096}: blitzkv::storage::device: Creating new SsdDevice with page_size: 4096
2025-02-05T06:06:34.246626Z  INFO run_benchmark: blitzkv: Generating 10000 key-value pairs...
2025-02-05T06:06:34.305841Z  INFO run_benchmark: blitzkv: Pre-populating database...
2025-02-05T06:06:34.719304Z  INFO run_benchmark: blitzkv: Starting benchmark (100000 operations)...
2025-02-05T06:06:37.415668Z  INFO run_benchmark: blitzkv: Benchmark completed in 2.70s
2025-02-05T06:06:37.415690Z  INFO run_benchmark: blitzkv: Throughput: 37087.26 ops/sec
2025-02-05T06:06:37.415698Z  INFO run_benchmark: blitzkv: Operation distribution:
2025-02-05T06:06:37.415702Z  INFO run_benchmark: blitzkv:   Reads: 80032 (80.0%)
2025-02-05T06:06:37.415706Z  INFO run_benchmark: blitzkv:   Updates: 14983 (15.0%)
2025-02-05T06:06:37.415712Z  INFO run_benchmark: blitzkv:   Writes: 4985 (5.0%)
2025-02-05T06:06:37.415716Z  INFO run_benchmark: blitzkv: Zipf parameter: s=1.2
2025-02-05T06:06:37.415723Z  INFO run_benchmark: blitzkv: Total unique keys: 14985
2025-02-05T06:06:37.422930Z  INFO blitzkv::storage::device: Dropping SsdDevice with metrics:
SsdMetrics:
  Reads: 18820
  Writes: 29968
  Read Bytes: 77086720
  Write Bytes: 122748928
  Read Latency (μs):
    p50: 40.16
    p95: 87.42
    p99: 94.02
    max: 858.11
  Write Latency (μs):
    p50: 26.57
    p95: 32.59
    p99: 42.40
    max: 318.46

2025-02-05T06:07:19.004173Z  INFO new{page_size=4096}: blitzkv::storage::device: Creating new SsdDevice with page_size: 4096
2025-02-05T06:07:19.004307Z  INFO run_benchmark: blitzkv: Generating 10000 key-value pairs...
2025-02-05T06:07:19.063878Z  INFO run_benchmark: blitzkv: Pre-populating database...
2025-02-05T06:07:19.501822Z  INFO run_benchmark: blitzkv: Starting benchmark (100000 operations)...
2025-02-05T06:07:22.143256Z  INFO run_benchmark: blitzkv: Benchmark completed in 2.64s
2025-02-05T06:07:22.143285Z  INFO run_benchmark: blitzkv: Throughput: 37858.59 ops/sec
2025-02-05T06:07:22.143294Z  INFO run_benchmark: blitzkv: Operation distribution:
2025-02-05T06:07:22.143300Z  INFO run_benchmark: blitzkv:   Reads: 80084 (80.1%)
2025-02-05T06:07:22.143306Z  INFO run_benchmark: blitzkv:   Updates: 14960 (15.0%)
2025-02-05T06:07:22.143311Z  INFO run_benchmark: blitzkv:   Writes: 4956 (5.0%)
2025-02-05T06:07:22.143316Z  INFO run_benchmark: blitzkv: Zipf parameter: s=1.2
2025-02-05T06:07:22.143324Z  INFO run_benchmark: blitzkv: Total unique keys: 14956
2025-02-05T06:07:22.149612Z  INFO blitzkv::storage::device: Dropping SsdDevice with metrics:
SsdMetrics:
  Reads: 21883
  Writes: 29916
  Read Bytes: 89632768
  Write Bytes: 122535936
  Read Latency (μs):
    p50: 39.90
    p95: 87.10
    p99: 95.30
    max: 615.42
  Write Latency (μs):
    p50: 26.40
    p95: 32.59
    p99: 41.95
    max: 373.25