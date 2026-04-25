# Spark Taxi Analysis (Big Data)

[![Python](https://img.shields.io/badge/Python-3.x-blue.svg)](https://www.python.org/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.x-orange.svg)](https://spark.apache.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

High-performance taxi trip analytics project built with Apache Spark to compare **RDD**, **DataFrame**, and **Spark SQL** APIs across equivalent workloads and optimization scenarios.

## 1) Problem Statement

Urban mobility datasets are large, schema-rich, and expensive to process with naive approaches. The core challenge is to answer analytical questions efficiently while understanding how implementation style affects performance.

This project investigates:

- How API choice (RDD vs DataFrame vs SQL) impacts execution efficiency.
- How Spark-level optimizations (broadcast joins, caching, partitioning) influence runtime.
- How data format choices (CSV vs Parquet) affect throughput and scalability.

## 2) Objectives

- Implement a common analytics workload over NYC taxi trip data.
- Benchmark and compare multiple Spark programming abstractions.
- Demonstrate optimization techniques using reproducible scripts.
- Document findings with execution evidence and screenshots.

## 3) Dataset

- **Domain:** NYC Taxi trip records
- **Scale:** ~1M rows (sampled project dataset)
- **Formats:** CSV and Parquet
- **Schema:** 19 mixed-type fields (timestamps, categorical, numeric)

## 4) Repository Structure

```text
project/
├── data/                # Input dataset (CSV + Parquet)
├── dataframe/           # q1..q10 implementations using DataFrame API
├── rdd/                 # q1..q10 implementations using RDD API
├── sql/                 # q1..q10 implementations using Spark SQL
├── optimization/        # Join, caching, and partitioning experiments
├── scripts/             # Cluster config and batch execution script
└── results/screenshots/ # Execution evidence
```

## 5) Environment Requirements

- Python 3.x
- Apache Spark 3.x
- Linux/macOS shell (recommended)

## 6) Setup

```bash
# from project/ directory
chmod +x scripts/run_all.sh scripts/cluster_config.sh
source scripts/cluster_config.sh
```

> Note: Some scripts may contain machine-specific absolute paths. If needed, replace them with your local workspace path before execution.

## 7) Running the Project

Run all jobs:

```bash
bash scripts/run_all.sh
```

Run one job manually:

```bash
spark-submit dataframe/q1_df.py
```

## 8) Analytical Workload (Q1–Q10)

Representative tasks include:

- Daily trip volume aggregation
- Conditional filtering over trip/fare thresholds
- Daily average fare computation
- Grouped distribution by passenger/payment classes
- Window-based moving averages
- Frequency ranking of pickup locations
- Above-global-average fare filtering
- Join strategy comparison (Sort-Merge vs Broadcast)
- Caching impact on repeated actions
- Partition-count impact on count workloads

## 9) Optimization Experiments

Implemented under `optimization/`:

- **Join optimization:** Sort-Merge Join vs Broadcast Join
- **Caching test:** repeated action with and without cache
- **Partitioning test:** runtime sensitivity to partition counts (2, 4, 8)

## 10) Key Insights

- DataFrame/SQL APIs generally outperform raw RDD pipelines due to Catalyst and Tungsten optimizations.
- Broadcast join minimizes shuffle cost when joining against a small side table.
- Caching significantly improves repeated actions over the same dataset.
- Better partition sizing improves cluster resource utilization and parallelism.

## 11) Results Gallery

Execution screenshots are available in `results/screenshots/`.

| Sample Result | Screenshot |
|---|---|
| Query/plan output 1 | ![Result 1](results/screenshots/Screenshot%202026-04-23%20233028.png) |
| Query/plan output 2 | ![Result 2](results/screenshots/Screenshot%202026-04-23%20235906.png) |
| Query/plan output 3 | ![Result 3](results/screenshots/Screenshot%202026-04-23%20235930.png) |
| Query/plan output 4 | ![Result 4](results/screenshots/Screenshot%202026-04-23%20235946.png) |
| Query/plan output 5 | ![Result 5](results/screenshots/Screenshot%202026-04-24%20010001.png) |
| Query/plan output 6 | ![Result 6](results/screenshots/Screenshot%202026-04-24%20010011.png) |
| Query/plan output 7 | ![Result 7](results/screenshots/Screenshot%202026-04-24%20010023.png) |
| Query/plan output 8 | ![Result 8](results/screenshots/Screenshot%202026-04-24%20010050.png) |

## 12) Reproducibility Notes

- Keep Spark and Python versions stable across runs.
- Warm-up effects and caching state can influence timing results.
- For fair comparisons, run workloads multiple times and report mean runtime.

## 13) Contributing

Contributions are welcome. Please read [CONTRIBUTING.md](CONTRIBUTING.md) before opening a pull request.

## 14) License

This project is licensed under the MIT License. See [LICENSE](LICENSE).
