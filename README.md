# Kafka Partitioner Benchmark

[![Go Version](https://img.shields.io/badge/Go-1.21+-00ADD8?style=flat&logo=go)](https://golang.org)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

Benchmark tool for testing and comparing hash algorithm performance in Kafka message partitioning. Measures distribution quality, throughput, and collision rates across different partitioning strategies.

## 🎯 Objective

This project aims to identify the best hash algorithm for message partitioning in Apache Kafka, considering:

- **Performance**: Average processing time per message
- **Distribution**: Uniformity in distribution across partitions
- **Consistency**: Stability and predictability of partitioning
- **Variability**: Standard deviation and coefficient of variation

## 🚀 Tested Algorithms

| Algorithm | Strategy | Description |
|-----------|----------|-------------|
| **CRC32** | Direct hash + modulo | CRC32 IEEE checksum with modulo operation |
| **CRC32 Random** | Hash as seed | CRC32 as seed for deterministic random |
| **Murmur2** | Direct hash + modulo | Implementation compatible with Kafka Java |
| **Murmur2 Random** | Hash as seed | Murmur2 as seed for deterministic random |
| **FNV-1a** | Direct hash + modulo | 32-bit FNV-1a (Fowler-Noll-Vo) |
| **FNV-1a Random** | Hash as seed | FNV-1a as seed for deterministic random |

## 📊 Sample Results

### Topic with 10 Partitions (100,000 messages)

| Algorithm | Avg Time (ms) | Std Dev | Coef. of Variation (%) | Diff (max-min) |
|-----------|---------------|---------|------------------------|----------------|
| crc32 | 0.416 | 87.93 | 0.88 | 320 |
| consistent_random | 0.425 | 312.45 | 3.12 | 1,024 |
| murmur2 | 0.427 | 80.12 | 0.80 | 232 |
| murmur2_random | 0.438 | 298.76 | 2.99 | 978 |
| fnv1a | 0.411 | 36.30 | 0.36 | 111 |
| fnv1a_random | 0.421 | 287.54 | 2.88 | 891 |

### 🏆 Highlights

- **⚡ Fastest**: FNV-1a (0.411 ms)
- **📈 Best Distribution**: FNV-1a (36.30 std dev)
- **🎯 Lowest Variation**: FNV-1a (0.36%)
- **⚖️ Smallest Max-Min Difference**: FNV-1a (111 messages)

## 🛠️ Technologies

- **Go 1.21+**: Programming language
- **Sarama**: Kafka client for Go
- **Redpanda**: Kafka-compatible streaming platform (test environment)
- **Docker**: Containerization and isolated environment

## 📋 Prerequisites

- Docker and Docker Compose installed
- Make (optional but recommended)
- Go 1.21+ (for local development)

## 🔧 Installation and Usage

### 1. Clone the repository

```bash
git clone https://github.com/your-username/kafka-partitioner-benchmark.git
cd kafka-partitioner-benchmark
```

### 2. Setup the environment

```bash
make setup
```

This command will:
- Build Docker containers
- Start Redpanda (Kafka-compatible)
- Create test topic with 10 partitions
- Prepare environment for execution

### 3. Run the benchmark

```bash
make benchmark
```

### 4. Access Redpanda Console

Open your browser at: [http://localhost:8660](http://localhost:8660)

## 📈 Interpreting Results

### Important Metrics

1. **Average Time**: Lower is better (indicates performance)
2. **Standard Deviation**: Lower indicates more uniform distribution
3. **Coefficient of Variation**: Relative measure of dispersion (lower is better)
4. **Diff (max-min)**: Difference between most and least populated partitions

### Result File

After execution, the file `BENCHMARK_RESULT.md` is generated at the project root with:
- Complete test configuration
- Formatted results table
- Analysis and recommendation

## 🎛️ Configuration

You can modify benchmark parameters by editing `cmd/benchmark/main.go`:

```go
numMessages := 100000  // Number of messages
broker := "localhost:9092"  // Broker address
topic := "create-10"  // Topic name
```

## 🔍 How It Works

### Partitioning Strategies

#### 1. Direct Hash + Modulo
```go
hash := algorithm(key)
partition := hash % numPartitions
```

#### 2. Hash as Seed (Deterministic Random)
```go
hash := algorithm(key)
random := NewRandom(hash)
partition := random.Next(numPartitions)
```

### Execution Flow

1. For each algorithm:
   - Configure partitioner in producer
   - Send N messages with unique keys
   - Measure hash time per message
   - Record distribution per partition

2. Calculate metrics:
   - Average hash time
   - Standard deviation of distribution
   - Coefficient of variation
   - Max-min difference between partitions

3. Generate comparative report

## 📝 Available Make Commands

```bash
make setup      # Setup complete environment
make benchmark  # Run benchmark
make container  # Access application container
make help       # Show all available commands
```

## 🤝 Contributing

Contributions are welcome! Feel free to:

1. Fork the project
2. Create a branch for your feature (`git checkout -b feature/NewFeature`)
3. Commit your changes (`git commit -m 'Add new feature'`)
4. Push to the branch (`git push origin feature/NewFeature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## 👨‍💻 Author

Douglas Medeiros

## 🔗 Useful Links

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Sarama Client](https://github.com/IBM/sarama)
- [Redpanda](https://redpanda.com/)
- [Murmur2 Hash](https://en.wikipedia.org/wiki/MurmurHash)
- [FNV Hash](https://en.wikipedia.org/wiki/Fowler%E2%80%93Noll%E2%80%93Vo_hash_function)

---

⭐ If this project was useful, consider giving it a star!
