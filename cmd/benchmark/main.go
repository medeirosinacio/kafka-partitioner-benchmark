package main

import (
	"encoding/json"
	"fmt"
	"hash/crc32"
	"hash/fnv"
	"log"
	"math"
	"math/rand"
	"time"

	"github.com/IBM/sarama"
)

// CRC32Partitioner - Hash direto com módulo
type CRC32Partitioner struct{}

func (p *CRC32Partitioner) Partition(message *sarama.ProducerMessage, numPartitions int32) (int32, error) {
	if message.Key == nil {
		return int32(rand.Intn(int(numPartitions))), nil
	}
	key, _ := message.Key.Encode()
	hash := crc32.ChecksumIEEE(key)
	return int32(hash % uint32(numPartitions)), nil
}

func (p *CRC32Partitioner) RequiresConsistency() bool { return true }

// ConsistentRandomPartitioner - Usa hash como seed para random determinístico
type ConsistentRandomPartitioner struct{}

func (p *ConsistentRandomPartitioner) Partition(message *sarama.ProducerMessage, numPartitions int32) (int32, error) {
	if message.Key == nil {
		return int32(rand.Intn(int(numPartitions))), nil
	}
	key, _ := message.Key.Encode()
	hash := crc32.ChecksumIEEE(key)
	// USA O HASH COMO SEED - isso é diferente de módulo direto
	r := rand.New(rand.NewSource(int64(hash)))
	return int32(r.Intn(int(numPartitions))), nil
}

func (p *ConsistentRandomPartitioner) RequiresConsistency() bool { return true }

// Murmur2Partitioner - Hash direto com módulo
type Murmur2Partitioner struct{}

func (p *Murmur2Partitioner) Partition(message *sarama.ProducerMessage, numPartitions int32) (int32, error) {
	if message.Key == nil {
		return int32(rand.Intn(int(numPartitions))), nil
	}
	key, _ := message.Key.Encode()
	hash := murmur2(key)
	return toPositive(hash) % numPartitions, nil
}

func (p *Murmur2Partitioner) RequiresConsistency() bool { return true }

// Murmur2RandomPartitioner - Usa hash como seed para random determinístico
type Murmur2RandomPartitioner struct{}

func (p *Murmur2RandomPartitioner) Partition(message *sarama.ProducerMessage, numPartitions int32) (int32, error) {
	if message.Key == nil {
		return int32(rand.Intn(int(numPartitions))), nil
	}
	key, _ := message.Key.Encode()
	hash := murmur2(key)
	// USA O HASH COMO SEED - isso é diferente de módulo direto
	r := rand.New(rand.NewSource(int64(toPositive(hash))))
	return int32(r.Intn(int(numPartitions))), nil
}

func (p *Murmur2RandomPartitioner) RequiresConsistency() bool { return true }

// FNV1aPartitioner - Hash direto com módulo
type FNV1aPartitioner struct{}

func (p *FNV1aPartitioner) Partition(message *sarama.ProducerMessage, numPartitions int32) (int32, error) {
	if message.Key == nil {
		return int32(rand.Intn(int(numPartitions))), nil
	}
	key, _ := message.Key.Encode()
	h := fnv.New32a()
	h.Write(key)
	hash := h.Sum32()
	return int32(hash % uint32(numPartitions)), nil
}

func (p *FNV1aPartitioner) RequiresConsistency() bool { return true }

// FNV1aRandomPartitioner - Usa hash como seed para random determinístico
type FNV1aRandomPartitioner struct{}

func (p *FNV1aRandomPartitioner) Partition(message *sarama.ProducerMessage, numPartitions int32) (int32, error) {
	if message.Key == nil {
		return int32(rand.Intn(int(numPartitions))), nil
	}
	key, _ := message.Key.Encode()
	h := fnv.New32a()
	h.Write(key)
	hash := h.Sum32()
	// USA O HASH COMO SEED - isso é diferente de módulo direto
	r := rand.New(rand.NewSource(int64(hash)))
	return int32(r.Intn(int(numPartitions))), nil
}

func (p *FNV1aRandomPartitioner) RequiresConsistency() bool { return true }

// murmur2 - Implementação do Murmur2 compatível com Kafka Java
func murmur2(data []byte) int32 {
	length := int32(len(data))
	const (
		seed uint32 = 0x9747b28c
		m    int32  = 0x5bd1e995
		r    uint32 = 24
	)

	h := int32(seed ^ uint32(length))
	length4 := length / 4

	for i := int32(0); i < length4; i++ {
		i4 := i * 4
		k := int32(data[i4+0]&0xff) + (int32(data[i4+1]&0xff) << 8) +
			(int32(data[i4+2]&0xff) << 16) + (int32(data[i4+3]&0xff) << 24)
		k *= m
		k ^= int32(uint32(k) >> r)
		k *= m
		h *= m
		h ^= k
	}

	switch length % 4 {
	case 3:
		h ^= int32(data[(length & ^3)+2]&0xff) << 16
		fallthrough
	case 2:
		h ^= int32(data[(length & ^3)+1]&0xff) << 8
		fallthrough
	case 1:
		h ^= int32(data[length & ^3] & 0xff)
		h *= m
	}

	h ^= int32(uint32(h) >> 13)
	h *= m
	h ^= int32(uint32(h) >> 15)

	return h
}

// toPositive - Converte hash para positivo (compatível com Kafka)
func toPositive(number int32) int32 {
	return number & 0x7fffffff
}

// BenchmarkResult armazena resultados do benchmark
type BenchmarkResult struct {
	Algorithm              string
	AvgHashTime            time.Duration
	StdDev                 float64
	CoefficientOfVariation float64
	CollisionRate          float64
	MinMaxDiff             int
	TotalSent              int
	Duration               time.Duration
}

func main() {
	numMessages := 100000
	broker := "localhost:9092"
	topic := "create-100"

	algorithms := []string{
		"crc32",
		"consistent_random", // Agora DIFERENTE - usa random com seed
		"murmur2",
		"murmur2_random", // Agora DIFERENTE - usa random com seed
		"fnv1a",
		"fnv1a_random", // Agora DIFERENTE - usa random com seed
	}

	fmt.Println("╔═══════════════════════════════════════════════╗")
	fmt.Println("║    KAFKA PARTITIONER BENCHMARK                ║")
	fmt.Println("╚═══════════════════════════════════════════════╝")
	fmt.Printf("\nMensagens por algoritmo: %d\n", numMessages)
	fmt.Printf("Broker: %s\n", broker)
	fmt.Printf("Tópico: %s\n\n", topic)

	var results []BenchmarkResult

	for _, algorithm := range algorithms {
		fmt.Printf("Executando benchmark: %s\n", algorithm)
		result := runBenchmark(algorithm, numMessages, broker, topic)
		results = append(results, result)
		fmt.Println()
	}

	printResultsTable(results)
}

func runBenchmark(algorithm string, numMessages int, broker string, topic string) BenchmarkResult {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForLocal

	switch algorithm {
	case "crc32":
		config.Producer.Partitioner = func(topic string) sarama.Partitioner {
			return &CRC32Partitioner{}
		}
	case "consistent_random":
		config.Producer.Partitioner = func(topic string) sarama.Partitioner {
			return &ConsistentRandomPartitioner{}
		}
	case "murmur2":
		config.Producer.Partitioner = func(topic string) sarama.Partitioner {
			return &Murmur2Partitioner{}
		}
	case "murmur2_random":
		config.Producer.Partitioner = func(topic string) sarama.Partitioner {
			return &Murmur2RandomPartitioner{}
		}
	case "fnv1a":
		config.Producer.Partitioner = func(topic string) sarama.Partitioner {
			return &FNV1aPartitioner{}
		}
	case "fnv1a_random":
		config.Producer.Partitioner = func(topic string) sarama.Partitioner {
			return &FNV1aRandomPartitioner{}
		}
	default:
		config.Producer.Partitioner = func(topic string) sarama.Partitioner {
			return &CRC32Partitioner{}
		}
	}

	producer, err := sarama.NewSyncProducer([]string{broker}, config)
	if err != nil {
		log.Fatalf("Erro ao criar producer: %v", err)
	}
	defer producer.Close()

	start := time.Now()
	distribution := make(map[int32]int)
	var hashTimes []time.Duration

	for i := 0; i < numMessages; i++ {
		key := i

		message := map[string]interface{}{
			"created_at": time.Now().Format(time.RFC3339),
		}

		payload, _ := json.Marshal(message)

		msg := &sarama.ProducerMessage{
			Topic: topic,
			Key:   sarama.Encoder(key),
			Value: sarama.ByteEncoder(payload),
		}

		hashStart := time.Now()
		partition, _, err := producer.SendMessage(msg)
		hashDuration := time.Since(hashStart)
		hashTimes = append(hashTimes, hashDuration)

		if err != nil {
			log.Printf("Erro ao enviar: %v", err)
			continue
		}

		distribution[partition]++

		if (i+1)%20000 == 0 {
			fmt.Printf("  ✓ %d mensagens enviadas\n", i+1)
		}
	}

	duration := time.Since(start)

	totalSent := 0
	for _, count := range distribution {
		totalSent += count
	}

	avgHashTime := calculateAvgDuration(hashTimes)
	expectedPerPartition := float64(totalSent) / float64(len(distribution))
	stdDev := calculateStdDev(distribution, expectedPerPartition)
	collisionRate := calculateCollisionRate(distribution, expectedPerPartition)
	coefficientOfVariation := stdDev / expectedPerPartition
	minCount, maxCount := getMinMax(distribution)
	minMaxDiff := maxCount - minCount

	fmt.Printf("  Concluído em %v (%.0f msg/s)\n", duration, float64(totalSent)/duration.Seconds())

	return BenchmarkResult{
		Algorithm:              algorithm,
		AvgHashTime:            avgHashTime,
		StdDev:                 stdDev,
		CoefficientOfVariation: coefficientOfVariation,
		CollisionRate:          collisionRate,
		MinMaxDiff:             minMaxDiff,
		TotalSent:              totalSent,
		Duration:               duration,
	}
}

func printResultsTable(results []BenchmarkResult) {
	fmt.Println("\n╔═══════════════════════════════════════════════════════════════════════════════════╗")
	fmt.Println("║                         RESULTADOS DO BENCHMARK                                   ║")
	fmt.Println("╠═══════════════════════════════════════════════════════════════════════════════════╣")
	fmt.Println("║ Algoritmo          │ Tempo médio │ Desvio padrão │ Coef. variação │ Diff (max-min) ║")
	fmt.Println("╠════════════════════╪═════════════╪═══════════════╪════════════════╪════════════════╣")

	for _, result := range results {
		avgTimeMs := float64(result.AvgHashTime.Microseconds()) / 1000.0
		fmt.Printf("║ %-18s │ %8.3f ms │ %13.2f │ %13.2f%% │ %14d ║\n",
			result.Algorithm,
			avgTimeMs,
			result.StdDev,
			result.CoefficientOfVariation*100,
			result.MinMaxDiff)
	}

	fmt.Println("╚════════════════════╧═════════════╧═══════════════╧════════════════╧════════════════╝")

	bestTime := results[0]
	bestStdDev := results[0]
	bestCoefVar := results[0]
	bestDiff := results[0]

	for _, result := range results {
		if result.AvgHashTime < bestTime.AvgHashTime {
			bestTime = result
		}
		if result.StdDev < bestStdDev.StdDev {
			bestStdDev = result
		}
		if result.CoefficientOfVariation < bestCoefVar.CoefficientOfVariation {
			bestCoefVar = result
		}
		if result.MinMaxDiff < bestDiff.MinMaxDiff {
			bestDiff = result
		}
	}

	fmt.Println("\n📊 MELHORES RESULTADOS:")
	fmt.Printf("  🏆 Mais rápido: %s (%.3f ms)\n", bestTime.Algorithm, float64(bestTime.AvgHashTime.Microseconds())/1000.0)
	fmt.Printf("  🏆 Melhor distribuição: %s (%.2f)\n", bestStdDev.Algorithm, bestStdDev.StdDev)
	fmt.Printf("  🏆 Melhor coeficiente: %s (%.2f%%)\n", bestCoefVar.Algorithm, bestCoefVar.CoefficientOfVariation*100)
	fmt.Printf("  🏆 Menor diferença max-min: %s (%d mensagens)\n", bestDiff.Algorithm, bestDiff.MinMaxDiff)
}

func calculateAvgDuration(durations []time.Duration) time.Duration {
	if len(durations) == 0 {
		return 0
	}
	var total int64
	for _, d := range durations {
		total += d.Nanoseconds()
	}
	return time.Duration(total / int64(len(durations)))
}

func calculateStdDev(distribution map[int32]int, mean float64) float64 {
	if len(distribution) == 0 {
		return 0
	}

	var sumSquares float64
	for _, count := range distribution {
		diff := float64(count) - mean
		sumSquares += diff * diff
	}

	variance := sumSquares / float64(len(distribution))
	return math.Sqrt(variance)
}

func calculateCollisionRate(distribution map[int32]int, mean float64) float64 {
	if len(distribution) == 0 {
		return 0
	}

	collisions := 0
	for _, count := range distribution {
		deviation := math.Abs(float64(count) - mean)
		deviationPercent := (deviation / mean) * 100
		if deviationPercent > 5 {
			collisions++
		}
	}

	return (float64(collisions) / float64(len(distribution))) * 100
}

func getMinMax(distribution map[int32]int) (int, int) {
	if len(distribution) == 0 {
		return 0, 0
	}

	min := int(^uint(0) >> 1)
	max := 0

	for _, count := range distribution {
		if count < min {
			min = count
		}
		if count > max {
			max = count
		}
	}

	return min, max
}
