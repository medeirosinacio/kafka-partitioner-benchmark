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
	"github.com/google/uuid"
	"github.com/spaolacci/murmur3"
)

// CRC32Partitioner Custom partitioners
type CRC32Partitioner struct{}

func (p *CRC32Partitioner) Partition(message *sarama.ProducerMessage, numPartitions int32) (int32, error) {
	if message.Key == nil {
		return int32(rand.Intn(int(numPartitions))), nil
	}
	key, _ := message.Key.Encode()
	hash := int32(crc32.ChecksumIEEE(key))
	if hash < 0 {
		hash = -hash
	}
	return hash % numPartitions, nil
}

func (p *CRC32Partitioner) RequiresConsistency() bool { return true }

type ConsistentRandomPartitioner struct{}

func (p *ConsistentRandomPartitioner) Partition(message *sarama.ProducerMessage, numPartitions int32) (int32, error) {
	if message.Key == nil {
		return int32(rand.Intn(int(numPartitions))), nil
	}
	key, _ := message.Key.Encode()
	h := fnv.New32a()
	h.Write(key)
	seed := int64(h.Sum32())
	r := rand.New(rand.NewSource(seed))
	return int32(r.Intn(int(numPartitions))), nil
}

func (p *ConsistentRandomPartitioner) RequiresConsistency() bool { return true }

type Murmur2Partitioner struct{}

func (p *Murmur2Partitioner) Partition(message *sarama.ProducerMessage, numPartitions int32) (int32, error) {
	if message.Key == nil {
		return int32(rand.Intn(int(numPartitions))), nil
	}
	key, _ := message.Key.Encode()
	hash := int32(murmur3.Sum32(key))
	if hash < 0 {
		hash = -hash
	}
	return hash % numPartitions, nil
}

func (p *Murmur2Partitioner) RequiresConsistency() bool { return true }

type Murmur2RandomPartitioner struct{}

func (p *Murmur2RandomPartitioner) Partition(message *sarama.ProducerMessage, numPartitions int32) (int32, error) {
	if message.Key == nil {
		return int32(rand.Intn(int(numPartitions))), nil
	}
	key, _ := message.Key.Encode()
	hash := murmur3.Sum32(key)
	r := rand.New(rand.NewSource(int64(hash)))
	return int32(r.Intn(int(numPartitions))), nil
}

func (p *Murmur2RandomPartitioner) RequiresConsistency() bool { return true }

type FNV1aPartitioner struct{}

func (p *FNV1aPartitioner) Partition(message *sarama.ProducerMessage, numPartitions int32) (int32, error) {
	if message.Key == nil {
		return int32(rand.Intn(int(numPartitions))), nil
	}
	key, _ := message.Key.Encode()
	h := fnv.New32a()
	h.Write(key)
	hash := int32(h.Sum32())
	if hash < 0 {
		hash = -hash
	}
	return hash % numPartitions, nil
}

func (p *FNV1aPartitioner) RequiresConsistency() bool { return true }

type FNV1aRandomPartitioner struct{}

func (p *FNV1aRandomPartitioner) Partition(message *sarama.ProducerMessage, numPartitions int32) (int32, error) {
	if message.Key == nil {
		return int32(rand.Intn(int(numPartitions))), nil
	}
	key, _ := message.Key.Encode()
	h := fnv.New32a()
	h.Write(key)
	seed := int64(h.Sum32())
	r := rand.New(rand.NewSource(seed))
	return int32(r.Intn(int(numPartitions))), nil
}

func (p *FNV1aRandomPartitioner) RequiresConsistency() bool { return true }

func main() {
	// Lê configurações das variáveis de ambiente
	algorithm := "crc32" // Altere aqui: crc32, consistent_random, murmur2, murmur2_random, fnv1a, fnv1a_random
	numMessages := 100000
	broker := "localhost:9092"
	topic := "create-10"

	fmt.Printf("Produzindo %d mensagens\n", numMessages)
	fmt.Printf("Broker: %s\n", broker)
	fmt.Printf("Tópico: %s\n", topic)
	fmt.Printf("Algoritmo: %s\n\n", algorithm)

	// Configura produtor
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForLocal

	// Define particionador baseado no algoritmo
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
	case "hash":
		config.Producer.Partitioner = sarama.NewHashPartitioner
	case "hash-ref":
		config.Producer.Partitioner = sarama.NewReferenceHashPartitioner
	case "random":
		config.Producer.Partitioner = sarama.NewRandomPartitioner
	case "round-robin":
		config.Producer.Partitioner = sarama.NewRoundRobinPartitioner
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

	// Produz mensagens
	start := time.Now()
	distribution := make(map[int32]int)
	var hashTimes []time.Duration

	for i := 0; i < numMessages; i++ {
		key := uuid.New().String()

		message := map[string]interface{}{
			"created_at": time.Now().Format(time.RFC3339),
		}

		payload, _ := json.Marshal(message)

		msg := &sarama.ProducerMessage{
			Topic: topic,
			Key:   sarama.StringEncoder(key),
			Value: sarama.ByteEncoder(payload),
		}

		// Mede tempo de cálculo do hash/particionamento
		hashStart := time.Now()
		partition, _, err := producer.SendMessage(msg)
		hashDuration := time.Since(hashStart)
		hashTimes = append(hashTimes, hashDuration)

		if err != nil {
			log.Printf("Erro ao enviar: %v", err)
			continue
		}

		distribution[partition]++

		// Mostra progresso
		if (i+1)%10000 == 0 {
			fmt.Printf("Enviadas %d mensagens...\n", i+1)
		}
	}

	duration := time.Since(start)

	// Calcula total de mensagens enviadas
	totalSent := 0
	for _, count := range distribution {
		totalSent += count
	}

	// Calcula métricas de performance
	avgHashTime := calculateAvgDuration(hashTimes)

	// Calcula métricas de distribuição
	numPartitions := len(distribution)
	expectedPerPartition := float64(totalSent) / float64(numPartitions)
	stdDev := calculateStdDev(distribution, expectedPerPartition)
	collisionRate := calculateCollisionRate(distribution, expectedPerPartition)
	coefficientOfVariation := stdDev / expectedPerPartition

	// Mostra resultado
	fmt.Printf("\n✓ Concluído!\n")
	fmt.Printf("Tempo total: %v\n", duration)
	fmt.Printf("Taxa: %.0f msg/s\n", float64(numMessages)/duration.Seconds())
	fmt.Printf("Total de mensagens enviadas: %d/%d\n", totalSent, numMessages)
	fmt.Printf("Taxa de sucesso: %.2f%%\n\n", (float64(totalSent)/float64(numMessages))*100)

	fmt.Println("═══════════════════════════════════════════════")
	fmt.Println("MÉTRICAS DE PERFORMANCE")
	fmt.Println("═══════════════════════════════════════════════")
	fmt.Printf("Tempo médio por hash: %v\n", avgHashTime)
	fmt.Printf("Throughput teórico: %.0f msg/s\n\n", 1000000000.0/float64(avgHashTime.Nanoseconds()))

	fmt.Println("═══════════════════════════════════════════════")
	fmt.Println("MÉTRICAS DE DISTRIBUIÇÃO")
	fmt.Println("═══════════════════════════════════════════════")
	fmt.Printf("Total de partições: %d\n", numPartitions)
	fmt.Printf("Média esperada por partição: %.2f mensagens\n", expectedPerPartition)
	fmt.Printf("Desvio padrão: %.2f\n", stdDev)
	fmt.Printf("Coeficiente de variação: %.4f (%.2f%%)\n", coefficientOfVariation, coefficientOfVariation*100)
	fmt.Printf("Taxa de colisão (>5%% desvio): %.2f%%\n\n", collisionRate)

	fmt.Println("═══════════════════════════════════════════════")
	fmt.Println("DISTRIBUIÇÃO POR PARTIÇÃO")
	fmt.Println("═══════════════════════════════════════════════")

	// Ordena partições para exibição
	var partitions []int32
	for p := range distribution {
		partitions = append(partitions, p)
	}

	for i := int32(0); i < int32(numPartitions); i++ {
		count := distribution[i]
		percentage := (float64(count) / float64(totalSent)) * 100
		deviation := float64(count) - expectedPerPartition
		deviationPercent := (deviation / expectedPerPartition) * 100

		indicator := " "
		if math.Abs(deviationPercent) > 5 {
			indicator = "⚠"
		}

		fmt.Printf("  %s P%02d: %6d msgs (%.2f%%) | Desvio: %+7.2f (%+6.2f%%)\n",
			indicator, i, count, percentage, deviation, deviationPercent)
	}

	fmt.Printf("\n═══════════════════════════════════════════════")
	fmt.Println("\nRESUMO ESTATÍSTICO")
	fmt.Println("═══════════════════════════════════════════════")

	minCount, maxCount := getMinMax(distribution)
	fmt.Printf("Partição com menos msgs: %d\n", minCount)
	fmt.Printf("Partição com mais msgs: %d\n", maxCount)
	fmt.Printf("Diferença (max-min): %d\n", maxCount-minCount)
	fmt.Printf("Razão (max/min): %.2fx\n", float64(maxCount)/float64(minCount))
}

// calculateAvgDuration calcula a duração média
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

// calculateStdDev calcula o desvio padrão da distribuição
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

// calculateCollisionRate calcula a taxa de partições com desvio > 5%
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

// getMinMax retorna o menor e maior valor de mensagens em partições
func getMinMax(distribution map[int32]int) (int, int) {
	if len(distribution) == 0 {
		return 0, 0
	}

	min := int(^uint(0) >> 1) // max int
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
