package main

import (
	"encoding/json"
	"fmt"
	"hash/crc32"
	"hash/fnv"
	"log"
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

		partition, _, err := producer.SendMessage(msg)
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

	// Mostra resultado
	fmt.Printf("\n✓ Concluído!\n")
	fmt.Printf("Tempo: %v\n", duration)
	fmt.Printf("Taxa: %.0f msg/s\n", float64(numMessages)/duration.Seconds())
	fmt.Printf("Total de mensagens enviadas: %d/%d\n", totalSent, numMessages)
	fmt.Printf("Taxa de sucesso: %.2f%%\n\n", (float64(totalSent)/float64(numMessages))*100)

	fmt.Println("Distribuição por partição:")
	for p := int32(0); p < int32(len(distribution)); p++ {
		count := distribution[p]
		percentage := (float64(count) / float64(totalSent)) * 100
		fmt.Printf("  P%d: %d mensagens (%.2f%%)\n", p, count, percentage)
	}

	fmt.Printf("\nResumo:\n")
	fmt.Printf("  Total de partições: %d\n", len(distribution))
	fmt.Printf("  Total enviado: %d mensagens\n", totalSent)
	fmt.Printf("  Média por partição: %.0f mensagens\n", float64(totalSent)/float64(len(distribution)))
}
