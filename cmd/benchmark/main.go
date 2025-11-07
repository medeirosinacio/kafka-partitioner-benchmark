package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
)

func main() {
	// Lê configurações das variáveis de ambiente
	algorithm := "hash"
	numMessages := 100000
	broker := "localhost:9092"
	topic := "create"

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
	case "hash":
		config.Producer.Partitioner = sarama.NewHashPartitioner
	case "hash-ref":
		config.Producer.Partitioner = sarama.NewReferenceHashPartitioner
	case "random":
		config.Producer.Partitioner = sarama.NewRandomPartitioner
	case "round-robin":
		config.Producer.Partitioner = sarama.NewRoundRobinPartitioner
	default:
		config.Producer.Partitioner = sarama.NewHashPartitioner
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
			"created_at":     time.Now().Format(time.RFC3339),
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

	// Mostra resultado
	fmt.Printf("\n✓ Concluído!\n")
	fmt.Printf("Tempo: %v\n", duration)
	fmt.Printf("Taxa: %.0f msg/s\n\n", float64(numMessages)/duration.Seconds())

	fmt.Println("Distribuição por partição:")
	for p := int32(0); p < int32(len(distribution)); p++ {
		count := distribution[p]
		fmt.Printf("  P%d: %d mensagens\n", p, count)
	}
}
