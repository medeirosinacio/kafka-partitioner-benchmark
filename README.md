# Kafka Partitioner Benchmark

[![Go Version](https://img.shields.io/badge/Go-1.21+-00ADD8?style=flat&logo=go)](https://golang.org)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

Ferramenta de benchmark para testar e comparar o desempenho de algoritmos de hash no particionamento de mensagens Kafka. Mede qualidade de distribuição, throughput e taxas de colisão entre diferentes estratégias de particionamento.

## 🎯 Objetivo

Este projeto tem como objetivo identificar o melhor algoritmo de hash para particionamento de mensagens no Apache Kafka, considerando:

- **Performance**: Tempo médio de processamento por mensagem
- **Distribuição**: Uniformidade na distribuição entre partições
- **Consistência**: Estabilidade e previsibilidade do particionamento
- **Variabilidade**: Desvio padrão e coeficiente de variação

## 🚀 Algoritmos Testados

| Algoritmo | Estratégia | Descrição |
|-----------|-----------|-----------|
| **CRC32** | Hash direto + módulo | Checksum CRC32 IEEE com operação de módulo |
| **CRC32 Random** | Hash como seed | CRC32 como seed para random determinístico |
| **Murmur2** | Hash direto + módulo | Implementação compatível com Kafka Java |
| **Murmur2 Random** | Hash como seed | Murmur2 como seed para random determinístico |
| **FNV-1a** | Hash direto + módulo | FNV-1a (Fowler-Noll-Vo) de 32 bits |
| **FNV-1a Random** | Hash como seed | FNV-1a como seed para random determinístico |

## 📊 Resultados de Exemplo

### Tópico com 10 Partições (100.000 mensagens)

| Algoritmo | Tempo Médio (ms) | Desvio Padrão | Coef. Variação (%) | Diff (max-min) |
|-----------|------------------|---------------|--------------------|----------------|
| crc32 | 0.416 | 87.93 | 0.88 | 320 |
| consistent_random | 0.425 | 312.45 | 3.12 | 1,024 |
| murmur2 | 0.427 | 80.12 | 0.80 | 232 |
| murmur2_random | 0.438 | 298.76 | 2.99 | 978 |
| fnv1a | 0.411 | 36.30 | 0.36 | 111 |
| fnv1a_random | 0.421 | 287.54 | 2.88 | 891 |

### 🏆 Destaques

- **⚡ Mais Rápido**: FNV-1a (0.411 ms)
- **📈 Melhor Distribuição**: FNV-1a (36.30 desvio padrão)
- **🎯 Menor Variação**: FNV-1a (0.36%)
- **⚖️ Menor Diferença Max-Min**: FNV-1a (111 mensagens)

## 🛠️ Tecnologias

- **Go 1.21+**: Linguagem de programação
- **Sarama**: Cliente Kafka para Go
- **Redpanda**: Kafka-compatible streaming platform (ambiente de testes)
- **Docker**: Containerização e ambiente isolado

## 📋 Pré-requisitos

- Docker e Docker Compose instalados
- Make (opcional, mas recomendado)
- Go 1.21+ (para desenvolvimento local)

## 🔧 Instalação e Uso

### 1. Clone o repositório

```bash
git clone https://github.com/seu-usuario/kafka-partitioner-benchmark.git
cd kafka-partitioner-benchmark
```

### 2. Configure o ambiente

```bash
make setup
```

Este comando irá:
- Construir os containers Docker
- Iniciar o Redpanda (Kafka-compatible)
- Criar o tópico de testes com 10 partições
- Preparar o ambiente para execução

### 3. Execute o benchmark

```bash
make benchmark
```

### 4. Acesse o Redpanda Console

Abra o navegador em: [http://localhost:8660](http://localhost:8660)

## 📈 Interpretando os Resultados

### Métricas Importantes

1. **Tempo Médio**: Menor é melhor (indica performance)
2. **Desvio Padrão**: Menor indica distribuição mais uniforme
3. **Coeficiente de Variação**: Medida relativa de dispersão (menor é melhor)
4. **Diff (max-min)**: Diferença entre partição mais e menos populada

### Arquivo de Resultado

Após a execução, o arquivo `BENCHMARK_RESULT.md` é gerado na raiz do projeto com:
- Configuração completa do teste
- Tabela de resultados formatada
- Análise e recomendação

## 🎛️ Configuração

Você pode modificar os parâmetros do benchmark editando `cmd/benchmark/main.go`:

```go
numMessages := 100000  // Número de mensagens
broker := "localhost:9092"  // Endereço do broker
topic := "create-10"  // Nome do tópico
```

## 🔍 Como Funciona

### Estratégias de Particionamento

#### 1. Hash Direto + Módulo
```go
hash := algorithm(key)
partition := hash % numPartitions
```

#### 2. Hash como Seed (Random Determinístico)
```go
hash := algorithm(key)
random := NewRandom(hash)
partition := random.Next(numPartitions)
```

### Fluxo de Execução

1. Para cada algoritmo:
   - Configura o partitioner no producer
   - Envia N mensagens com chaves únicas
   - Mede tempo de hash por mensagem
   - Registra distribuição por partição

2. Calcula métricas:
   - Tempo médio de hash
   - Desvio padrão da distribuição
   - Coeficiente de variação
   - Diferença max-min entre partições

3. Gera relatório comparativo

## 📝 Comandos Make Disponíveis

```bash
make setup      # Configura o ambiente completo
make benchmark  # Executa o benchmark
make container  # Acessa o container da aplicação
make help       # Mostra todos os comandos disponíveis
```

## 🤝 Contribuindo

Contribuições são bem-vindas! Sinta-se à vontade para:

1. Fazer fork do projeto
2. Criar uma branch para sua feature (`git checkout -b feature/NovaFeature`)
3. Commit suas mudanças (`git commit -m 'Adiciona nova feature'`)
4. Push para a branch (`git push origin feature/NovaFeature`)
5. Abrir um Pull Request

## 📄 Licença

Este projeto está sob a licença MIT. Veja o arquivo [LICENSE](LICENSE) para mais detalhes.

## 👨‍💻 Autor

Douglas Medeiros

## 🔗 Links Úteis

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Sarama Client](https://github.com/IBM/sarama)
- [Redpanda](https://redpanda.com/)
- [Murmur2 Hash](https://en.wikipedia.org/wiki/MurmurHash)
- [FNV Hash](https://en.wikipedia.org/wiki/Fowler%E2%80%93Noll%E2%80%93Vo_hash_function)

---

⭐ Se este projeto foi útil, considere dar uma estrela!
