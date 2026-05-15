# Arquitetura — Industrial Streaming Lab

Este documento detalha as decisões de arquitetura do projeto e a função de cada componente. Complementa o README com profundidade técnica voltada para quem quer entender o "por quê" de cada escolha.

---

## Visão geral

O lab simula um cenário IIoT (Industrial Internet of Things) com pipeline completo de streaming de dados, desde a geração até a visualização, no contexto de uma planta de mineração.

```
[Node-RED] → [EMQX] → [Bridge Python] → [Kafka] → [Consumer Python] → [InfluxDB] → [Grafana]
```

A arquitetura combina **MQTT + Kafka** como fundação para integração entre TO (Tecnologia Operacional) e TI (Tecnologia da Informação): MQTT cobre a "última milha" de coleta em campo, Kafka cobre o "hub central" de distribuição e processamento.

---

## Componentes detalhados

### 1. Node-RED (Simulador de sensores)

**Função:** simular sensores industriais publicando dados em tempo real via MQTT.

**Por que Node-RED:**

- Ferramenta amplamente usada em IIoT real, especialmente para protótipos e edge computing
- Interface visual com drag-and-drop reduz barreira de entrada
- Cliente MQTT nativo, sem necessidade de bibliotecas externas
- Permite simular múltiplos sensores com flows independentes
- Mantém o lab próximo de stacks IIoT reais — em campo, gateways frequentemente rodam Node-RED

**Sensores simulados no lab:**

O cenário simulado envolve dois equipamentos críticos de uma planta de mineração, totalizando 7 sensores:

| Equipamento | Sensor | Faixa | Frequência |
|---|---|---|---|
| Britador (JAW-CRUSHER-01) | `temperature_bearing` | 50–85 °C | 1s |
| Britador | `pressure_hydraulic` | 120–180 bar | 1s |
| Britador | `vibration` | 2–8 mm/s | 500ms |
| Britador | `power_motor` | 200–450 kW | 1s |
| Correia (BELT-CONVEYOR-01) | `belt_speed` | 1.5–3.5 m/s | 500ms |
| Correia | `load_weight` | 50–300 ton/h | 1s |
| Correia | `temperature_roller` | 30–65 °C | 2s |

**Decisões de design do simulador:**

- Cada sensor mantém uma **baseline interna evolutiva** (passeio aleatório suave) para gerar curvas com tendência realista, não ziguezague puro
- O sensor de vibração tem **3% de chance de pico curto** simulando comportamento real de equipamento rotativo
- Frequências escalonadas — variáveis "rápidas" (vibração, velocidade) publicam a 500ms; "lentas" (temperatura de roletes) a 2s
- **Throughput agregado:** ~8.5 mensagens/segundo contínuas

### 2. EMQX (Broker MQTT)

**Função:** receber mensagens dos sensores e distribuir para subscribers.

**Por que EMQX:**

- Open source, gratuito, performance excelente
- Dashboard web nativo (porta 18083) facilita inspeção em tempo real
- Suporta MQTT 3.1, 3.1.1 e 5.0
- Uma das soluções mais maduras do ecossistema MQTT

**Decisões de design:**

- **QoS 1 (at-least-once)** nos sensores — balanço entre confiabilidade e performance
- **Sem autenticação no lab** — em produção: TLS + autenticação obrigatórios + ACLs por tópico
- **Sem persistência configurada** — mensagens são entregues e descartadas (durabilidade fica no Kafka)

### 3. MQTT-Kafka Bridge (Python customizado)

**Função:** ponte entre o broker MQTT e o cluster Kafka. Recebe mensagens do MQTT e republica no Kafka, preservando contexto.

**Por que Python customizado e não Kafka Connect:**

- Curva de aprendizado mais suave para um lab de estudo
- Código transparente — cada decisão de configuração é visível e modificável
- Suficiente para o volume do lab
- Em produção, **Kafka Connect MQTT Source Connector** seria a escolha mais robusta (deixado como evolução)

**Decisões de design:**

- **Particionamento por `{plant_id}-{equipment_id}-{sensor_id}`** garante ordenação consistente para o mesmo sensor (mensagens do mesmo sensor sempre vão para a mesma partição)
- **Header `mqtt-topic`** preserva o caminho MQTT original para auditoria e roteamento futuro
- **`compression.type=gzip`** reduz tráfego de rede e armazenamento (JSON com tags repetitivas comprime muito bem — observamos ~70% de redução em testes)
- **`acks=all`** garante durabilidade — mensagem confirmada apenas após replicação (relevante quando houver cluster)
- **`linger.ms=10`** acumula mensagens em pequenos batches, melhorando throughput sem impacto perceptível em latência

### 4. Apache Kafka (1 broker, modo KRaft)

**Função:** log distribuído de eventos que desacopla produtores e consumidores. Permite múltiplos consumidores independentes, retenção temporal e replay.

**Por que Kafka:**

- Padrão da indústria para streaming de dados
- Persistência com retenção configurável (replay possível)
- Múltiplos consumidores independentes lendo do mesmo tópico
- Throughput muito alto, mesmo em hardware modesto

**Por que modo KRaft (sem Zookeeper):**

Desde Kafka 3.3, KRaft é o modo recomendado pela Apache Foundation. Vantagens:

- Elimina dependência operacional do Zookeeper
- Simplifica arquitetura (um sistema a menos)
- É o futuro do Kafka (Zookeeper será descontinuado)

Versão do lab: `confluentinc/cp-kafka:7.6.0`.

**Detalhe técnico importante sobre KRaft:** a variável `CLUSTER_ID` precisa ser um **UUID base64 válido de exatamente 16 bytes (22 caracteres após encoding)**. Strings arbitrárias são rejeitadas no preflight check do broker. O UUID deve ser gerado com o utilitário oficial:

```bash
docker run --rm confluentinc/cp-kafka:7.6.0 kafka-storage random-uuid
```

**Configuração do lab:**

- **1 broker** — em produção: 3+ brokers com replication factor 3
- **Auto-criação de tópicos** habilitada (apenas para conveniência do lab)
- **Retenção de 24h** — suficiente para demonstração
- **Listener interno** `PLAINTEXT://kafka:9092` para containers
- **Listener externo** `PLAINTEXT_HOST://localhost:29092` para acesso do host

### 5. Kafka-InfluxDB Consumer (Python customizado)

**Função:** consumir mensagens do Kafka e gravar no InfluxDB com mapeamento adequado de tags e fields.

**Decisões de design:**

- **Consumer group `influxdb-writer`** permite escalabilidade horizontal — múltiplas instâncias balanceariam partições automaticamente
- **Commit manual após gravação** garante semântica at-least-once — a mensagem só é confirmada depois que o InfluxDB respondeu OK
- **Tags InfluxDB** para campos indexáveis (`plant_id`, `area_id`, `equipment_id`, `equipment_type`, `sensor_id`, `unit`)
- **Field `value`** para o dado numérico do sensor
- **Field `quality`** para o estado semântico da leitura

**Sobre o erro `UNKNOWN_TOPIC_OR_PART` no startup:**

É esperado e não é problema real. Com `KAFKA_AUTO_CREATE_TOPICS_ENABLE=true`, o tópico `industrial-sensors` só é criado quando a primeira mensagem é publicada. Até lá, o consumer reporta o erro em loop, mas se "engata" automaticamente assim que o tópico nasce. O container permanece `Up`, não em `Restarting`.

### 6. InfluxDB 2.7

**Função:** banco de séries temporais para armazenar leituras dos sensores e responder queries do Grafana.

**Por que InfluxDB:**

- Otimizado para séries temporais — compactação, queries por janela, downsampling automático
- Linguagem **Flux** poderosa para queries analíticas
- Excelente integração com Grafana
- UI própria (Data Explorer) para exploração ad-hoc

**Estrutura dos dados:**

- **Bucket:** `sensors` (retenção 7 dias no lab)
- **Measurement:** `sensor_reading`
- **Tags:** `plant_id`, `area_id`, `equipment_id`, `equipment_type`, `sensor_id`, `unit`
- **Fields:** `value` (float), `quality` (string)
- **Timestamp:** original do sensor, não da gravação no InfluxDB

A escolha entre tag vs field tem impacto significativo de performance:

- **Tags são indexadas** — boas para filtrar com cardinalidade baixa (área, equipamento, sensor)
- **Fields não são indexados** — guardam valores que mudam constantemente (`value`) ou raramente são filtrados (`quality`)

### 7. Grafana

**Função:** dashboards em tempo real consultando o InfluxDB via Flux.

**Decisões de design do dashboard:**

O dashboard "Industrial Streaming Lab — Mineração" foi organizado em 4 linhas, com hierarquia visual da saúde do pipeline para o detalhe operacional:

**Linha 1 — KPIs do pipeline (Stats)**
- Mensagens por segundo processadas (throughput agregado)
- Sensores ativos no intervalo (contagem distinta)
- Latência da última leitura (tempo desde a última mensagem)

**Linha 2 — Estado atual dos equipamentos (Gauges)**
- Temperatura do mancal (com thresholds verde/amarelo/vermelho)
- Vibração (com thresholds em 6 e 8 mm/s)
- Potência do motor (com thresholds em 200/400/470 kW)

**Linha 3 — Histórico do Britador (Time series multi-série)**
- Painel único com vibração, pressão hidráulica e temperatura do mancal
- Três eixos Y independentes (Bar à esquerda, Celsius e mm/s à direita)
- Cores temáticas por variável

**Linha 4 — Histórico da Correia (3 Time series separados)**
- Velocidade da correia
- Vazão mássica
- Temperatura dos roletes (com threshold tracejado em 60°C)

**Decisões transversais:**

- **Cores semânticas:** verde/amarelo/vermelho em thresholds. Em séries temporais: laranja para temperatura, azul para pressão, vermelho para vibração, roxo para vazão, verde-água para velocidade
- **Tooltip mode "Single"** em todos os painéis (mostra apenas a série sob o cursor)
- **Auto-refresh padrão de 5s** — equilíbrio entre fluidez visual e carga de query
- **Range padrão de 5 minutos** — mostra movimento visível dos dados

**Provisionamento automático:**

Datasource e dashboard são provisionados via arquivos em `grafana/provisioning/`. Quem clona o repo recebe tudo configurado, sem precisar importar manualmente.

---

## Healthchecks e ordem de inicialização

Healthchecks no Docker Compose foram **essenciais** para o funcionamento estável do pipeline. Sem eles, o consumer tentava conectar ao Kafka antes do broker estar pronto e entrava em loop de erro infinito.

**Configuração adotada:**

```yaml
depends_on:
  emqx:
    condition: service_healthy
  kafka:
    condition: service_healthy
```

A diferença entre `service_started` (default) e `service_healthy`:

- `service_started`: container subiu (não significa que serviço está pronto)
- `service_healthy`: container passou no healthcheck — serviço respondendo

**Cadeia de dependência do projeto:**

1. **Kafka, EMQX, InfluxDB** sobem primeiro (sem dependências entre si)
2. **Bridge** espera EMQX + Kafka healthy
3. **Consumer** espera Kafka + InfluxDB healthy
4. **Grafana** espera InfluxDB healthy
5. **Node-RED** espera EMQX iniciado
6. **Kafka UI** espera Kafka healthy

Essa cadeia garante que cada serviço só inicia quando suas dependências estão prontas, eliminando a corrida típica de "consumer subiu antes do broker".

---

## Decisões deliberadas de simplificação

Este é um **lab de estudo**. Algumas escolhas foram simplificadas conscientemente:

| Decisão | Em produção seria... |
|---|---|
| 1 broker Kafka | 3+ brokers com replication factor 3 |
| Sem autenticação MQTT | TLS + usuário/senha + ACLs por tópico |
| Sem TLS no Kafka | mTLS entre todos os componentes |
| Tokens hardcoded no compose | Vault, AWS Secrets Manager ou similar |
| Sem Schema Registry | Schema Registry + Avro/Protobuf |
| Bridge custom em Python | Kafka Connect MQTT Source Connector |
| 1 instância de cada serviço | Cluster com alta disponibilidade (HA) |
| Auto-criação de tópicos | Tópicos pré-criados com configuração explícita |
| Retenção de 24h no Kafka | 7–30 dias dependendo do caso de uso |
| Sem políticas de retry | Retry policies, dead letter queues, idempotência |

Essas simplificações são pontos de evolução natural — viram tópicos para projetos subsequentes da série `lab-iiot-*`.

---

## Próximas iterações

O Industrial Streaming Lab é o primeiro projeto de uma série focada em arquiteturas de dados industriais. A evolução planejada:

### `lab-iiot-02-kafka-production` — Kafka em produção

Foco em **resiliência e escala** da camada Kafka:

- Cluster com múltiplos brokers e replicação real entre eles
- Tópicos particionados explorando paralelismo de processamento
- Configurações de ISR (In-Sync Replicas) e replication factor
- Monitoramento do cluster via JMX Exporter + Prometheus
- Painéis Grafana dedicados a métricas Kafka (throughput, lag, saúde dos brokers)
- Demonstrações práticas de tolerância a falhas (broker caindo, consumidores travando)

### `lab-iiot-03-anomaly-detection` — Inteligência sobre o stream

Foco em **valor analítico** sobre os dados em tempo real:

- Detecção de anomalias com algoritmos clássicos (média móvel, desvio padrão, Isolation Forest)
- Possível introdução de Spark Streaming ou Flink no pipeline
- Geração de alertas baseados nos sinais detectados
- Painéis dedicados a sinais anômalos no histórico

### Projetos subsequentes

Em definição. Áreas exploratórias: data lake com Iceberg/Parquet, integração com sistemas externos (ERP/MES), digital twin com visualização 3D, edge computing com agregação local.

---

*Documento mantido como referência arquitetural do projeto. Última revisão: maio/2026.*
