# Memória Técnica — Industrial Streaming Lab

> **Documento de continuidade técnica.** Esta é uma memória estruturada das decisões, especificações e estado técnico do projeto **lab-iiot-01-streaming-pipeline**. Pode ser anexada à memória de um assistente de IA (como o Claude) ou usada como referência por qualquer pessoa que queira entender, estender ou dar continuidade ao projeto.

---

## 1. Contexto e tese técnica do projeto

### Identidade do projeto

O **Industrial Streaming Lab** é um laboratório de estudo open source que demonstra uma arquitetura completa de streaming de dados industriais em ambiente local containerizado. É o primeiro projeto de uma série (`lab-iiot-*`) voltada para arquiteturas de dados em ambientes IIoT (Industrial Internet of Things).

O cenário simulado representa uma planta de **mineração** com dois equipamentos críticos da cadeia produtiva: um britador de mandíbula (área de britagem) e uma correia transportadora (área de transporte). Sete sensores publicam leituras continuamente, atravessando todo o pipeline desde a coleta até a visualização.

### Tese técnica

A combinação **MQTT + Kafka** é hoje uma das fundações para integração entre TO (Tecnologia Operacional) e TI (Tecnologia da Informação) em ambientes industriais. Os dois protocolos não competem — são complementares por design:

- **MQTT** é otimizado para a "última milha": leve, tolera reconexões, opera bem sobre redes restritas ou intermitentes típicas de campo industrial.
- **Kafka** é otimizado para o "hub central": alto throughput, retenção temporal, distribuição confiável para múltiplos consumidores simultâneos.

O projeto materializa essa arquitetura num lab que pode ser executado por qualquer pessoa com Docker instalado.

### Escopo deliberado

O lab tem escopo **deliberadamente reduzido** para focar na arquitetura de dados:

| Componente | Configuração do lab | Configuração de produção |
|---|---|---|
| Kafka | 1 broker em modo KRaft | Cluster com 3+ brokers, replicação |
| MQTT | EMQX sem autenticação | TLS + autenticação + ACLs |
| Bridge MQTT-Kafka | Python customizado | Kafka Connect MQTT Source |
| Schemas | JSON livre | Schema Registry + Avro/Protobuf |
| Tolerância a falhas | At-least-once básico | Idempotência, exactly-once, retry policies |
| Observabilidade | Logs e healthchecks | Prometheus + Grafana + alerting |

Essas simplificações são conscientes — viram pontos de evolução para os próximos projetos da série.

---

## 2. Arquitetura — componentes, decisões e trade-offs

### Visão geral do fluxo

```
[Node-RED] → [EMQX] → [Bridge Python] → [Kafka] → [Consumer Python] → [InfluxDB] → [Grafana]
```

### Componente 1 — Node-RED (simulador)

**Função:** simular sensores industriais publicando dados via MQTT em tempo real.

**Por que Node-RED:** ferramenta amplamente usada em IIoT real (edge gateways, integração de protocolos industriais). Tem cliente MQTT nativo, interface visual e permite simular múltiplos sensores com flows independentes. Reproduz, em ambiente de lab, o tipo de ferramenta que seria usada num gateway industrial real.

**Alternativas avaliadas:** scripts Python publicando direto via paho-mqtt foram considerados, mas Node-RED foi escolhido para manter o lab mais próximo de stacks IIoT reais e facilitar a expansão (qualquer pessoa pode editar o flow visualmente).

### Componente 2 — EMQX (broker MQTT)

**Função:** receber mensagens dos sensores e distribuir para os subscribers.

**Por que EMQX:** open source, alta performance, suporte a MQTT 3.1/3.1.1/5.0, dashboard web embutido. É um dos brokers MQTT mais maduros do mercado.

**Configurações relevantes:**
- QoS 1 nos sensores (at-least-once) — balanço entre confiabilidade e performance
- Sem autenticação no lab (em produção: TLS + usuário/senha + ACLs obrigatórios)
- Dashboard disponível em `localhost:18083` com credenciais `admin/public`

### Componente 3 — Bridge MQTT-Kafka (Python customizado)

**Função:** subscriber MQTT que republica cada mensagem no Kafka, preservando o tópico original como header.

**Por que Python customizado e não Kafka Connect:** curva de aprendizado mais suave para um lab de estudo, código transparente (todas as decisões são visíveis), suficiente para o volume do lab. Em produção, Kafka Connect MQTT Source seria a escolha mais robusta.

**Decisões de design da bridge:**
- **Particionamento por `{plant_id}-{equipment_id}-{sensor_id}`** garante ordenação cronológica por sensor (mesma chave sempre cai na mesma partição). Não usar `equipment_id` puro evita hot spots quando sensores diferentes do mesmo equipamento publicam em taxas diferentes.
- **Header `mqtt-topic`** preserva o caminho MQTT original para auditoria e debugging.
- **`compression.type=gzip`** reduz tráfego de rede e armazenamento (JSON com tags repetitivas comprime muito bem).
- **`acks=all`** garante durabilidade — mensagem só é confirmada após replicação (relevante quando houver cluster).
- **`linger.ms=10`** balanceia latência e throughput agrupando mensagens em pequenos batches.

### Componente 4 — Apache Kafka (1 broker, modo KRaft)

**Função:** log distribuído de eventos que desacopla produtores e consumidores.

**Por que Kafka:** padrão da indústria para streaming, suporta replay, permite múltiplos consumidores independentes lendo do mesmo tópico, alta retenção e throughput.

**Por que modo KRaft (sem Zookeeper):** desde Kafka 3.3, KRaft é a forma recomendada — elimina dependência de Zookeeper, simplifica operação, é o futuro do projeto. Versão do lab: `confluentinc/cp-kafka:7.6.0`.

**Detalhe técnico importante:** no modo KRaft, a variável `CLUSTER_ID` precisa ser um **UUID base64 válido de exatamente 16 bytes (22 caracteres após encoding)**. Strings arbitrárias rejeitam a inicialização. O UUID deve ser gerado com o utilitário oficial:

```bash
docker run --rm confluentinc/cp-kafka:7.6.0 kafka-storage random-uuid
```

**Configurações relevantes:**
- 1 broker (em produção: 3+ com replication factor 3)
- `KAFKA_AUTO_CREATE_TOPICS_ENABLE=true` (apenas conveniência de lab)
- `KAFKA_LOG_RETENTION_HOURS=24`
- Listener `PLAINTEXT://kafka:9092` para containers; `PLAINTEXT_HOST://localhost:29092` para acesso do host

### Componente 5 — Consumer Kafka-InfluxDB (Python customizado)

**Função:** consumir mensagens do tópico `industrial-sensors` e gravar no InfluxDB.

**Decisões de design:**
- **Consumer group `influxdb-writer`** permite escalabilidade horizontal futura (múltiplas instâncias balanceando partições).
- **Commit manual após gravação** garante at-least-once: a mensagem só é "confirmada" depois que o InfluxDB respondeu OK.
- **Mapeamento tags vs fields no InfluxDB:** todos os identificadores hierárquicos viram tags (indexáveis); apenas os valores numéricos viram fields.

### Componente 6 — InfluxDB 2.7

**Função:** banco de séries temporais para armazenar leituras dos sensores.

**Por que InfluxDB:** otimizado para time-series (compactação, queries por janela temporal, downsampling), Flux como linguagem de query, excelente integração com Grafana.

**Schema dos dados:**
- **Bucket:** `sensors` (retenção 7 dias)
- **Measurement:** `sensor_reading`
- **Tags:** `plant_id`, `area_id`, `equipment_id`, `equipment_type`, `sensor_id`, `unit`
- **Fields:** `value` (float), `quality` (string)
- **Timestamp:** original do sensor, não da gravação

**Credenciais do lab:** organização `industrial-lab`, token `industrial-lab-super-secret-token`, usuário `admin` / senha `industrial-lab-2026`.

### Componente 7 — Grafana

**Função:** dashboards em tempo real consultando o InfluxDB via Flux.

**Decisões de design do dashboard:**
- Provisionamento automático via arquivos em `grafana/provisioning/`
- Dashboard único "Industrial Streaming Lab — Mineração" com 4 linhas: KPIs do pipeline, gauges de estado, histórico do britador (multi-eixo), histórico da correia (3 painéis separados)
- Cores semânticas: verde/amarelo/vermelho em thresholds; laranja para temperatura, azul para pressão, vermelho para vibração, roxo para vazão, verde-água para velocidade
- Tooltip mode "Single" em todos os painéis
- Auto-refresh padrão de 5s

---

## 3. Especificações técnicas precisas

### Estrutura de tópicos MQTT

Hierarquia inspirada em ISA-95:

```
plant/{plant_id}/area/{area_id}/equipment/{equipment_id}/sensor/{sensor_id}
```

### Equipamento 1 — JAW-CRUSHER-01 (Britador de Mandíbula)

Área: `BRITAGEM`. Quatro sensores:

| Sensor ID | Faixa | Unidade | Frequência | Quality warning |
|---|---|---|---|---|
| `temperature_bearing` | 50–85 | °C | 1s | > 80 |
| `pressure_hydraulic` | 120–180 | bar | 1s | (sem warning) |
| `vibration` | 2–8 | mm/s | 500ms | > 7 |
| `power_motor` | 200–450 | kW | 1s | (sem warning) |

### Equipamento 2 — BELT-CONVEYOR-01 (Correia Transportadora)

Área: `TRANSPORTE`. Três sensores:

| Sensor ID | Faixa | Unidade | Frequência | Quality warning |
|---|---|---|---|---|
| `belt_speed` | 1.5–3.5 | m/s | 500ms | (sem warning) |
| `load_weight` | 50–300 | ton/h | 1s | (sem warning) |
| `temperature_roller` | 30–65 | °C | 2s | > 58 |

**Throughput agregado:** ~8.5 mensagens/segundo contínuas.

### Schema do payload JSON

```json
{
  "timestamp": "2026-05-13T14:30:00.123Z",
  "plant_id": "01",
  "area_id": "BRITAGEM",
  "equipment_id": "JAW-CRUSHER-01",
  "equipment_type": "jaw_crusher",
  "sensor_id": "vibration",
  "value": 5.23,
  "unit": "mm/s",
  "quality": "good"
}
```

### Configuração Kafka

- **Tópico:** `industrial-sensors`
- **Key:** `{plant_id}-{equipment_id}-{sensor_id}` (string)
- **Value:** payload JSON completo
- **Headers:** `mqtt-topic` com o caminho MQTT original

### Portas expostas (Docker Compose)

| Serviço | Porta host | Porta interna | Função |
|---|---|---|---|
| Node-RED | 1880 | 1880 | UI do simulador |
| EMQX | 1883 | 1883 | MQTT |
| EMQX | 8083 | 8083 | MQTT over WebSocket |
| EMQX | 18083 | 18083 | Dashboard |
| Kafka | 9092 | 9092 | Broker (containers) |
| Kafka | 9093 | 9093 | Controller (KRaft) |
| Kafka UI | 8080 | 8080 | Interface visual |
| InfluxDB | 8086 | 8086 | API e UI |
| Grafana | 3000 | 3000 | Dashboards |

### Frequências de publicação — racional

Foram escolhidas frequências mais altas do que o típico de SCADA pura (que costuma ser 1–10s para variáveis lentas) para **evidenciar a capacidade da arquitetura de processar volume sustentado**. Em cenário industrial real, essas frequências representam dados agregados de um *edge gateway* consolidando leituras de múltiplos pontos de coleta antes de transmiti-los à camada central.

---

## 4. Estado do projeto e próximos passos

### Estado atual — o que está pronto

- ✅ Pipeline funcional ponta a ponta: Node-RED → EMQX → Bridge → Kafka → Consumer → InfluxDB → Grafana
- ✅ Cenário de mineração com 2 equipamentos e 7 sensores
- ✅ Dashboard Grafana provisionado automaticamente (4 linhas, 10 painéis)
- ✅ Containers orquestrados via Docker Compose com healthchecks encadeados
- ✅ Documentação: README, architecture.md, learnings.md, topics-schema.md

### Pontos simplificados conscientemente (deixados para projetos futuros)

| Simplificação | Motivo | Onde evolui |
|---|---|---|
| 1 broker Kafka | Foco no fluxo, não na resiliência | Projeto 2 |
| Sem autenticação MQTT/Kafka | Foco didático | Projetos futuros |
| Sem Schema Registry | JSON é suficiente para demonstração | Possível projeto futuro |
| Bridge custom em vez de Kafka Connect | Transparência didática | Projeto 2 ou 3 |
| Sem detecção de anomalias | Pipeline básico primeiro | Projeto 3 |
| Sem replay/reprocessamento explícito | Conceito apresentado, não exercitado | Projeto 2 |

### Aprendizados técnicos consolidados

**1. Kafka KRaft Cluster ID:** precisa ser UUID base64 válido de 16 bytes. Strings comuns como "industrial-streaming-cluster-001" causam falha no preflight. Usar `kafka-storage random-uuid`.

**2. InfluxDB Data Explorer e tipos string:** o builder mode aplica `aggregateWindow(every: ..., fn: mean)` por padrão, o que falha em fields do tipo string (como `quality`). Para visualizar campos string, usar Script Editor com função `last()` ou `unique()`.

**3. Grafana Field Overrides com Placement Hidden:** ao usar `Placement: Hidden` em uma série, o Grafana esconde o eixo Y dela mas a série é plotada no eixo padrão restante, fazendo escalas serem aplicadas erradas. Para escala isolada de uma série, usar `Placement: Right` (cria eixo adicional) com Min/Max próprios, em vez de Hidden.

**4. Renomeação de séries em queries Flux para Grafana:** sem `keep(columns: ["_time", "_value", "sensor_id"])`, as séries aparecem na legenda com toda a estrutura de tags do InfluxDB, poluindo visualmente. O `keep` reduz o nome da série ao identificador útil.

**5. Healthchecks no Docker Compose são essenciais:** sem `condition: service_healthy` no `depends_on`, os consumidores tentam conectar antes do Kafka estar pronto e entram em loop de erro. O healthcheck adequado evita esse problema.

**6. Auto-criação de tópicos no Kafka:** com `KAFKA_AUTO_CREATE_TOPICS_ENABLE=true`, é normal o consumer reportar `UNKNOWN_TOPIC_OR_PART` no startup — o tópico só é criado na primeira publicação. O consumer "engata" automaticamente quando a primeira mensagem chega.

### Próximo projeto da série

**`lab-iiot-02-kafka-production`** evoluirá esta arquitetura para um ambiente mais próximo do que se vê em operações industriais reais, com foco em **resiliência e escala**:

- **Cluster Kafka multi-broker** com replicação real entre brokers, demonstrando comportamento de failover
- **Tópicos particionados** explorando paralelismo de processamento e distribuição de carga
- **Replication factor adequado** e configurações de ISR (In-Sync Replicas)
- **Monitoramento do cluster** via JMX Exporter + Prometheus, com painéis Grafana dedicados a métricas Kafka (throughput, lag de consumidores, saúde dos brokers)
- **Demonstrações práticas de tolerância a falhas** — comportamento quando brokers caem, consumidores travam, ou mensagens chegam fora de ordem

A bridge e o consumer Python serão preservados. O simulador Node-RED continua o mesmo. O foco da evolução é estritamente a camada Kafka e sua operabilidade.

---

*Documento mantido como referência técnica viva. Última revisão: maio/2026.*
