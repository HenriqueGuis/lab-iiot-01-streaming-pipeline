# Aprendizados Técnicos

Este documento consolida os principais aprendizados do desenvolvimento do **Industrial Streaming Lab**. Inclui tanto reflexões conceituais sobre as tecnologias envolvidas quanto problemas reais encontrados e resolvidos durante a construção.

---

## 🔄 MQTT vs Kafka — quando usar cada um

Uma das discussões mais importantes do projeto foi entender **por que usar MQTT + Kafka juntos**, em vez de apenas um deles. Não é redundância — os dois resolvem problemas diferentes.

### MQTT é melhor quando:

- Dispositivos de borda (edge) com recursos limitados de CPU e memória
- Conexões intermitentes (ex: equipamentos móveis, sensores em campo aberto)
- Muitos publishers com baixo throughput individual
- Latência mínima é prioridade absoluta
- Comunicação direta máquina-a-máquina (M2M)

### Kafka é melhor quando:

- Volumes massivos de dados agregados (alto throughput sustentado)
- Necessidade de replay histórico de eventos
- Múltiplos consumidores independentes lendo o mesmo fluxo
- Processamento downstream (analytics, ML, persistência em múltiplos destinos)
- Garantias de durabilidade são críticas

### Por que usar ambos juntos em IIoT

O padrão emergente em arquiteturas industriais modernas:

```
Sensores  →  MQTT  →  Bridge  →  Kafka  →  Múltiplos Consumidores
(edge)      (light)              (hub)     (analytics, ML, storage, dashboards)
```

- **MQTT:** ideal para a "última milha" entre sensores e um agregador
- **Kafka:** ideal como hub central de dados para distribuição e processamento

Os dois não competem — se reforçam. MQTT cobre o que Kafka não foi feito para cobrir (devices com recursos limitados em redes restritas), e Kafka cobre o que MQTT não foi feito para cobrir (retenção, replay, múltiplos consumidores independentes).

---

## 📦 Estruturação de tópicos

### Lição: hierarquia importa, mesmo quando parece exagero

Tópicos planos (`sensor1`, `sensor2`) parecem mais simples no início, mas viram pesadelo na escala. Hierarquia inspirada em ISA-95 (`plant/01/area/BRITAGEM/equipment/JAW-CRUSHER-01/sensor/vibration`) permite:

- **Filtragem natural por wildcard** — subscribe em `plant/01/#` traz tudo da planta
- **Auditoria por nível** — saber facilmente que dado veio de qual parte da planta
- **Manutenção quando equipamentos mudam** — adicionar/remover equipamentos não exige redesenhar a estrutura

Para um lab pequeno parece overhead, mas é justamente por ser pequeno que é o momento certo de aprender a fazer certo.

### Lição: chave de particionamento Kafka

**Erro comum:** usar apenas `equipment_id` como chave.

**Problema:** sensores diferentes do mesmo equipamento competem pela mesma partição. Se um equipamento tem 4 sensores e outro tem 7, e cada um publica em frequências diferentes, isso cria *hot spots* — partições mais carregadas que outras.

**Solução adotada:** chave composta `{plant_id}-{equipment_id}-{sensor_id}`.

- Distribui melhor a carga entre partições
- Mantém ordenação cronológica garantida **por sensor** (mensagens do mesmo sensor sempre na mesma partição)
- Permite consumidores trabalharem em paralelo em sensores diferentes

---

## ⚡ QoS no MQTT vs garantias no Kafka

### MQTT QoS

| QoS | Garantia | Uso típico |
|---|---|---|
| 0 | At most once | Sensores não críticos, alta frequência, perdas aceitáveis |
| 1 | At least once (com possíveis duplicatas) | Padrão para a maioria dos casos industriais |
| 2 | Exactly once (alta sobrecarga) | Transações financeiras, comandos críticos |

### Kafka

- **acks=0:** sem confirmação (rápido, mas pode perder)
- **acks=1:** apenas líder confirma (balanço)
- **acks=all:** todos os ISRs (In-Sync Replicas) confirmam (mais seguro)

### Lição: o pipeline tem garantia mais fraca que o elo mais fraco

Se você usa MQTT QoS 0, mesmo configurando Kafka com `acks=all` você pode perder mensagens **antes** delas chegarem ao Kafka. **Avalie o pipeline ponta a ponta**, não cada componente isoladamente.

No lab, foi adotado QoS 1 (at-least-once) no MQTT e `acks=all` no Kafka, com commit manual no consumer. A garantia agregada é at-least-once em todo o pipeline — pode haver duplicatas em condições de falha, mas não perda silenciosa.

---

## 🚀 Configurações de performance do Kafka Producer

### Batching

```python
"linger.ms": 10,     # esperar 10ms para acumular mensagens
"batch.size": 16384, # tamanho máximo do batch em bytes
```

**Lição:** `linger.ms=0` (default) tem latência menor, mas throughput pior. Para dados industriais que vêm em alta frequência, `linger.ms=10–50` melhora throughput sem impacto perceptível em latência para o tipo de uso de dashboards/analytics.

### Compressão

```python
"compression.type": "gzip",
```

**Lição:** dados industriais (JSON com tags repetitivas) comprimem muito bem. A relação benefício-custo é favorável: ganho significativo de rede e disco com overhead de CPU mínimo.

### Confiabilidade

```python
"acks": "all",
```

**Lição:** `acks=all` é essencial para dados industriais. A perda silenciosa de leituras pode comprometer análises e decisões operacionais — e é difícil detectar depois.

---

## 🐳 Docker Compose — lições aprendidas

### Healthchecks são obrigatórios em pipelines com dependências

Sem healthcheck, o Docker Compose não sabe se um serviço está realmente pronto. Resultado: serviços dependentes tentam conectar antes da hora e entram em loop de erro.

```yaml
healthcheck:
  test: ["CMD-SHELL", "kafka-topics --bootstrap-server localhost:9092 --list || exit 1"]
  interval: 15s
  timeout: 10s
  retries: 5
```

A diferença entre `service_started` (default) e `service_healthy` no `depends_on` faz diferença real:

- `service_started`: o container subiu (não significa que o serviço está pronto)
- `service_healthy`: passou no healthcheck (o serviço está respondendo)

No projeto, todos os serviços com dependentes usam `service_healthy`.

### Network compartilhada

Todos os containers precisam estar na mesma rede para se comunicarem por nome de serviço:

```yaml
networks:
  industrial-net:
    driver: bridge
```

**Detalhe importante:** dentro da rede Docker, os containers se comunicam pelos **nomes dos serviços** (`kafka:9092`, `emqx:1883`, `influxdb:8086`), não pelo `localhost`. Esta foi uma fonte comum de confusão ao configurar o datasource do Grafana — `localhost:8086` falha (o Grafana tenta acessar a si mesmo), `influxdb:8086` funciona.

### Volumes nomeados vs bind mounts

- **Volumes nomeados** (`kafka-data:`, `influxdb-data:`): persistência gerenciada pelo Docker, sobrevivem a `docker compose down` (mas não a `docker compose down -v`)
- **Bind mounts** (`./grafana/provisioning:`): para arquivos de configuração do projeto que precisam ser editáveis fora do container

---

## 🔧 Aprendizados específicos do desenvolvimento

Esta seção lista problemas reais encontrados durante a construção do lab e como foram resolvidos. Cada um virou uma "armadilha conhecida" — útil para quem for adaptar ou estender o projeto.

### 1. Kafka modo KRaft exige UUID base64 válido como Cluster ID

**Problema observado:** broker em loop de restart, com log repetindo:

```
Cluster ID string industrial-streaming-cluster-001 does not appear to be a valid UUID
```

**Causa:** desde Kafka 3.3, o modo KRaft exige que `CLUSTER_ID` seja um **UUID base64 válido decodável para exatamente 16 bytes** (22 caracteres após encoding). Strings descritivas comuns como `"industrial-streaming-cluster-001"` falham no preflight check do broker.

**Solução:** gerar o ID com o utilitário oficial do próprio Kafka:

```bash
docker run --rm confluentinc/cp-kafka:7.6.0 kafka-storage random-uuid
```

O retorno é um UUID válido (ex: `5L6g3nShT-eMCtK--X86sw`). Aplicar como `CLUSTER_ID` no `docker-compose.yml`.

### 2. Grafana Field Overrides com `Placement: Hidden` não cria escala isolada

**Problema observado:** ao plotar 3 séries com escalas muito diferentes (pressão em ~150 bar, temperatura em ~70°C, vibração em ~4 mm/s), a vibração estava sendo "achatada" no gráfico. Configurar `Min: 0` e `Max: 10` no override não corrigia.

**Diagnóstico:** `Placement: Hidden` no Axis esconde o eixo Y da série, **mas a série continua sendo plotada no eixo padrão restante** (no caso, o eixo da pressão em Bar). Os Min/Max definidos no override são ignorados porque a série não tem eixo próprio onde aplicá-los.

**Solução:** mudar `Placement: Hidden` para `Placement: Right`. Isso cria um terceiro eixo Y à direita do gráfico, dedicado à vibração. A série passa a ser plotada na escala correta (0–10 mm/s).

**Aprendizado mais geral:** `Hidden` ≠ "escala isolada sem mostrar eixo". Para escala isolada, é necessário ter eixo, mesmo que visualmente discreto.

### 3. Queries Flux precisam de `keep()` para legendas limpas no Grafana

**Problema observado:** ao plotar múltiplos sensores no mesmo painel, a legenda mostrava nomes longos como:

```
value {area_id="BRITAGEM", equipment_id="JAW-CRUSHER-01", plant_id="01", sensor_id="vibration", unit="mm/s"}
```

**Causa:** sem instrução contrária, o Grafana nomeia cada série com **toda a estrutura de tags** que veio do InfluxDB.

**Solução:** adicionar `keep(columns: ["_time", "_value", "sensor_id"])` antes do `aggregateWindow`. Isso descarta as tags desnecessárias e mantém apenas o `sensor_id` como identificador da série. A legenda passa a mostrar apenas `vibration`, `temperature_bearing`, etc.

**Bonus:** com séries renomeadas corretamente, os Field Overrides funcionam por nome simples (`Fields with name → vibration`), sem precisar referenciar a estrutura completa.

### 4. InfluxDB Data Explorer falha em fields do tipo string

**Problema observado:** ao tentar visualizar `_field == "quality"` no Data Explorer, retornava o erro:

```
unsupported input type for mean aggregate: string
```

**Causa:** o builder mode do Data Explorer aplica automaticamente `aggregateWindow(every: ..., fn: mean)`. A função `mean()` só opera em valores numéricos. Como `quality` é string (`"good"`, `"warning"`), a função quebra.

**Solução:** para fields string, usar o **Script Editor** (não o builder mode) e substituir `mean()` por funções compatíveis com strings como `last()`, `first()`, ou `unique()`:

```flux
from(bucket: "sensors")
  |> range(start: -5m)
  |> filter(fn: (r) => r._measurement == "sensor_reading")
  |> filter(fn: (r) => r._field == "quality")
  |> last()
```

**Aprendizado mais geral:** Data Explorer no builder mode assume cenário "numérico padrão". Para fields não-numéricos, sempre Script Editor.

### 5. Auto-criação de tópicos no Kafka — comportamento esperado no startup

**Problema aparente:** logo após `docker compose up`, o consumer reporta no log:

```
KafkaError{code=UNKNOWN_TOPIC_OR_PART, ...}
```

**Diagnóstico:** com `KAFKA_AUTO_CREATE_TOPICS_ENABLE=true`, o tópico `industrial-sensors` só é criado **quando a primeira mensagem é publicada**. Até lá, o consumer reporta o erro em loop. Não é um problema — o consumer "engata" automaticamente assim que o tópico nasce.

**Como saber se é problema real:** se o container do consumer estiver `Up` (não em `Restarting`), o loop de erro é benigno. Se estiver em `Restarting`, há outro problema.

**Solução em produção:** pré-criar tópicos explicitamente em vez de depender de auto-criação. Auto-criação é prática de lab/dev — em produção, criação de tópicos é parte do deploy.

---

## ⏱️ Latência fim a fim

> ⚠️ **Disclaimer:** as latências abaixo são **estimativas baseadas em literatura técnica e experiência geral**, não medições controladas neste lab específico. Servem como ordem de grandeza para entender onde estão os gargalos típicos.

Para um pipeline rodando em hardware modesto (laptop com Docker Desktop):

| Etapa | Latência esperada |
|---|---|
| Sensor → MQTT broker | < 5 ms |
| MQTT → Bridge (Python) | < 5 ms |
| Bridge → Kafka | 10–15 ms (com `linger.ms=10`) |
| Kafka → Consumer | < 10 ms |
| Consumer → InfluxDB | 5–15 ms |
| InfluxDB → Grafana (pull) | 1–5s (intervalo do refresh) |
| **Total ponta-a-ponta (sem o Grafana refresh):** | **30–60 ms** |
| **Total visível no dashboard:** | **1–5s (limitado pelo refresh)** |

**Lição:** o gargalo de "tempo real" no dashboard é o **intervalo de refresh do Grafana**, não o pipeline em si. Em sistemas que precisam de reação real-time (ex: alertas automáticos), o Grafana não é a interface adequada — o consumer pode disparar eventos diretamente para o sistema de notificação.

---

## 🔮 Próximas explorações

Áreas a aprofundar nas próximas iterações da série `lab-iiot-*`:

- **Cluster Kafka multi-broker** — comportamento de failover, eleição de líderes, ISR, replication factor (próximo projeto)
- **Particionamento e paralelismo de consumidores** — escalabilidade horizontal real
- **Monitoramento de Kafka com JMX + Prometheus** — métricas de cluster, lag de consumidores
- **Sparkplug B vs JSON puro** — vantagens em ambientes com bandwidth limitado
- **Detecção de anomalias com PySpark Structured Streaming** — média móvel, desvio padrão, Isolation Forest
- **Schema Registry + Avro** — contratos de dados versionados
- **Comparação de performance:** 1 broker vs cluster de 3 brokers vs 5 brokers

---

## 🔗 Referências úteis

- [MQTT Best Practices (HiveMQ)](https://www.hivemq.com/mqtt/mqtt-best-practices/)
- [Kafka: The Definitive Guide (livro Confluent)](https://www.confluent.io/resources/kafka-the-definitive-guide-v2/)
- [InfluxDB schema design best practices](https://docs.influxdata.com/influxdb/v2/write-data/best-practices/schema-design/)
- [ISA-95 standards](https://www.isa.org/standards-and-publications/isa-standards/isa-standards-committees/isa95)
- [Kafka KRaft mode documentation](https://kafka.apache.org/documentation/#kraft)
- [Grafana Field Overrides documentation](https://grafana.com/docs/grafana/latest/panels-visualizations/configure-overrides/)

---

*Documento mantido como referência viva. Última revisão: maio/2026.*
