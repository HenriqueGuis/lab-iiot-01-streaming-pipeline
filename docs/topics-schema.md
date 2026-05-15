# Schemas de Tópicos — MQTT, Kafka e InfluxDB

Este documento descreve a estrutura de tópicos e payloads usados no projeto. Serve como referência para quem for adaptar o lab, integrar novos sensores ou consumir os dados em sistemas externos.

---

## Tópicos MQTT

### Padrão de nomenclatura

Os tópicos MQTT seguem a hierarquia de ativos comum em ambientes industriais, inspirada no padrão ISA-95:

```
plant/{plant_id}/area/{area_id}/equipment/{equipment_id}/sensor/{sensor_id}
```

**Por que essa estrutura:**

- Reflete a hierarquia física da planta industrial
- Permite subscribe com wildcards em qualquer nível
- Facilita filtragem por área, equipamento ou tipo de sensor
- É próxima do padrão ISA-95 usado em automação industrial

### Exemplos de tópicos do cenário de mineração

**Área BRITAGEM — Britador de mandíbula (JAW-CRUSHER-01):**

```
plant/01/area/BRITAGEM/equipment/JAW-CRUSHER-01/sensor/temperature_bearing
plant/01/area/BRITAGEM/equipment/JAW-CRUSHER-01/sensor/pressure_hydraulic
plant/01/area/BRITAGEM/equipment/JAW-CRUSHER-01/sensor/vibration
plant/01/area/BRITAGEM/equipment/JAW-CRUSHER-01/sensor/power_motor
```

**Área TRANSPORTE — Correia transportadora (BELT-CONVEYOR-01):**

```
plant/01/area/TRANSPORTE/equipment/BELT-CONVEYOR-01/sensor/belt_speed
plant/01/area/TRANSPORTE/equipment/BELT-CONVEYOR-01/sensor/load_weight
plant/01/area/TRANSPORTE/equipment/BELT-CONVEYOR-01/sensor/temperature_roller
```

### Wildcards MQTT úteis

O MQTT suporta dois wildcards para subscribe:

- **`+`** (sinal de mais) — substitui exatamente **um nível** do tópico
- **`#`** (cerquilha) — substitui **um ou mais níveis** (apenas no final)

| Wildcard | Significado prático |
|---|---|
| `plant/01/#` | Todos os sensores da planta 01 |
| `plant/+/area/BRITAGEM/#` | Toda a área BRITAGEM (qualquer planta) |
| `plant/+/area/+/equipment/+/sensor/vibration` | Todos os sensores de vibração de qualquer equipamento |
| `plant/01/area/BRITAGEM/equipment/JAW-CRUSHER-01/sensor/+` | Todos os sensores do britador |
| `plant/+/area/+/equipment/+/sensor/+` | Todos os sensores de todas as plantas (usado pela bridge do projeto) |

### Payload MQTT (JSON)

Estrutura padrão das mensagens publicadas:

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

**Descrição dos campos:**

| Campo | Tipo | Obrigatório | Descrição |
|---|---|---|---|
| `timestamp` | string (ISO 8601) | Sim | Momento da leitura no sensor |
| `plant_id` | string | Sim | Identificador da planta |
| `area_id` | string | Sim | Identificador da área dentro da planta |
| `equipment_id` | string | Sim | Identificador único do equipamento |
| `equipment_type` | string | Sim | Tipo do equipamento (ex: `jaw_crusher`, `belt_conveyor`) |
| `sensor_id` | string | Sim | Identificador do sensor |
| `value` | number | Sim | Valor lido pelo sensor |
| `unit` | string | Sim | Unidade de medida (ex: `celsius`, `bar`, `mm/s`) |
| `quality` | string | Sim | Qualidade da leitura: `good`, `warning`, `uncertain`, `bad` |

### Sobre o campo `quality`

O campo `quality` carrega a **semântica operacional** da leitura, não apenas a saúde do sensor:

| Valor | Significado | Quando aparece |
|---|---|---|
| `good` | Leitura dentro da faixa esperada de operação | Default |
| `warning` | Valor próximo de limite de alerta | Configurável por sensor (ex: `temperature_bearing` > 80°C) |
| `uncertain` | Reservado para uso futuro | Não usado no lab atual |
| `bad` | Reservado para uso futuro | Não usado no lab atual |

Esse campo viabiliza queries de "quantas leituras em zona de atenção nas últimas X horas" sem precisar reaplicar lógica de threshold no consumer.

### QoS recomendado

- **QoS 1 (at-least-once):** padrão do lab — garante entrega com possibilidade de duplicação
- **QoS 0:** apenas para dados não críticos onde perda é aceitável
- **QoS 2 (exactly-once):** evitar quando possível — alta sobrecarga de rede para benefício marginal

---

## Tópicos Kafka

### Tópico principal

- **Nome:** `industrial-sensors`
- **Particionamento:** padrão por chave (key-based)
- **Auto-criação:** habilitada no lab (`KAFKA_AUTO_CREATE_TOPICS_ENABLE=true`)
- **Retenção:** 24h no lab / 7–30 dias em produção

### Estrutura da mensagem Kafka

| Campo | Conteúdo |
|---|---|
| **Key** | `{plant_id}-{equipment_id}-{sensor_id}` (string) |
| **Value** | Payload JSON original (igual ao MQTT) |
| **Headers** | `mqtt-topic`: caminho MQTT original |

### Por que essa estrutura de Key

A chave de particionamento determina em qual partição cada mensagem é gravada. Usar `{plant_id}-{equipment_id}-{sensor_id}` tem três benefícios:

1. **Ordenação cronológica preservada por sensor** — mensagens do mesmo sensor sempre caem na mesma partição, garantindo ordem
2. **Paralelismo entre sensores diferentes** — sensores distintos podem ser processados em partições distintas
3. **Distribuição balanceada** — chave composta evita hot spots em partições

**Por que não usar apenas `equipment_id` como key?** Sensores diferentes do mesmo equipamento podem ter taxas de publicação muito diferentes. Se um sensor publica 4x mais que outro, a partição daquele equipamento fica desbalanceada. Granularidade no nível do sensor distribui melhor a carga.

### Exemplo de mensagem Kafka

```
Topic:      industrial-sensors
Partition:  3
Offset:     127845
Key:        01-JAW-CRUSHER-01-vibration
Value:      {"timestamp":"2026-05-13T14:30:00.123Z","plant_id":"01",...}
Headers:    mqtt-topic: plant/01/area/BRITAGEM/equipment/JAW-CRUSHER-01/sensor/vibration
```

### Preservação do tópico MQTT no header

O caminho MQTT original é preservado no header `mqtt-topic` da mensagem Kafka. Isso permite:

- **Auditoria** — saber a origem exata da mensagem
- **Debugging** — identificar de qual hierarquia veio o dado
- **Roteamento futuro** — consumidores podem rotear baseado no caminho original sem precisar reconstruí-lo das tags

---

## Schema do InfluxDB

### Measurement

- **Nome:** `sensor_reading`
- **Bucket:** `sensors`
- **Organização:** `industrial-lab`
- **Retenção do bucket:** 7 dias

### Tags (indexáveis para queries)

| Tag | Origem | Tipo |
|---|---|---|
| `plant_id` | Payload JSON | string |
| `area_id` | Payload JSON | string |
| `equipment_id` | Payload JSON | string |
| `equipment_type` | Payload JSON | string |
| `sensor_id` | Payload JSON | string |
| `unit` | Payload JSON | string |

### Fields (não-indexáveis, contêm os valores)

| Field | Origem | Tipo |
|---|---|---|
| `value` | Payload JSON | float |
| `quality` | Payload JSON | string |

### Timestamp

Usa o `timestamp` do payload original (precisão de milissegundos), **não** o momento da gravação no InfluxDB. Isso preserva a temporalidade real da leitura, independente de atrasos no pipeline.

### Decisão tags vs fields

Por que `value` e `quality` são fields (não-indexáveis) e o resto é tag (indexável)?

- **Tags são indexadas** — boas para filtrar (`WHERE equipment_id = "X"`), mas têm overhead de armazenamento. Identificadores hierárquicos com cardinalidade baixa funcionam bem como tags.
- **Fields não são indexados** — guardam valores numéricos e textuais que mudam constantemente. `value` muda a cada leitura; transformá-lo em tag exploderia a cardinalidade.
- **`quality` como field e não tag:** decisão consciente. Embora tenha cardinalidade baixa, raramente é usado como filtro principal (geralmente filtra-se por sensor primeiro). Manter como field é mais idiomático.

---

## Exemplos de queries Flux

### Última leitura de cada sensor do britador

```flux
from(bucket: "sensors")
  |> range(start: -5m)
  |> filter(fn: (r) => r._measurement == "sensor_reading")
  |> filter(fn: (r) => r.equipment_id == "JAW-CRUSHER-01")
  |> filter(fn: (r) => r._field == "value")
  |> last()
```

### Vibração média do britador na última hora

```flux
from(bucket: "sensors")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "sensor_reading")
  |> filter(fn: (r) => r.equipment_id == "JAW-CRUSHER-01")
  |> filter(fn: (r) => r.sensor_id == "vibration")
  |> filter(fn: (r) => r._field == "value")
  |> aggregateWindow(every: 10s, fn: mean, createEmpty: false)
```

### Throughput do pipeline (mensagens/segundo)

```flux
from(bucket: "sensors")
  |> range(start: -5m)
  |> filter(fn: (r) => r._measurement == "sensor_reading")
  |> filter(fn: (r) => r._field == "value")
  |> aggregateWindow(every: 1s, fn: count, createEmpty: false)
  |> group()
  |> aggregateWindow(every: 1s, fn: sum, createEmpty: false)
  |> mean()
```

### Contar sensores ativos no intervalo

```flux
from(bucket: "sensors")
  |> range(start: -5m)
  |> filter(fn: (r) => r._measurement == "sensor_reading")
  |> filter(fn: (r) => r._field == "value")
  |> keep(columns: ["sensor_id"])
  |> group()
  |> distinct(column: "sensor_id")
  |> count()
```

### Detectar leituras em zona de atenção

```flux
from(bucket: "sensors")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "sensor_reading")
  |> filter(fn: (r) => r._field == "quality")
  |> filter(fn: (r) => r._value == "warning")
  |> group(columns: ["sensor_id"])
  |> count()
```

---

## Resumo das convenções

| Camada | Identidade | Granularidade | Particionamento |
|---|---|---|---|
| MQTT | Tópico hierárquico (5 níveis) | Por sensor | N/A |
| Kafka | Tópico único `industrial-sensors` | Por mensagem | Por `{plant_id}-{equipment_id}-{sensor_id}` |
| InfluxDB | Measurement `sensor_reading` | Por ponto temporal | Por tags (indexação interna) |

Essa convenção uniforme entre camadas facilita rastreabilidade e auditoria. Qualquer mensagem pode ser localizada do MQTT ao InfluxDB usando os mesmos identificadores hierárquicos.
