"""
═══════════════════════════════════════════════════════════════
Consumer Kafka → InfluxDB
═══════════════════════════════════════════════════════════════
Lê mensagens do tópico Kafka 'industrial-sensors' e grava no
InfluxDB como séries temporais.

- Consumer group para escalabilidade
- Batch de gravação para reduzir IO
- Tratamento de erros sem perder mensagens
═══════════════════════════════════════════════════════════════
"""

import json
import logging
import os
import signal
import sys
from datetime import datetime
from typing import Any

from confluent_kafka import Consumer, KafkaError
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

# ─────────────────────────────────────────────────────────────
# Configuração
# ─────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "industrial-sensors")
KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "influxdb-writer")

INFLUXDB_URL = os.getenv("INFLUXDB_URL", "http://localhost:8086")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN", "")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG", "industrial-lab")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET", "sensors")

POLL_TIMEOUT_SECONDS = 1.0

# ─────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("consumer")


# ─────────────────────────────────────────────────────────────
# Kafka Consumer
# ─────────────────────────────────────────────────────────────
def create_kafka_consumer() -> Consumer:
    """Cria e retorna um Kafka Consumer configurado."""
    config = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": KAFKA_CONSUMER_GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "session.timeout.ms": 30000,
    }
    consumer = Consumer(config)
    consumer.subscribe([KAFKA_TOPIC])
    logger.info(f"Consumer subscrito em: {KAFKA_TOPIC}")
    return consumer


# ─────────────────────────────────────────────────────────────
# InfluxDB
# ─────────────────────────────────────────────────────────────
def create_influxdb_client() -> InfluxDBClient:
    """Cria e retorna um cliente InfluxDB."""
    logger.info(f"Conectando ao InfluxDB em {INFLUXDB_URL}")
    return InfluxDBClient(
        url=INFLUXDB_URL,
        token=INFLUXDB_TOKEN,
        org=INFLUXDB_ORG,
    )


def parse_message_to_point(payload: dict) -> Point:
    """
    Converte payload JSON em um Point InfluxDB.

    Estrutura do Point:
      - Measurement: 'sensor_reading'
      - Tags (indexáveis): plant_id, area_id, equipment_id, sensor_id
      - Fields: value (float), quality (string)
      - Timestamp: do payload (preciso ao milissegundo)
    """
    timestamp = datetime.fromisoformat(
        payload["timestamp"].replace("Z", "+00:00")
    )

    point = (
        Point("sensor_reading")
        .tag("plant_id", payload["plant_id"])
        .tag("area_id", payload["area_id"])
        .tag("equipment_id", payload["equipment_id"])
        .tag("sensor_id", payload["sensor_id"])
        .tag("unit", payload.get("unit", "unknown"))
        .field("value", float(payload["value"]))
        .field("quality", payload.get("quality", "good"))
        .time(timestamp, WritePrecision.MS)
    )
    return point


# ─────────────────────────────────────────────────────────────
# Loop principal
# ─────────────────────────────────────────────────────────────
class ConsumerState:
    """Encapsula o estado do consumer para shutdown gracioso."""
    def __init__(self):
        self.running = True


def consume_loop(consumer: Consumer, write_api: Any, state: ConsumerState) -> None:
    """Loop principal de consumo: polling do Kafka e escrita no InfluxDB."""
    message_count = 0

    while state.running:
        msg = consumer.poll(POLL_TIMEOUT_SECONDS)

        if msg is None:
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            logger.error(f"Erro no consumer: {msg.error()}")
            continue

        try:
            payload = json.loads(msg.value().decode("utf-8"))
            point = parse_message_to_point(payload)

            write_api.write(
                bucket=INFLUXDB_BUCKET,
                org=INFLUXDB_ORG,
                record=point,
            )

            consumer.commit(msg, asynchronous=False)
            message_count += 1

            if message_count % 100 == 0:
                logger.info(f"Processadas {message_count} mensagens")

        except json.JSONDecodeError as e:
            logger.warning(f"JSON inválido: {e}")
            consumer.commit(msg, asynchronous=False)
        except KeyError as e:
            logger.warning(f"Campo obrigatório faltando: {e}")
            consumer.commit(msg, asynchronous=False)
        except Exception as e:
            logger.error(f"Erro ao processar mensagem: {e}")
            # NÃO faz commit em caso de erro inesperado para tentar novamente

    logger.info(f"Loop encerrado. Total processado: {message_count}")


# ─────────────────────────────────────────────────────────────
# Shutdown
# ─────────────────────────────────────────────────────────────
def setup_signal_handlers(state: ConsumerState) -> None:
    def shutdown_handler(signum, frame):
        logger.info(f"Sinal recebido ({signum}), encerrando consumer...")
        state.running = False

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)


# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────
def main():
    logger.info("Iniciando Kafka-InfluxDB Consumer")
    logger.info(f"  Kafka: {KAFKA_BOOTSTRAP_SERVERS} (topic: {KAFKA_TOPIC})")
    logger.info(f"  InfluxDB: {INFLUXDB_URL} (bucket: {INFLUXDB_BUCKET})")

    state = ConsumerState()
    setup_signal_handlers(state)

    consumer = create_kafka_consumer()
    influx_client = create_influxdb_client()
    write_api = influx_client.write_api(write_options=SYNCHRONOUS)

    try:
        consume_loop(consumer, write_api, state)
    except Exception as e:
        logger.error(f"Erro fatal no consumer: {e}")
        sys.exit(1)
    finally:
        logger.info("Fechando conexões...")
        consumer.close()
        influx_client.close()
        logger.info("Encerrado com sucesso.")


if __name__ == "__main__":
    main()
