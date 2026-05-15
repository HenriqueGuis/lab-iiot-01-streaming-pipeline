"""
═══════════════════════════════════════════════════════════════
Bridge MQTT → Kafka
═══════════════════════════════════════════════════════════════
Lê mensagens do broker MQTT (EMQX) e publica no Kafka.

- Subscribe em todos os tópicos de sensores (wildcard)
- Mantém o tópico MQTT original como header da mensagem Kafka
- Particionamento por equipamento+sensor para ordenação consistente
═══════════════════════════════════════════════════════════════
"""

import json
import logging
import os
import signal
import sys
from typing import Any

import paho.mqtt.client as mqtt
from confluent_kafka import Producer

# ─────────────────────────────────────────────────────────────
# Configuração via variáveis de ambiente
# ─────────────────────────────────────────────────────────────
MQTT_BROKER = os.getenv("MQTT_BROKER", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC_PATTERN = os.getenv(
    "MQTT_TOPIC_PATTERN", "plant/+/area/+/equipment/+/sensor/+"
)
MQTT_CLIENT_ID = os.getenv("MQTT_CLIENT_ID", "mqtt-kafka-bridge")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "industrial-sensors")

# ─────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("bridge")


# ─────────────────────────────────────────────────────────────
# Kafka Producer
# ─────────────────────────────────────────────────────────────
def create_kafka_producer() -> Producer:
    """Cria e retorna um Kafka Producer com configurações otimizadas."""
    config = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "client.id": "mqtt-kafka-bridge",
        "compression.type": "gzip",
        "linger.ms": 10,
        "batch.size": 16384,
        "acks": "all",
    }
    logger.info(f"Conectando ao Kafka em {KAFKA_BOOTSTRAP_SERVERS}")
    return Producer(config)


def kafka_delivery_callback(err: Any, msg: Any) -> None:
    """Callback chamado após tentativa de entrega ao Kafka."""
    if err is not None:
        logger.error(f"Falha na entrega ao Kafka: {err}")
    else:
        logger.debug(
            f"Entregue ao Kafka: topic={msg.topic()} "
            f"partition={msg.partition()} offset={msg.offset()}"
        )


# ─────────────────────────────────────────────────────────────
# Lógica de transformação MQTT → Kafka
# ─────────────────────────────────────────────────────────────
def extract_kafka_key(mqtt_topic: str) -> str:
    """
    Extrai uma chave estável para particionamento Kafka.
    Garante que mensagens do mesmo sensor vão sempre para a mesma partição.

    Exemplo:
        Input:  plant/01/area/A1/equipment/PUMP-001/sensor/temperature
        Output: 01-PUMP-001-temperature
    """
    parts = mqtt_topic.split("/")
    try:
        plant_id = parts[1]
        equipment_id = parts[5]
        sensor_id = parts[7]
        return f"{plant_id}-{equipment_id}-{sensor_id}"
    except IndexError:
        logger.warning(f"Tópico fora do padrão esperado: {mqtt_topic}")
        return mqtt_topic


# ─────────────────────────────────────────────────────────────
# Callbacks MQTT
# ─────────────────────────────────────────────────────────────
def on_mqtt_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        logger.info(f"Conectado ao MQTT broker {MQTT_BROKER}:{MQTT_PORT}")
        client.subscribe(MQTT_TOPIC_PATTERN, qos=1)
        logger.info(f"Subscrito em: {MQTT_TOPIC_PATTERN}")
    else:
        logger.error(f"Falha ao conectar ao MQTT (código: {rc})")


def on_mqtt_message(client, userdata, msg):
    """Recebe mensagem do MQTT e publica no Kafka."""
    producer: Producer = userdata["producer"]

    try:
        # Validar payload JSON
        payload = msg.payload.decode("utf-8")
        json.loads(payload)  # apenas valida; reusa o payload original

        # Determinar chave de particionamento
        key = extract_kafka_key(msg.topic)

        # Publicar no Kafka com header indicando o tópico MQTT original
        producer.produce(
            topic=KAFKA_TOPIC,
            key=key,
            value=payload,
            headers={"mqtt-topic": msg.topic.encode("utf-8")},
            callback=kafka_delivery_callback,
        )
        producer.poll(0)

        logger.debug(f"Encaminhado: {msg.topic} → Kafka:{KAFKA_TOPIC} (key={key})")

    except json.JSONDecodeError:
        logger.warning(f"Payload não é JSON válido: {msg.topic}")
    except Exception as e:
        logger.error(f"Erro ao processar mensagem: {e}")


def on_mqtt_disconnect(client, userdata, rc, properties=None):
    if rc != 0:
        logger.warning(f"Desconectado inesperadamente do MQTT (código: {rc})")


# ─────────────────────────────────────────────────────────────
# Encerramento gracioso
# ─────────────────────────────────────────────────────────────
def setup_signal_handlers(mqtt_client: mqtt.Client, producer: Producer) -> None:
    """Configura handlers para encerramento limpo (SIGINT/SIGTERM)."""

    def shutdown_handler(signum, frame):
        logger.info(f"Sinal recebido ({signum}), encerrando bridge...")
        mqtt_client.disconnect()
        mqtt_client.loop_stop()
        producer.flush(timeout=5)
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)


# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────
def main():
    logger.info("Iniciando MQTT-Kafka Bridge")
    logger.info(f"  MQTT: {MQTT_BROKER}:{MQTT_PORT} (pattern: {MQTT_TOPIC_PATTERN})")
    logger.info(f"  Kafka: {KAFKA_BOOTSTRAP_SERVERS} (topic: {KAFKA_TOPIC})")

    producer = create_kafka_producer()

    mqtt_client = mqtt.Client(
        client_id=MQTT_CLIENT_ID,
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        userdata={"producer": producer},
    )
    mqtt_client.on_connect = on_mqtt_connect
    mqtt_client.on_message = on_mqtt_message
    mqtt_client.on_disconnect = on_mqtt_disconnect

    setup_signal_handlers(mqtt_client, producer)

    try:
        mqtt_client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
        mqtt_client.loop_forever()
    except Exception as e:
        logger.error(f"Erro fatal na bridge: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
