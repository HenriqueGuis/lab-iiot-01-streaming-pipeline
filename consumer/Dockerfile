# Dockerfile — Consumer Kafka-InfluxDB
FROM python:3.11-slim

WORKDIR /app

# Instalar dependências do sistema necessárias para confluent-kafka
RUN apt-get update && apt-get install -y --no-install-recommends \
    librdkafka-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copiar requirements e instalar dependências Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código da aplicação
COPY main.py .

# Executar como usuário não-root
RUN useradd -m -u 1000 appuser
USER appuser

CMD ["python", "-u", "main.py"]
