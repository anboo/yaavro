#!/bin/sh

echo "⏳ Waiting for Kafka to become available..."

# Ждём, пока Kafka поднимется и будет отвечать
until kafka-topics.sh --bootstrap-server kafka:29092 --list > /dev/null 2>&1; do
  echo "$(date) Kafka not ready yet..."
  sleep 1
done

echo "✅ Kafka is ready! Creating topic..."

kafka-topics.sh --create \
  --bootstrap-server kafka:29092 \
  --topic vitals \
  --partitions 3 \
  --replication-factor 1 \
  --if-not-exists

echo "✅ Topic 'vitals' created or already exists"
