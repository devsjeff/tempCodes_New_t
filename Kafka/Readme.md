```markdown
# Web-Vector — Local Kafka + Postgres + Redis Setup

Dev environment for a first Kafka project. Single EC2 friendly.

---

## Stack
- **Kafka**: `apache/kafka:3.7.0` (KRaft mode, no ZooKeeper)
- **Postgres**: `pgvector/pgvector:pg16`
- **Redis**: `redis:7-alpine`
- All via `docker compose`

---

## Project Structure
```
.
├── docker-compose.yml
├── test_kafka.py
└── README.md
```

---

## Quick Start

```bash
# Start everything
docker compose up -d

# Wait until all show "healthy"
docker compose ps

# Tail kafka logs
docker compose logs -f kafka
```

---

## docker-compose.yml

```yaml

  kafka:
    image: apache/kafka:3.7.0
    container_name: webvector-kafka
    restart: unless-stopped
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
    volumes:
      - kafka_data:/var/lib/kafka/data
    healthcheck:
      test: ["CMD-SHELL", "/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list || exit 1"]
      interval: 10s
      timeout: 10s
      retries: 10
      start_period: 30s

volumes:
  kafka_data:
```

---

## Common Commands

```bash
# Start
docker compose up -d

# Status
docker compose ps

# Logs
docker compose logs -f kafka

# Stop (keep data)
docker compose down

# Stop + wipe data
docker compose down -v

# Shell into kafka
docker exec -it webvector-kafka bash

# List topics
docker exec -it webvector-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list

# Create topic
docker exec -it webvector-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create \
  --topic test-topic --partitions 1 --replication-factor 1

# Produce from CLI
docker exec -it webvector-kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic test-topic

# Consume from CLI
docker exec -it webvector-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic test-topic --from-beginning
```

---

## Python Client

Install: `pip install kafka-python`

```python
from kafka import KafkaProducer, KafkaConsumer
import json

BOOTSTRAP = "localhost:9092"
TOPIC = "test-topic"

# Producer
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)
producer.send(TOPIC, {"id": 1, "msg": "hello"})
producer.flush()

# Consumer
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP,
    auto_offset_reset="earliest",
    group_id="my-group",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)
for msg in consumer:
    print(msg.value)
```

Note: `kafka-python` is fine for learning. For prod, prefer `confluent-kafka`.

---

## JS Client

Install: `npm i kafkajs`

```js
const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['localhost:9092'],
});

// Producer
const producer = kafka.producer();
await producer.connect();
await producer.send({
  topic: 'test-topic',
  messages: [{ value: JSON.stringify({ id: 1, msg: 'hello' }) }],
});
await producer.disconnect();

// Consumer
const consumer = kafka.consumer({ groupId: 'my-group' });
await consumer.connect();
await consumer.subscribe({ topic: 'test-topic', fromBeginning: true });
await consumer.run({
  eachMessage: async ({ message }) => {
    console.log(JSON.parse(message.value.toString()));
  },
});
```

---

## Address Rule (most common bug)

| Where app runs          | Advertised listener            |
|-------------------------|--------------------------------|
| Laptop (outside docker) | `PLAINTEXT://localhost:9092`   |
| Inside docker           | `PLAINTEXT://kafka:9092`       |

Kafka tells clients "connect here". If wrong → `NoBrokersAvailable`.

---

## What to Avoid in Production

1. **Single broker** — dev only. Prod: 3+ brokers, replication factor 3.
2. **`AUTO_CREATE_TOPICS_ENABLE: true`** — hides typos. Prod: `false`.
3. **`PLAINTEXT`** — no auth, no encryption. Prod: `SASL_SSL`.
4. **No resource limits** — set `mem_limit`, `cpus` or use k8s.
5. **`localhost` in advertised listeners** — breaks remote clients.
6. **`replication_factor=1`** — data loss on broker crash.
7. **No monitoring** — use Prometheus + JMX exporter or Confluent Control Center.
8. **One EC2 for everything** — fine for demo, risky for real traffic.
9. **No backups** — Postgres volumes need `pg_dump` schedule.
10. **Default passwords** — change `POSTGRES_PASSWORD`, use secrets.
11. **No retention policy** — set `log.retention.hours` so disk doesn't fill.
12. **Running as root in container** — use non-root user.
13. **`latest` tags** — pin versions.
14. **No healthchecks** — you already have them, keep them.
15. **Exposing ports publicly** — EC2 security group: only open what's needed.

---

## EC2 Deploy (single box, quick)

1. Install `docker` + `docker compose`.
2. Clone repo, put secrets in `.env`.
3. Set Kafka `KAFKA_ADVERTISED_LISTENERS`:
   - App also in docker → `PLAINTEXT://kafka:9092`
   - App on host → `PLAINTEXT://<EC2_PUBLIC_IP>:9092`
4. Security group: `22` (your IP), `80/443` (public). DB/Redis/Kafka **closed**.
5. `docker compose up -d`.
6. Reverse proxy (nginx/caddy) for HTTPS.

---

## Sanity Checklist

- [ ] `docker compose ps` → all `healthy`
- [ ] Python producer/consumer round-trip works
- [ ] JS producer/consumer round-trip works
- [ ] `docker compose down` then `up -d` → data persists
- [ ] Topic list shows your topic

---

## Golden Rule

Give the bootstrap address the client can actually reach.
Verify everything locally before shipping to prod.
```
