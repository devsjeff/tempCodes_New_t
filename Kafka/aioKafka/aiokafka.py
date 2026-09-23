# pip install aiokafka

import asyncio, json
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

BOOTSTRAP = "localhost:9092"
TOPIC = "first_KafkaTopic"

async def main():
    # Create topic
    admin = AIOKafkaAdminClient(bootstrap_servers=BOOTSTRAP)
    await admin.start()
    try:
        await admin.create_topics([NewTopic(name=TOPIC, num_partitions=1, replication_factor=1)])
        print(f"Topic {TOPIC} created")
    except Exception as e:
        print(f"Topic exists or error: {e}")
    await admin.close()

    # Consumer task
    async def consume():
        consumer = AIOKafkaConsumer(
            TOPIC,
            bootstrap_servers=BOOTSTRAP,
            auto_offset_reset="earliest",
            group_id="test-group",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        )
        await consumer.start()
        print("Consumer started, waiting...")
        try:
            async for msg in consumer:
                print(f"message: {msg.value}")
                if msg.value.get("last") == True:
                    break
        finally:
            await consumer.stop()

    consumer_task = asyncio.create_task(consume())
    await asyncio.sleep(2)

    # Producer
    producer = AIOKafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    await producer.start()
    for i in range(5):
        data = {"id": i, "message": i, "last": i == 4}
        await producer.send_and_wait(TOPIC, data)
        print(f"sent: {data}")
        await asyncio.sleep(0.5)
    await producer.stop()

    await consumer_task
    print("finished")

asyncio.run(main())
