from kafka import KafkaConsumer , KafkaProducer
from kafka.admin import KafkaAdminClient , NewTopic
import threading , time , json

Bootstrap = "localhost:9092"
TOPIC = "first_KafkaTopic"

admin = KafkaAdminClient(bootstrap_servers=Bootstrap)
try:
    admin.create_topics([NewTopic(name=TOPIC , num_partitions=1, replication_factor=1)])
    print (f"Topic {TOPIC} created")
except Exception as e:
    print(f"Topic exists or error: {e}")

def CreateConsumer():
    consumer = KafkaConsumer(TOPIC ,
                             bootstrap_servers=Bootstrap,
                             auto_offset_reset="earliest",
                             group_id="test group", 
                              value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                        )

    print("Consumer started, waiting...")
    for msg in consumer:
        print(f"message :{msg.value}")
        if msg.value.get("last")==True:
            break
    consumer.close()
t = threading.Thread(target=CreateConsumer , daemon=True)
t.start()
time.sleep(2)


producer = KafkaProducer(bootstrap_servers=Bootstrap ,value_serializer=lambda v: json.dumps(v).encode("utf-8"))
for i in range(5):
    DATA = {"id":i, "message":i, "last": i ==  4}
    producer.send(TOPIC , DATA )
    time.sleep(0.5)
producer.flush()
t.join(timeout=10)
print("finished")
