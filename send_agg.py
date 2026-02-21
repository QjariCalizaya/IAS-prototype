import os, json, random
from kafka import KafkaProducer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

p = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

for _ in range(30):
    msg = {"product_id": random.randint(1, 100), "count": random.randint(1, 5)}
    p.send("aggregated_clicks", msg)

p.flush()
print("OK: sent 30 msgs to aggregated_clicks")
