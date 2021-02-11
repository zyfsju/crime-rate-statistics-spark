import json
from kafka import KafkaConsumer


consumer = KafkaConsumer(
    "police.service.calls",
    group_id=0,
    bootstrap_servers=["localhost:9092"],
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)

for message in consumer:
    print(f"offset {message.offset}", message.value)
