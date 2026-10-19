import json
import time
import random
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic_name = 'sensor_data'
print(f"Iniciando productor para el topic: {topic_name}")

try:
    while True:
        data = {
            "sensor_id": f"SENSOR_{random.randint(1, 5)}",
            "temperature": round(random.uniform(15.0, 35.0), 2),
            "humidity": round(random.uniform(40.0, 80.0), 2),
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }
        producer.send(topic_name, value=data)
        print(f"Sent: {data}")
        time.sleep(1)
except KeyboardInterrupt:
    print("\nDeteniendo productor...")
finally:
    producer.flush()
    producer.close()