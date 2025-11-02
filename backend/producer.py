import json
from kafka import KafkaProducer
import time

# Create producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Send messages
topic = 'test-topic'

while True:
    try:
        chat_msg = input("Type message: ")
        message = {'message': chat_msg}
        producer.send(topic, value=message)
        print(f"Sent: {message}")
        time.sleep(1)
    except KeyboardInterrupt:
        break

# Ensure all messages are sent
producer.flush()
producer.close()

print("\nAll messages sent successfully!")