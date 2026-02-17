from confluent_kafka import Consumer, KafkaError

# Kafka broker (external)
broker = 'localhost:19092'

# Configure consumer
conf = {
    'bootstrap.servers': broker,
    'group.id': 'test-group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(**conf)

# Subscribe to topic
topic = 'mbust'
consumer.subscribe([topic])

print("Consuming messages...")

try:
    while True:
        msg = consumer.poll(1.0)  # 1 second timeout
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break
        print(f"Received message: {msg.value().decode('utf-8')}")
finally:
    consumer.close()
 