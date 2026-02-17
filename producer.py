from confluent_kafka import Producer

# Kafka broker (external)
broker = 'localhost:19092'

# Configure producer
conf = {'bootstrap.servers': broker}
producer = Producer(**conf)

# Callback for delivery reports
def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# Produce messages
# topic = 'test-topic'
topic = 'mbust'

producer.produce(topic, value=f'Hello World again', callback=delivery_report)