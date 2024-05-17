from kafka import KafkaConsumer

# Initialize the KafkaConsumer with the appropriate settings
consumer = KafkaConsumer(
    'raw-images',  # The name of the Kafka topic
    bootstrap_servers='localhost:9092',  # Address of your Kafka broker
    auto_offset_reset='earliest',  # Start reading at the earliest message
    enable_auto_commit=True,  # Automatically commit offsets
    group_id='test-large-image',  # Consumer group ID
    value_deserializer=lambda x: x,  # Deserialize messages as raw bytes
    fetch_max_bytes=10485760,
    max_partition_fetch_bytes=10485760  # Max bytes to fetch per partition request
)

# Loop to read messages from the topic
for message in consumer:
    print(f"Received message with size {len(message.value)} bytes from partition {message.partition} at offset {message.offset}")
