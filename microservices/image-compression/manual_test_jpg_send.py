from kafka import KafkaProducer
import os



def read_image(file_path):
    with open(file_path, 'rb') as file:
        return file.read()

def send_to_kafka(topic, image_bytes):
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda x: x,  # Sending bytes directly
        max_request_size=10485760,
        acks=1  # Wait for leader acknowledgment
    )
    future = producer.send(topic, image_bytes)
    result = future.get(timeout=60)  # Wait up to 60 seconds for the send to complete
    print(f"Sent image to Kafka topic {topic}, partition: {result.partition}, offset: {result.offset}")

if __name__ == "__main__":
    # Specify the path to your JPG file
    image_path = 'test_images/DJI_20240221121213_0001_V.jpg'
    if os.path.exists(image_path):
        image_bytes = read_image(image_path)
        print(f"Read image {image_path} of size {len(image_bytes)} bytes")
        # Kafka topic to send the image
        topic = 'raw-images'
        send_to_kafka(topic, image_bytes)
    else:
        print(f"Image file {image_path} not found.")
