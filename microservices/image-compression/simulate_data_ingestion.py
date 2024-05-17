from kafka import KafkaProducer
import json
import os
from PIL import Image
import datetime
import random
import uuid

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: x,  # Sending bytes directly
    max_request_size=10485760,  
    acks=1  
)

# Function to read image file as bytes
def read_image(file_path):
    with open(file_path, 'rb') as f:
        image_bytes = f.read()
    print(f"Read image {file_path} with size {len(image_bytes)} bytes")
    return image_bytes

# Function to load images and metadata from directory
def load_images_with_metadata(directory):
    image_data = []
    for filename in os.listdir(directory):
        if filename.lower().endswith((".jpg", ".jpeg", ".png")):
            image_path = os.path.join(directory, filename)
            image_bytes = read_image(image_path)
            metadata = {
                "filename": filename,
                "drone_id": random.randint(1, 100),  # Example: Assign random drone ID
                "timestamp": datetime.datetime.now().isoformat(),  # Current timestamp
                "location": f"{random.uniform(-90, 90)}, {random.uniform(-180, 180)}",  # Random lat-long coordinates
                "camera_settings": {"resolution": "1920x1080", "fps": random.choice([24, 30, 60])},  # Random fps selection
                "image_id": str(uuid.uuid4())
            }
            image_data.append((image_bytes, metadata))
    return image_data

# Function to send image and metadata to Kafka
def send_to_kafka(image_bytes, metadata):
    producer.send('raw-images', value=image_bytes)
    producer.send('image-metadata', value=json.dumps(metadata).encode('utf-8'))
    producer.flush()
    print(f"Sent image and metadata for {metadata['filename']}")

if __name__ == "__main__":
    test_images_directory = 'test_images/'
    image_data_with_metadata = load_images_with_metadata(test_images_directory)
    for image_bytes, metadata in image_data_with_metadata:
        send_to_kafka(image_bytes, metadata)
