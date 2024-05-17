from kafka import KafkaConsumer
import io
import imageio

consumer = KafkaConsumer(
    'compressed-images',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda x: x  # binary data
)
print("Listening for compressed images on Kafka...")
for message in consumer:
    print(f"Received compressed image data: {len(message.value)} bytes")
    image_data = message.value
    image = imageio.imread(io.BytesIO(image_data), format='JP2')  # JPEG2000 format

    # print the size of the image data
    print(f"Received compressed image with size {len(image_data)} bytes")
