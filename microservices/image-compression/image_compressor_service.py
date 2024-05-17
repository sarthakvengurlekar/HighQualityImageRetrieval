from fastapi import FastAPI
import uvicorn
from kafka import KafkaProducer, KafkaConsumer
import imageio
import io
import numpy as np
import asyncio
from PIL import Image

app = FastAPI()

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: x,
    max_request_size=10485760
)

# Initialize Kafka consumer
consumer = KafkaConsumer(
    'raw-images',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    group_id='image-compressor-service-group',
    value_deserializer=lambda x: x,
    max_partition_fetch_bytes=10485760
)

def process_image(image_data):
    print(f"Processing image of size {len(image_data)} bytes.")
    try:
        # Attempt to read with imageio first
        image = imageio.imread(io.BytesIO(image_data))
        print("Image processed with imageio.")
    except ValueError as e:
        print(f"imageio failed with error: {e}. Trying with PIL.")
        # Fall back to PIL for robust handling, especially for formats like JPEG
        image = np.array(Image.open(io.BytesIO(image_data)))
        print("Image processed with PIL.")

    if image.ndim == 2:  # Grayscale to RGB
        image = np.stack((image,) * 3, axis=-1)
    elif image.shape[2] == 4:  # RGBA to RGB
        image = image[..., :3]
    return image

@app.on_event("startup")
async def consume_and_process_images():
    print("Application started, waiting for images...")
    for message in consumer:
        print("Received image from Kafka.")
        image_data = message.value
        try:
            image = process_image(image_data)
            buffer = io.BytesIO()
            imageio.imwrite(buffer, image, format='JP2', compression=5)  # Using JPEG2000
            compressed_image = buffer.getvalue()
            producer.send('compressed-images', compressed_image)
            producer.flush()
            print("Image processed and sent back to Kafka.")
        except Exception as e:
            print(f"Failed to process image: {e}")
        await asyncio.sleep(0.01)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
