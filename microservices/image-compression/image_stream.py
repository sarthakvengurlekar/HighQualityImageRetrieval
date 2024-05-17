import os
from kafka import KafkaProducer
import imageio.v2 as imageio
import io
from PIL import Image

def send_images_to_kafka(image_directory, topic):
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda x: x
    )

    if not os.path.exists(image_directory):
        print(f"Directory not found: {image_directory}")
        return
    
    images_found = False
    print(f"Loading images from {image_directory}")
    for filename in os.listdir(image_directory):
        if filename.lower().endswith((".jpg", ".jpeg", ".png")):
            images_found = True
            print(f"Found image {filename}")
            image_path = os.path.join(image_directory, filename)
            try:
                # Read the image using imageio
                image = imageio.imread(image_path)

                # Convert the image using PIL to handle RGBA to RGB conversion if necessary
                pil_image = Image.fromarray(image)
                if pil_image.mode == 'RGBA':
                    pil_image = pil_image.convert('RGB')
                
                # Write the image to a bytes buffer in JPEG format
                buffer = io.BytesIO()
                pil_image.save(buffer, format='JPEG')
                image_bytes = buffer.getvalue()

                # Send the processed image to Kafka
                producer.send(topic, image_bytes)
                producer.flush()
                print(f"Sent {filename} to Kafka")
            except Exception as e:
                print(f"Error processing {filename}: {e}")


    if not images_found:
        print("No images found in directory.")

# Example usage
send_images_to_kafka('test_images/', 'raw-images')
