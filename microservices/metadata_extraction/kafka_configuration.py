from aiokafka import AIOKafkaConsumer
import asyncio
from uuid import uuid4
from datetime import datetime
from cassandra_module import write_to_cassandra
import json
import uuid

async def start_consumers():
    print("Starting consumers...")
    # Consumer for images, handling data as bytes directly
    consumer_images = AIOKafkaConsumer(
        'compressed-images',
        bootstrap_servers='localhost:9092',
        fetch_max_bytes=10485760,
        max_partition_fetch_bytes=10485760,
        group_id="test-group-images"
    )

    # Consumer for metadata, handling data as JSON
    consumer_metadata = AIOKafkaConsumer(
        'image-metadata',
        bootstrap_servers='localhost:9092',
        group_id="test-group-metadata",
        heartbeat_interval_ms=3000,
        session_timeout_ms=30000,
        max_poll_interval_ms=300000,
        fetch_max_bytes=10485760,
        max_partition_fetch_bytes=10485760,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    await consumer_images.start()
    await consumer_metadata.start()

    print("Consumers started.")
    try:
        async for image_msg in consumer_images:
            print("Received an image message.")
            image_data = image_msg.value  # Now treated as raw bytes
            metadata = await fetch_metadata(consumer_metadata, image_msg)
            print("Received metadata before the if statement", metadata)
            if metadata:
                print("Received metadata.")
                image_id = metadata['image_id']
                drone_id = metadata['drone_id']
                timestamp = datetime.now()
                await write_to_cassandra(image_id, drone_id, image_data, timestamp, metadata)
                print(f"Processed and stored image and metadata for image_id {image_id}")
            else:
                print("Metadata not received.")
    finally:
        await consumer_images.stop()
        await consumer_metadata.stop()
        print("Consumers stopped.")

async def fetch_metadata(consumer_metadata, image_msg):
    async for metadata_msg in consumer_metadata:
        print("Received a metadata message:", metadata_msg.value)
        print(image_msg.key)
        metadata = metadata_msg.value
        if 'image_id' in metadata:
            print("Returning metadata from fetch_metadata:", metadata)
            return metadata
    return None
