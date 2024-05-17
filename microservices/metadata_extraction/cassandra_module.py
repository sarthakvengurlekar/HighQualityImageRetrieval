from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
import logging
from uuid import UUID

logger = logging.getLogger(__name__)

# Connect to the Cassandra cluster
cluster = Cluster(['localhost'])
session = cluster.connect('images_store')

async def write_to_cassandra(image_id, drone_id, image_blob, timestamp, metadata):
    try:

        image_id_uuid = UUID(image_id)
        camera_settings = metadata['camera_settings']
        camera_settings['fps'] = str(camera_settings['fps'])
        session.execute("""
            INSERT INTO images (image_id, drone_id, image_blob, timestamp)
            VALUES (%s, %s, %s, %s)
        """, (image_id_uuid, drone_id, image_blob, timestamp))
        session.execute("""
            INSERT INTO metadata (image_id, drone_id, location, timestamp, camera_settings)
            VALUES (%s, %s, %s, %s, %s)
        """, (image_id_uuid, drone_id, metadata['location'], timestamp, camera_settings))
        logger.info(f"Data for image_id {image_id} inserted successfully.")
    except Exception as e:
        logger.error(f"Failed to insert data for image_id {image_id}: {e}")

