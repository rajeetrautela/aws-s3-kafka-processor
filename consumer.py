from kafka import KafkaConsumer
import json
import redis
import os
from dotenv import load_dotenv

from models import Database  # import your Database class

load_dotenv()

class FileConsumer:
    def __init__(self):
        # Initialize Database
        self.db = Database()
        self.db.init_db()
        self.session = self.db.get_session()
        self.FileRecord = self.db.FileRecord
        
        # Initialize Redis
        self.redis_client = redis.Redis(
            host=os.getenv("REDIS_HOST"),
            port=int(os.getenv("REDIS_PORT")),
            db=int(os.getenv("REDIS_DB"))
        )

        # Initialize Kafka consumer
        self.consumer = KafkaConsumer(
            os.getenv("KAFKA_TOPIC"),
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
            group_id="file-consumer-group",  # Required to track offsets
            auto_offset_reset="earliest",    # Start from earliest if no offset exists
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )


    def process_messages(self):
        print("📥 Kafka Consumer started...")
        for msg in self.consumer:
            file_name = msg.value['file_name']

            # Save to DB
            record = self.FileRecord(file_name=file_name)
            self.session.add(record)
            self.session.commit()

            # Cache in Redis
            self.redis_client.set(file_name, json.dumps({'file_name': file_name}))

            print(f"✅ Processed file: {file_name}")

if __name__ == "__main__":
    consumer = FileConsumer()
    consumer.process_messages()
