import time
from kafka import KafkaConsumer
import json
import redis
import os
from dotenv import load_dotenv
from models import Database  # your Database class
from kafka.errors import NoBrokersAvailable
from sqlalchemy.exc import OperationalError
import sys

print("🚀 Consumer script started!", flush=True)

print("🔥 Consumer container started")
load_dotenv()
print("🌿 Environment variables loaded")

class FileConsumer:
    def __init__(self):
        # Initialize Database with retry
        self.db = Database()
        self._init_db_with_retry()

        self.session = self.db.get_session()
        self.FileRecord = self.db.FileRecord
        
        # Initialize Redis
        self.redis_client = redis.Redis(
            host=os.getenv("REDIS_HOST"),
            port=int(os.getenv("REDIS_PORT")),
            db=int(os.getenv("REDIS_DB"))
        )

        # Initialize Kafka consumer with retry
        self.consumer = self._init_kafka_consumer_with_retry()

    def _init_db_with_retry(self, retries=5, delay=5):
        while retries > 0:
            try:
                self.db.init_db()
                print("✅ Database initialized successfully")
                return
            except OperationalError as e:
                print(f"⚠️ DB connection failed: {e}. Retrying in {delay} seconds...")
                retries -= 1
                time.sleep(delay)
        raise Exception("❌ Could not connect to the DB after multiple retries")

    def _init_kafka_consumer_with_retry(self, retries=5, delay=5):
        while retries > 0:
            try:
                consumer = KafkaConsumer(
                    os.getenv("KAFKA_TOPIC"),
                    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
                    group_id="file-consumer-group",
                    auto_offset_reset="earliest",
                    enable_auto_commit=True,
                    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
                )
                print("✅ Kafka consumer connected successfully")
                return consumer
            except NoBrokersAvailable:
                print(f"⚠️ Kafka brokers not available, retrying in {delay} seconds...")
                retries -= 1
                time.sleep(delay)
        raise Exception("❌ Could not connect to Kafka brokers after multiple retries")

    def process_messages(self):
        print("📥 Kafka Consumer started...")
        try:
            for msg in self.consumer:
                file_name = msg.value['file_name']

                # Save to DB
                record = self.FileRecord(file_name=file_name)
                self.session.add(record)
                self.session.commit()

                # Cache in Redis
                self.redis_client.set(file_name, json.dumps({'file_name': file_name}))

                print(f"✅ Processed file: {file_name}")
        except Exception as e:
            print(f"❌ Unexpected error during message processing: {e}")
            # You can add more sophisticated error handling/retry here if you want

if __name__ == "__main__":
    consumer = FileConsumer()
    consumer.process_messages()
