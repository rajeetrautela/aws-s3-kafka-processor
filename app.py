import requests
from flask import Flask, request, render_template, redirect, url_for, flash
# import boto3
import uuid
import json
from kafka import KafkaProducer
import redis
import os
from dotenv import load_dotenv
import logging
import psutil
import subprocess
from minio import Minio
from minio.notificationconfig import (
    NotificationConfig,
    QueueConfig
)
import tempfile

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

load_dotenv()

class FileUploadApp:
    def __init__(self):
        self.app = Flask(__name__)
        self.app.secret_key = os.urandom(24)

        # Initialize clients

        #s3 client
        # self.s3 = boto3.client(
        #     's3',
        #     aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        #     aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        #     region_name=os.getenv("AWS_REGION")
        # )

        # minio client
        self.minio_client = Minio(
                            os.getenv("MINIO_HOST"),
                            access_key=os.getenv("MINIO_ACCESS_KEY"),
                            secret_key=os.getenv("MINIO_SECRET_KEY"),
                            secure=False
                        )
                        
        bucket_name = os.getenv("MINIO_BUCKET_NAME")
        if not self.minio_client.bucket_exists(bucket_name):
            self.minio_client.make_bucket(bucket_name)
            print(f"✅ Bucket '{bucket_name}' created")
        else:
            print(f"ℹ️ Bucket '{bucket_name}' already exists")

        self.minio_queue_config = QueueConfig(
                                    queue=os.getenv("MINIO_QUEUE_ARN"),  # This should match the identifier you registered via `mc event add`
                                    events=["s3:ObjectCreated:Put"]
                                )
        notification_config = NotificationConfig(queue_config_list=[self.minio_queue_config])

        self.minio_client.set_bucket_notification(bucket_name, notification_config)
    
        self.producer = KafkaProducer(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        self.redis_client = redis.Redis(
            host=os.getenv("REDIS_HOST"),
            port=int(os.getenv("REDIS_PORT")),
            db=int(os.getenv("REDIS_DB"))
        )

        # Register routes
        self.register_routes()

#this uses psutil lib to prevent multiple creation of consumers and cause memory issues
    def is_consumer_running(self):
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            try:
                if proc.info['cmdline'] and 'consumer.py' in proc.info['cmdline']:
                    return True
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                pass
        return False

    def register_routes(self):
        # decorators
        @self.app.route('/', methods=['GET', 'POST'])
        def upload():
            if request.method == 'POST':
                file = request.files.get('file')
                if not file:
                    flash('No file part', 'danger')
                    return redirect(url_for('upload'))

                file_id = str(uuid.uuid4())
                file_name = f"{file_id}_{file.filename}"
                try:
                    # if using s3 uncomment below
                    # self.s3.upload_fileobj(file, os.getenv("S3_BUCKET"), file_name)

                    #using minio starts here
                    # Save uploaded file temporarily
                    with tempfile.NamedTemporaryFile(delete=False) as tmp:
                        file.save(tmp.name)
                        tmp_path = tmp.name

                    # Upload using MinIO client (assuming self.minio_client is initialized)
                    self.minio_client.fput_object(
                        bucket_name=os.getenv("MINIO_BUCKET_NAME"),  
                        object_name=file_name,              
                        file_path=tmp_path                 
                    )
                    # minio upload ends here

                    # Send message to Kafka
                    self.producer.send(os.getenv("KAFKA_TOPIC"), {'file_name': file_name})

                    flash('✅ File uploaded and Kafka notification sent!', 'success')

                except Exception as e:
                    flash(f'Upload failed: {e}', 'danger')

                finally:
                    # Clean up temp file
                    if os.path.exists(tmp_path):
                        os.remove(tmp_path)

                return redirect(url_for('upload'))

            return render_template('upload.html')

        @self.app.route('/files')
        def files():
            keys = self.redis_client.keys()
            file_list = [json.loads(self.redis_client.get(k)) for k in keys]
            return render_template('files.html', files=file_list)

        @self.app.route('/sns-notify', methods=['POST'])
        def sns_notify():
            data = request.get_json(silent=True)
            # If no JSON, try manually decoding raw data (MinIO sends text/plain sometimes)
            if data is None:
                try:
                    data = json.loads(request.data.decode('utf-8'))
                except Exception as e:
                    print(f"❌ Failed to parse payload: {e}")
                    return 'Bad Request', 400

            if not data:
                return 'No data received', 400
            try:
                print(f"📥 Notification received: {json.dumps(data, indent=2)}")

                if not self.is_consumer_running():
                    subprocess.Popen(['python3', 'consumer.py'])
                    print("🚀 Kafka consumer started")
                else:
                    print("⚠️ Kafka consumer already running")

                return 'Notification processed', 200
            except Exception as e:
                print(f"❌ Error handling notification: {e}")
                return 'Internal Server Error', 500
            return 'Invalid SNS message', 400

    def run(self):
        # Setting host='0.0.0.0' allows external access — e.g., from your browser or another container.
        self.app.run(host='0.0.0.0', port=5000, debug=True)

if __name__ == '__main__':
    app = FileUploadApp()
    app.run()
