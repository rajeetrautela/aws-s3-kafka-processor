from flask import Flask, request, render_template, redirect, url_for, flash
import boto3
import uuid
import json
from kafka import KafkaProducer
import redis
import os
from dotenv import load_dotenv

load_dotenv()

class FileUploadApp:
    def __init__(self):
        self.app = Flask(__name__)
        self.app.secret_key = os.urandom(24)

        # Initialize clients
        self.s3 = boto3.client(
            's3',
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("AWS_REGION")
        )
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

    def register_routes(self):
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
                    self.s3.upload_fileobj(file, os.getenv("S3_BUCKET"), file_name)
                    self.producer.send(os.getenv("KAFKA_TOPIC"), {'file_name': file_name})
                    flash('✅ File uploaded and Kafka notification sent!', 'success')
                except Exception as e:
                    flash(f'Upload failed: {e}', 'danger')

                return redirect(url_for('upload'))

            return render_template('upload.html')

        @self.app.route('/files')
        def files():
            keys = self.redis_client.keys()
            file_list = [json.loads(self.redis_client.get(k)) for k in keys]
            return render_template('files.html', files=file_list)

    def run(self):
        self.app.run(debug=True)

if __name__ == '__main__':
    app = FileUploadApp()
    app.run()
