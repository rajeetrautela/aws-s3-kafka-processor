how to run-
    in the project root just run python3 app.py it will boot your application servers
    to start consumer run python3 consumer.py (if manually you want else it will start in the same server as app)

    docker-compose up --build (to run zooker and kafka server in docker)
    docker-compose down -v  (remove images)

---------------------------------------
update you docker file if you want to run whole setup in container

version: '3.8'

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.6.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
    ports:
      - "2181:2181"

  kafka:
    image: confluentinc/cp-kafka:7.6.0
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1

  redis:
    image: redis:7-alpine
    ports:
      - "6380:6379"

  db:
    image: postgres:15
    restart: always
    environment:
      POSTGRES_USER: go_test_db
      POSTGRES_PASSWORD: go_test_db
      POSTGRES_DB: flask_kafka_app
    ports:
      - "5433:5432"

  app:
    build: .
    depends_on:
      - kafka
      - redis
      - db
    ports:
      - "5005:5000"
    env_file:
      - .env.docker
  consumer:
    build: .
    depends_on:
      - kafka
      - redis
      - db
    env_file:
      - .env.docker
    command:  python -u consumer.py


    ---------------------- how to switch between s3 and minio -----------------------

        use this function body for s3 for minio everything is already set.
        
        def sns_notify():
    # If no JSON, try manually decoding raw data (SNS may send text/plain)
            # if data is None:
            #     try:
            #         data = json.loads(request.data.decode('utf-8'))
            #     except Exception as e:
            #         print(f"❌ Failed to parse SNS payload: {e}")
            #         return 'Bad Request', 400

            # if not data:
            #     return 'No data received', 400

            # # Handle subscription confirmation from SNS
            # if data.get('Type') == 'SubscriptionConfirmation':
            #     subscribe_url = data.get('SubscribeURL')
            #     print(f'🔔 Confirming subscription: {subscribe_url}')
            #     try:
            #         # Confirm subscription by calling the URL SNS provides
            #         requests.get(subscribe_url)
            #         return 'Subscription confirmed', 200
            #     except Exception as e:
            #         print(f"❌ Error confirming subscription: {e}")
            #         return 'Subscription confirmation failed', 500

            # # Handle notification messages from SNS
            # elif data.get('Type') == 'Notification':
                try:
                    message = json.loads(data.get('Message'))
                    print(f"📥 SNS Notification received: {message}")

                    # Start consumer if not already running
                    if not self.is_consumer_running():
                        subprocess.Popen(['python3', 'consumer.py'])
                        print("🚀 Kafka consumer started")
                    else:
                        print("⚠️ Kafka consumer already running to just keeping it alive")

                    return 'Notification received', 200
                except Exception as e:
                    print(f"❌ Error parsing SNS message: {e}")
                    return 'Error parsing SNS message', 500

            # If the message type is unknown or unsupported
            return 'Invalid SNS message', 400