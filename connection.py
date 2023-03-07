def get_rabbit_connection():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            heartbeat=0,
            host=settings.RABBITMQ_HOST,
            port=settings.RABBITMQ_PORT,
            credentials=PlainCredentials(
                settings.RABBITMQ_DEFAULT_USER, settings.RABBITMQ_DEFAULT_PASS
            ),
        )
    )
    return connection


rabbitmq_client = get_rabbit_connection()
rabbitmq_channel = rabbitmq_client.channel()
rabbitmq_channel.queue_declare(queue="my_queue")
