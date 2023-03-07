import pika
import threading
from connection import rabbit_channel


def main(ch, msgs):
    ch.basic_publish(
        exchange="",
        routing_key="my_queue",
        body=msgs,
        properties=pika.BasicProperties(delivery_mode=2),
    )


if __name__ == "__main__":
    thread = threading.Thread(target=main, args=(ch, msgs))
    thread.start()
