import logging
import threading
from connection import rabbitmq_thread, rabbitmq_publish

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def publish_message(msg):
    rabbitmq_publish(msg)


if __name__ == "__main__":
    stop_event = threading.Event()
    connected_event = threading.Event()

    logger.info("starting rabbitmq_thread...")
    rabbitmq_thread = threading.Thread(
        target=rabbitmq_thread, args=(connected_event, stop_event)
    )
    rabbitmq_thread.start()
    connected_event.wait()

    logger.info("rabbitmq_thread started, publishing a message...")
    publish_message("hello")

    logger.info("stopping and waiting for rabbitmq_thread to exit...")
    stop_event.set()
    rabbitmq_thread.join()

    logger.info("exiting")
