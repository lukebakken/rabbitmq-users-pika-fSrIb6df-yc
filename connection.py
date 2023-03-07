import functools
import pika
import threading

rmq_conn = None
rmq_ch = None


def rabbitmq_thread(connected_event: threading.Event, stop_event: threading.Event):
    global rmq_conn
    global rmq_ch
    c = pika.PlainCredentials("guest", "guest")
    rmq_conn = pika.BlockingConnection(pika.ConnectionParameters(credentials=c))
    rmq_ch = rmq_conn.channel()
    rmq_ch.queue_declare(queue="my_queue")
    connected_event.set()
    while not stop_event.is_set():
        rmq_conn.process_data_events(time_limit=1)


def rabbitmq_publish(msg: str):
    cb = functools.partial(do_publish, msg)
    rmq_conn.add_callback_threadsafe(cb)


def do_publish(msg: str):
    p = pika.BasicProperties(
        content_type="text/plain", delivery_mode=pika.DeliveryMode.Persistent
    )
    rmq_ch.basic_publish(
        exchange="", routing_key="my_queue", body=msg, properties=p, mandatory=True
    )
