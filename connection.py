import functools
import logging
import pika
import pika.channel
import pika.frame
import queue
import threading

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

rmq_conn: pika.adapters.BlockingConnection
rmq_ch: pika.channel.Channel


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
    rmq_ch.close()
    rmq_conn.close()


def rabbitmq_publish(msg: str):
    cb = functools.partial(_do_publish, msg)
    rmq_conn.add_callback_threadsafe(cb)


def rabbitmq_queue_declare(**kwargs):
    rq = queue.Queue(1)
    cb = functools.partial(_do_queue_declare, rq, **kwargs)
    rmq_conn.add_callback_threadsafe(cb)
    return rq.get()


def _do_publish(msg: str):
    p = pika.BasicProperties(
        content_type="text/plain", delivery_mode=pika.DeliveryMode.Persistent
    )
    rmq_ch.basic_publish(
        exchange="", routing_key="my_queue", body=msg, properties=p, mandatory=True
    )


def _do_queue_declare(rq: queue.Queue, **kwargs):
    result = rmq_ch.queue_declare(
        queue=kwargs["queue_name"],
        passive=kwargs["passive"],
        durable=True,
        auto_delete=False,
        exclusive=False,
    )
    rv = (
        result.method.queue,
        result.method.consumer_count,
        result.method.message_count,
    )
    rq.put(rv)
