"""Order consumer.

Responsibilities
----------------
1. Consume Avro order messages from the ``orders`` topic.
2. Real-time aggregation: maintain a running average of order prices.
3. Retry logic: transient processing failures are retried with exponential
   backoff up to ``MAX_RETRIES`` times.
4. Dead Letter Queue: messages that fail permanently (bad payload, business
   rule violation, or retries exhausted) are published to ``orders.DLQ`` with
   the failure reason attached as a Kafka header.

Offsets are committed manually only after a message has been fully handled
(processed OR routed to the DLQ), so nothing is silently lost.

    python src/consumer.py
"""
import random
import time

from confluent_kafka import Consumer, KafkaError, Producer

from avro_utils import deserialize_order
from config import (
    BOOTSTRAP_SERVERS,
    CONSUMER_GROUP,
    DLQ_TOPIC,
    MAX_RETRIES,
    ORDERS_TOPIC,
    PERMANENT_FAILURE_RATE,
    RETRY_BACKOFF_SECONDS,
    TRANSIENT_FAILURE_RATE,
)


class TransientError(Exception):
    """A failure that is expected to go away on retry."""


class PermanentError(Exception):
    """A failure that will never succeed -> straight to the DLQ."""


class RunningAverage:
    """Incremental mean - O(1) memory, updated per message."""

    def __init__(self) -> None:
        self.count = 0
        self.total = 0.0

    def add(self, value: float) -> float:
        self.count += 1
        self.total += value
        return self.total / self.count


def process_order(order: dict) -> None:
    """Pretend to do real work with a downstream system.

    For the demo the outcome is randomized so the retry path and the DLQ are
    exercised live. Replace the body with real logic in production.
    """
    if not order.get("orderId") or float(order["price"]) <= 0:
        raise PermanentError("business rule violation: missing id or non-positive price")

    roll = random.random()
    if roll < PERMANENT_FAILURE_RATE:
        raise PermanentError("downstream rejected the order (non-retryable)")
    if roll < PERMANENT_FAILURE_RATE + TRANSIENT_FAILURE_RATE:
        raise TransientError("downstream service temporarily unavailable")
    # else: success


def handle_with_retry(order: dict) -> int:
    """Run process_order with retries. Returns the number of retries used.

    Raises PermanentError if the failure is non-retryable or retries run out.
    """
    attempt = 0
    while True:
        try:
            process_order(order)
            return attempt
        except TransientError as exc:
            attempt += 1
            if attempt > MAX_RETRIES:
                raise PermanentError(f"retries exhausted after {MAX_RETRIES} attempts ({exc})") from exc
            backoff = RETRY_BACKOFF_SECONDS * (2 ** (attempt - 1))
            print(f"[consumer]   transient failure on order {order['orderId']}: {exc} "
                  f"-> retry {attempt}/{MAX_RETRIES} in {backoff:.2f}s")
            time.sleep(backoff)


def send_to_dlq(dlq: Producer, msg, reason: str) -> None:
    headers = list(msg.headers() or [])
    headers += [
        ("x-error", reason.encode("utf-8")),
        ("x-origin-topic", (msg.topic() or "").encode("utf-8")),
        ("x-origin-partition", str(msg.partition()).encode("utf-8")),
        ("x-origin-offset", str(msg.offset()).encode("utf-8")),
    ]
    dlq.produce(DLQ_TOPIC, key=msg.key(), value=msg.value(), headers=headers)
    dlq.poll(0)
    print(f"[consumer]   -> DLQ: {reason}")


def main() -> None:
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": CONSUMER_GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    dlq_producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS, "acks": "all"})
    consumer.subscribe([ORDERS_TOPIC])

    avg = RunningAverage()
    ok = 0
    dead = 0

    print(f"[consumer] group='{CONSUMER_GROUP}' listening on '{ORDERS_TOPIC}' "
          f"(retries={MAX_RETRIES}, dlq='{DLQ_TOPIC}')")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"[consumer] kafka error: {msg.error()}")
                continue

            # 1. Deserialize
            try:
                order = deserialize_order(msg.value())
            except Exception as exc:
                dead += 1
                send_to_dlq(dlq_producer, msg, f"deserialization error: {exc}")
                consumer.commit(msg)
                continue

            # 2. Process with retry
            try:
                retries = handle_with_retry(order)
            except PermanentError as exc:
                dead += 1
                send_to_dlq(dlq_producer, msg, str(exc))
                consumer.commit(msg)
                continue

            # 3. Real-time aggregation
            running = avg.add(float(order["price"]))
            ok += 1
            suffix = f" (after {retries} retr{'y' if retries == 1 else 'ies'})" if retries else ""
            print(f"[consumer] OK   order={order['orderId']:>5} product={order['product']:<6} "
                  f"price={order['price']:8.2f}{suffix}")
            print(f"[consumer] AGG  running average over {avg.count} orders = {running:.2f}")

            consumer.commit(msg)
            print(f"[consumer] STATS processed={ok} dead-lettered={dead}\n")
    except KeyboardInterrupt:
        print("\n[consumer] stopping...")
    finally:
        dlq_producer.flush(10)
        consumer.close()
        if avg.count:
            print(f"[consumer] final running average = {avg.total / avg.count:.2f} "
                  f"over {avg.count} orders; {dead} sent to DLQ")


if __name__ == "__main__":
    main()
