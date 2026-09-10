"""Order producer.

Generates random order transactions, serializes them with Avro and publishes
them to the ``orders`` topic.

    python src/producer.py                # stream one order/sec forever
    python src/producer.py --count 20     # send 20 orders then exit
    python src/producer.py --interval 0.2 # faster stream
"""
import argparse
import random
import time

from confluent_kafka import Producer

from avro_utils import serialize_order
from config import BOOTSTRAP_SERVERS, ORDERS_TOPIC

PRODUCTS = ["Item1", "Item2", "Item3", "Item4", "Item5"]


def delivery_report(err, msg) -> None:
    if err is not None:
        print(f"[producer] DELIVERY FAILED: {err}")
    else:
        print(f"[producer] delivered key={msg.key().decode()} "
              f"-> {msg.topic()}[{msg.partition()}]@{msg.offset()}")


def make_order(seq: int) -> dict:
    return {
        "orderId": str(1000 + seq),
        "product": random.choice(PRODUCTS),
        "price": round(random.uniform(5.0, 500.0), 2),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--count", type=int, default=0,
                        help="number of orders to send (0 = run until Ctrl+C)")
    parser.add_argument("--interval", type=float, default=1.0,
                        help="seconds to wait between orders")
    args = parser.parse_args()

    producer = Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "enable.idempotence": True,
        "acks": "all",
    })

    print(f"[producer] publishing to '{ORDERS_TOPIC}' at {BOOTSTRAP_SERVERS}")
    seq = 0
    try:
        while args.count == 0 or seq < args.count:
            seq += 1
            order = make_order(seq)
            producer.produce(
                ORDERS_TOPIC,
                key=order["orderId"].encode("utf-8"),
                value=serialize_order(order),
                on_delivery=delivery_report,
            )
            producer.poll(0)
            print(f"[producer] sent {order}")
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\n[producer] stopping...")
    finally:
        remaining = producer.flush(10)
        if remaining:
            print(f"[producer] WARNING: {remaining} message(s) not delivered")


if __name__ == "__main__":
    main()
