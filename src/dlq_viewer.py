"""Print everything currently in the Dead Letter Queue. Handy for the demo."""
from confluent_kafka import Consumer, KafkaError

from avro_utils import deserialize_order
from config import BOOTSTRAP_SERVERS, DLQ_TOPIC


def main() -> None:
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": "dlq-viewer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([DLQ_TOPIC])
    print(f"[dlq] reading '{DLQ_TOPIC}' from the beginning (Ctrl+C to stop)")

    seen = 0
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"[dlq] kafka error: {msg.error()}")
                continue

            headers = {k: v.decode("utf-8", "replace") for k, v in (msg.headers() or [])}
            try:
                payload = deserialize_order(msg.value())
            except Exception:
                payload = "<unreadable Avro payload>"

            seen += 1
            print(f"[dlq] #{seen} offset={msg.offset()} payload={payload}")
            print(f"      reason : {headers.get('x-error')}")
            print(f"      origin : {headers.get('x-origin-topic')}"
                  f"[{headers.get('x-origin-partition')}]@{headers.get('x-origin-offset')}")
    except KeyboardInterrupt:
        print(f"\n[dlq] done - {seen} dead-lettered message(s) shown")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
