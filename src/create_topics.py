"""Create the Kafka topics used by the demo. Safe to run repeatedly."""
from confluent_kafka.admin import AdminClient, NewTopic

from config import BOOTSTRAP_SERVERS, ORDERS_TOPIC, DLQ_TOPIC


def main() -> None:
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})

    wanted = [
        NewTopic(ORDERS_TOPIC, num_partitions=3, replication_factor=1),
        NewTopic(DLQ_TOPIC, num_partitions=1, replication_factor=1),
    ]

    for name, fut in admin.create_topics(wanted).items():
        try:
            fut.result()
            print(f"created topic: {name}")
        except Exception as exc:  # already exists / etc.
            print(f"topic {name}: {exc}")


if __name__ == "__main__":
    main()
