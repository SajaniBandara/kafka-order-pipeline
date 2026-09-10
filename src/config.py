"""Central configuration. Every value can be overridden with an environment variable."""
import os

# --- Kafka connection -------------------------------------------------------
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

ORDERS_TOPIC = os.getenv("ORDERS_TOPIC", "orders")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "orders.DLQ")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "order-processors")

# --- Avro schema ----------------------------------------------------------
SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "..", "schemas", "order.avsc")

# --- Retry behaviour ----------------------------------------------------------
# A message that keeps failing transiently is retried MAX_RETRIES times with an
# exponential backoff before it is treated as permanently failed.
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
RETRY_BACKOFF_SECONDS = float(os.getenv("RETRY_BACKOFF_SECONDS", "0.5"))

# --- Failure simulation (demo only) -----------------------------------------
# Used by the consumer to fake real-world processing failures so the retry
# path and the DLQ can be demonstrated live.
TRANSIENT_FAILURE_RATE = float(os.getenv("TRANSIENT_FAILURE_RATE", "0.25"))
PERMANENT_FAILURE_RATE = float(os.getenv("PERMANENT_FAILURE_RATE", "0.10"))
