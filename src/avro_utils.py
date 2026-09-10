"""Avro (de)serialization for order messages.

We use fastavro's *schemaless* writer/reader: the Avro binary payload is put on
the wire without any framing, and both producer and consumer share the same
``schemas/order.avsc``. This keeps the demo self-contained (no Schema Registry
service to run) while still being genuine Avro binary serialization.
"""
import io
import json

import fastavro

from config import SCHEMA_PATH

with open(SCHEMA_PATH, "r", encoding="utf-8") as _f:
    ORDER_SCHEMA = json.load(_f)

PARSED_SCHEMA = fastavro.parse_schema(ORDER_SCHEMA)


def serialize_order(order: dict) -> bytes:
    """dict -> Avro binary bytes."""
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, PARSED_SCHEMA, order)
    return buf.getvalue()


def deserialize_order(data: bytes) -> dict:
    """Avro binary bytes -> dict. Raises on malformed input."""
    return fastavro.schemaless_reader(io.BytesIO(data), PARSED_SCHEMA)
