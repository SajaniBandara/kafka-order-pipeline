# Kafka + Avro Order Pipeline

A Kafka-based system that **produces** and **consumes** order messages with
**Avro serialization**, plus:

| Requirement | Where it lives |
|---|---|
| Avro serialization (`order.avsc`) | [schemas/order.avsc](schemas/order.avsc), [src/avro_utils.py](src/avro_utils.py) |
| Real-time aggregation (running average of prices) | `RunningAverage` in [src/consumer.py](src/consumer.py) |
| Retry logic for temporary failures | `handle_with_retry()` in [src/consumer.py](src/consumer.py) (exponential backoff) |
| Dead Letter Queue for permanent failures | `send_to_dlq()` -> `orders.DLQ` topic in [src/consumer.py](src/consumer.py) |

**Stack:** Python 3 · `confluent-kafka` (librdkafka) · `fastavro` · Apache Kafka 3.9.1 in **KRaft mode** (no ZooKeeper, no Docker).

---

## Architecture

```
                 Avro binary                         Avro binary + x-error header
 producer.py ───────────────►  topic: orders  ─────►  consumer.py  ──────────────►  topic: orders.DLQ
 (random orders)               (3 partitions)         │                             (1 partition)
                                                      ├─ deserialize (fastavro)
                                                      ├─ process  ──► transient?  retry x3, backoff 0.5s,1s,2s
                                                      │            └► permanent?  ─────────────► DLQ
                                                      └─ running average of price  (printed every message)
```

- **Serialization:** `fastavro.schemaless_writer` / `schemaless_reader`. Producer
  and consumer share `schemas/order.avsc`; the Avro binary payload goes on the
  wire with no framing. (Self-contained — no Schema Registry service to run.)
- **Offsets** are committed **manually** only after a message is fully handled
  (processed *or* sent to the DLQ), so nothing is lost on a crash.
- **Failure simulation:** `process_order()` in the consumer randomly raises
  `TransientError` (~25%) or `PermanentError` (~10%) so the retry path and the
  DLQ can be shown live. Rates are configurable via env vars — see
  [src/config.py](src/config.py).

---

## One-time setup

Requires **Java 17+** on `PATH` (for the Kafka broker) and **Python 3.9+**.

```powershell
# 1. Python deps
pip install -r requirements.txt

# 2. Kafka: download the binary into tools\kafka.tgz, then set it up
#    (downloads kafka_2.13-3.9.1.tgz ~120 MB if you don't have it)
curl -L -o tools\kafka.tgz https://archive.apache.org/dist/kafka/3.9.1/kafka_2.13-3.9.1.tgz
powershell -ExecutionPolicy Bypass -File scripts\setup-kafka.ps1
```

`setup-kafka.ps1` extracts Kafka to `tools\kafka`, points its log dir at
`D:\kafka-logs` (a short path — avoids the Windows 260-char limit), and formats
a fresh KRaft storage directory.

> **Windows note:** `tools\kafka\bin\windows\kafka-run-class.bat` in this repo is
> patched to build the classpath with a single `libs\*` wildcard instead of
> listing all ~120 jars. Without that patch the broker fails to start with
> *"The input line is too long"*. If you re-extract Kafka from the archive,
> re-apply the patch (replace the `for %%i in ("%BASE_DIR%\libs\*")` loop near
> the *"Classpath addition for release"* comment with
> `call :concat "%BASE_DIR%\libs\*"`).
>
> **Always stop the broker with `scripts\stop-kafka.ps1` (or Ctrl+C in its
> window) — never close the window or `taskkill` the java process.** A hard
> kill skips the clean-shutdown marker; on the next start Kafka tries to
> recover the log dir and can corrupt it (`AccessDeniedException` renaming a
> "stray" partition dir, or `DUPLICATE_BROKER_REGISTRATION`). If that happens,
> the fix is `scripts\setup-kafka.ps1` again (wipes `D:\kafka-logs` and
> reformats — you lose whatever was in the topics, which is fine for a demo).

---

## Run the demo

Open **4 terminals** in the project root.

**Terminal 1 — start the broker** (keep open):
```powershell
powershell -ExecutionPolicy Bypass -File scripts\start-kafka.ps1
```
When you're done, stop it with `scripts\stop-kafka.ps1` (or Ctrl+C in this
window) — see the Windows note above about why a hard kill is a problem.

**Terminal 2 — create topics** (once per fresh broker):
```powershell
python src\create_topics.py
#  -> creates 'orders' (3 partitions) and 'orders.DLQ' (1 partition)
```

**Terminal 3 — start the consumer:**
```powershell
python src\consumer.py
```

**Terminal 4 — start the producer:**
```powershell
python src\producer.py                 # 1 order/sec until Ctrl+C
python src\producer.py --count 30 --interval 0.2   # burst of 30
```

Watch Terminal 3: every message prints the order, any retries, the updated
**running average**, and a `processed / dead-lettered` tally.

**Inspect the Dead Letter Queue** (Terminal 4, any time):
```powershell
python src\dlq_viewer.py
#  -> prints each dead-lettered order with its failure reason and origin offset
```

---

## Tuning (environment variables)

| Var | Default | Meaning |
|---|---|---|
| `KAFKA_BOOTSTRAP` | `localhost:9092` | broker address |
| `MAX_RETRIES` | `3` | transient-failure retries before DLQ |
| `RETRY_BACKOFF_SECONDS` | `0.5` | base backoff; doubles each retry |
| `TRANSIENT_FAILURE_RATE` | `0.25` | simulated transient failure probability |
| `PERMANENT_FAILURE_RATE` | `0.10` | simulated permanent failure probability |

Example — force lots of DLQ traffic:
```powershell
$env:PERMANENT_FAILURE_RATE = "0.4"; python src\consumer.py
```

---

## Project layout

```
schemas/order.avsc     Avro schema (orderId: string, product: string, price: float)
src/config.py          all tunables (env-var overridable)
src/avro_utils.py      Avro serialize / deserialize helpers
src/create_topics.py   creates 'orders' and 'orders.DLQ'
src/producer.py        random order generator -> 'orders'
src/consumer.py        deserialize + retry + DLQ + running-average aggregation
src/dlq_viewer.py      dump the DLQ with failure reasons
scripts/setup-kafka.ps1  extract + format Kafka (KRaft)
scripts/start-kafka.ps1  run the broker
scripts/stop-kafka.ps1   stop the broker cleanly (always use this, not taskkill)
```

## Reset between demo runs

> **Do not use `kafka-topics.bat --delete` on Windows.** Topic deletion doesn't
> reliably clean up the partition directory before the metadata log moves on,
> so the next broker start finds a "stray" directory with a stale topic ID,
> can't rename it away (`AccessDeniedException`), and the whole log dir goes
> offline. Use one of the two safe resets below instead.

**To replay the same messages** (fastest — just rewinds offsets, topics untouched):
```powershell
# stop the consumer first (Ctrl+C), then:
tools\kafka\bin\windows\kafka-consumer-groups.bat --bootstrap-server localhost:9092 `
  --group order-processors --reset-offsets --to-earliest --topic orders --execute
```

**For a true clean slate** (empty topics, fresh offsets — safe, no delete involved):
```powershell
# stop the broker first (scripts\stop-kafka.ps1 or Ctrl+C in its window), then:
powershell -ExecutionPolicy Bypass -File scripts\setup-kafka.ps1   # wipes D:\kafka-logs, reformats
powershell -ExecutionPolicy Bypass -File scripts\start-kafka.ps1
python src\create_topics.py
```
