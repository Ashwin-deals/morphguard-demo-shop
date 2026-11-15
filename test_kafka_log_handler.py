"""Prove KafkaLogHandler works end to end, and that a dead broker never hurts the app.

1. Round trip: log 3 lines with extra={"user": 42}, flush, and consume them
   back from logs.raw.django. Checks key == b"user-42", value == the
   formatted line, and that MorphGuard's Django parser (read from the backend
   repo, nothing written there) accepts each line as block "user-42".
2. Broker down: a handler pointed at a closed port. Logging a line must not
   raise and must return at once; the failure is counted once the message
   times out (message.timeout.ms = 60 s, so this part takes about a minute).

Usage:
    .venv/bin/python test_kafka_log_handler.py
    .venv/bin/python test_kafka_log_handler.py --backend /path/to/large-scale-log-analytics-backend
"""

import argparse
import importlib.util
import logging
import sys
import time
import uuid
from pathlib import Path

from confluent_kafka import Consumer, KafkaError, TopicPartition

from kafka_log_handler import KafkaLogHandler

BOOTSTRAP = "localhost:9092"
TOPIC = "logs.raw.django"
DEFAULT_BACKEND = Path.home() / "Documents/final-year-project/large-scale-log-analytics-backend"
# The line format parser/django_parser.py LINE_PATTERN expects.
LINE_FORMAT = "[%(asctime)s,%(msecs)03d] %(levelname)s %(name)s user=%(user)s %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def load_django_line_pattern(backend: Path):
    """LINE_PATTERN from the backend's parser, loaded without importing the
    package (so nothing, not even a .pyc, is written into the repo)."""
    sys.dont_write_bytecode = True
    source = (backend / "parser/django_parser.py").read_text()
    start = source.index("LINE_PATTERN = re.compile(")
    end = source.index("\n)\n", start) + 3
    namespace = {"re": __import__("re")}
    exec(source[start:end], namespace)  # noqa: S102 - our own repo's regex literal
    return namespace["LINE_PATTERN"]


def make_logger(name: str, handler: logging.Handler) -> logging.Logger:
    handler.setFormatter(logging.Formatter(LINE_FORMAT, DATE_FORMAT))
    logger = logging.getLogger(name)
    logger.handlers[:] = [handler]
    logger.setLevel(logging.INFO)
    logger.propagate = False
    return logger


def end_offsets(consumer: Consumer):
    metadata = consumer.list_topics(TOPIC, timeout=5)
    topic = metadata.topics[TOPIC]
    if topic.error is not None:
        return None, topic.error.str()
    parts = []
    for p in sorted(topic.partitions):
        _, high = consumer.get_watermark_offsets(TopicPartition(TOPIC, p), timeout=5)
        parts.append(TopicPartition(TOPIC, p, high))
    return parts, None


def round_trip(line_pattern) -> bool:
    print("== 1. round trip through", TOPIC)
    run = uuid.uuid4().hex[:8]
    consumer = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"demo-shop-test-{run}",
                         "enable.auto.commit": False})
    start, error = end_offsets(consumer)  # only read what this run writes
    if error:
        consumer.close()
        print(f"   topic {TOPIC} not usable: {error}")
        print("   RESULT: FAIL (topic missing; nothing produced)")
        return False

    handler = KafkaLogHandler(BOOTSTRAP, TOPIC)
    logger = make_logger("django.request", handler)
    requests = [("GET", f"/api/products/551?run={run}", 200, 1800, 45),
                ("POST", f"/api/cart?run={run}", 200, 420, 38),
                ("GET", f"/api/cart?run={run}", 200, 1100, 41)]
    for method, path, status, size, ms in requests:
        logger.info('ip=10.1.2.3 "%s %s HTTP/1.1" %d %d %dms', method, path, status, size, ms, extra={"user": 42})
    pending = handler.flush(5)
    print("   handler after flush:", handler.stats(), "| pending", pending)

    consumer.assign(start)
    got, deadline = [], time.monotonic() + 10
    while len(got) < len(requests) and time.monotonic() < deadline:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                print("   consumer error:", msg.error())
            continue
        if f"run={run}" in msg.value().decode("utf-8"):
            got.append(msg)
    consumer.close()
    handler.close()

    ok = len(got) == len(requests) and handler.stats()["failed"] == 0
    for msg in got:
        value = msg.value().decode("utf-8")
        match = line_pattern.match(value)
        block = f"user-{match.group('user')}" if match else None
        print(f"   partition {msg.partition()} offset {msg.offset()} key={msg.key()!r} value={value!r}")
        print(f"      django_parser: {'match, block ' + block if match else 'NO MATCH (would be dropped)'}")
        ok &= msg.key() == b"user-42" and block == "user-42"
    ok &= len({m.partition() for m in got}) <= 1  # one key -> one partition
    offsets = [m.offset() for m in got]
    ok &= offsets == sorted(offsets)
    print(f"   received {len(got)}/{len(requests)}; same partition, in order: "
          f"{len({m.partition() for m in got}) == 1 and offsets == sorted(offsets)}")
    print("   RESULT:", "PASS" if ok else "FAIL")
    return ok


def broker_down() -> bool:
    print("== 2. broker down (localhost:9, nothing listening)")
    handler = KafkaLogHandler("localhost:9", TOPIC)
    logger = make_logger("django.request.down", handler)
    t0 = time.perf_counter()
    try:
        logger.info('ip=10.1.2.3 "GET / HTTP/1.1" 200 5200 12ms', extra={"user": 42})
        raised = None
    except Exception as exc:  # noqa: BLE001
        raised = exc
    log_ms = (time.perf_counter() - t0) * 1000
    print(f"   logger.info returned in {log_ms:.2f} ms, raised: {raised!r}")
    print("   right after:", handler.stats())

    t0 = time.perf_counter()  # serve callbacks until the message times out
    while handler.stats()["failed"] == 0 and time.perf_counter() - t0 < 70:
        handler.producer.poll(0.5)
    print(f"   after {time.perf_counter() - t0:.1f} s:", handler.stats())

    t0 = time.perf_counter()
    handler.close()
    close_s = time.perf_counter() - t0
    print(f"   close() took {close_s:.2f} s")
    ok = raised is None and log_ms < 100 and handler.stats()["failed"] == 1 and close_s <= 5.5
    print("   RESULT:", "PASS" if ok else "FAIL")
    return ok


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--backend", type=Path, default=DEFAULT_BACKEND)
    args = parser.parse_args()
    results = [round_trip(load_django_line_pattern(args.backend)), broker_down()]
    print("== ALL PASS" if all(results) else "== SOME FAILED")
    sys.exit(0 if all(results) else 1)


if __name__ == "__main__":
    main()
