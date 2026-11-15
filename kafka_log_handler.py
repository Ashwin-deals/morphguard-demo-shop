"""A logging.Handler that publishes each log line to Kafka for MorphGuard.

Every record becomes one Kafka message:

  key    f"user-{record.user}" (UTF-8) when the record carries a `user`
         attribute (logger.info(..., extra={"user": 42})). This is the block id
         MorphGuard's Django parser assigns (parser/django_parser.py), and the
         same key its producer and syslog receiver use. Kafka sends every
         message with one key to one partition, so one user's lines stay in
         order, which the stream processor's per-user state relies on. Records
         without a user are sent unkeyed; the processor drops lines its parser
         cannot place in a block.
  value  the formatted record, UTF-8 encoded.

Logging must never slow down or break the app. produce() only queues the
message (the client sends it from a background thread), poll(0) serves
delivery callbacks without waiting, and every error is counted instead of
raised. A broker that is down costs nothing at log time: undelivered messages
fail after message.timeout.ms and are counted as failed. At exit the queue is
flushed for at most 5 seconds.
"""

from __future__ import annotations

import atexit
import logging
import threading

from confluent_kafka import KafkaException, Producer

DEFAULT_BOOTSTRAP = "localhost:9092"
DEFAULT_TOPIC = "logs.raw.django"
FLUSH_TIMEOUT_SECONDS = 5


class KafkaLogHandler(logging.Handler):
    def __init__(self, bootstrap: str = DEFAULT_BOOTSTRAP, topic: str = DEFAULT_TOPIC, level=logging.NOTSET):
        super().__init__(level)
        self.topic = topic
        self.producer = Producer({
            "bootstrap.servers": bootstrap,
            "linger.ms": 20,
            "socket.timeout.ms": 3000,
            # 60 s: rides out a broker restart (at 10 s, a ~9 s restart lost 19 of 20 lines).
            "message.timeout.ms": 60000,
        })
        self._lock = threading.Lock()
        self._guard = threading.local()  # stops recursion if something inside emit() logs
        self.queued = self.delivered = self.failed = 0
        self.last_error: str | None = None
        self._closed = False
        atexit.register(self._flush_at_exit)

    # -- delivery accounting -------------------------------------------------

    def _on_delivery(self, err, msg):
        with self._lock:
            if err is None:
                self.delivered += 1
            else:
                self.failed += 1
                self.last_error = str(err)

    def _count_failure(self, exc: BaseException):
        with self._lock:
            self.failed += 1
            self.last_error = f"{type(exc).__name__}: {exc}"

    def stats(self) -> dict:
        with self._lock:
            return {"queued": self.queued, "delivered": self.delivered, "failed": self.failed,
                    "pending": len(self.producer), "last_error": self.last_error}

    # -- logging.Handler -----------------------------------------------------

    def emit(self, record: logging.LogRecord) -> None:
        if self._closed or getattr(self._guard, "active", False):
            return
        self._guard.active = True
        try:
            line = self.format(record)
            user = getattr(record, "user", None)
            key = f"user-{user}".encode("utf-8") if user is not None else None
            self.producer.produce(self.topic, key=key, value=line.encode("utf-8"), on_delivery=self._on_delivery)
            with self._lock:
                self.queued += 1
        except BufferError as exc:  # local queue full: drop the line, never wait
            self._count_failure(exc)
        except Exception as exc:  # noqa: BLE001 - logging must never raise into the app
            self._count_failure(exc)
        finally:
            try:
                self.producer.poll(0)
            except Exception as exc:  # noqa: BLE001
                self._count_failure(exc)
            self._guard.active = False

    def flush(self, timeout: float = FLUSH_TIMEOUT_SECONDS) -> int:
        """Wait at most `timeout` seconds for queued messages; returns how many are still pending."""
        try:
            return self.producer.flush(timeout)
        except (KafkaException, Exception):  # noqa: BLE001
            return len(self.producer)

    def _flush_at_exit(self):
        if not self._closed:
            self.flush(FLUSH_TIMEOUT_SECONDS)

    def close(self) -> None:
        if not self._closed:
            self.flush(FLUSH_TIMEOUT_SECONDS)
            self._closed = True
            atexit.unregister(self._flush_at_exit)
        super().close()
