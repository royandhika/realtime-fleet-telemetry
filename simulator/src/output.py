from __future__ import annotations

import json
import logging
import time

import config
from models import DrivingEvent, Telemetry


log = logging.getLogger("simulator")


class StdoutSink:
    def emit_telemetry(self, t: Telemetry) -> None:
        print(json.dumps({"kind": "telemetry", **t.to_dict()}), flush=True)

    def emit_event(self, e: DrivingEvent) -> None:
        print(json.dumps({"kind": "event", **e.to_dict()}), flush=True)

    def close(self) -> None:
        pass


class KafkaSink:
    def __init__(
        self,
        bootstrap_servers: str,
        telemetry_topic: str,
        events_topic: str,
    ) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.telemetry_topic = telemetry_topic
        self.events_topic = events_topic
        self._producer = self._connect_with_retry(bootstrap_servers)

    @staticmethod
    def _connect_with_retry(bootstrap_servers: str, retries: int = 60, delay: float = 2.0):
        from kafka import KafkaProducer

        last_err: Exception | None = None
        for i in range(retries):
            try:
                p = KafkaProducer(
                    bootstrap_servers=bootstrap_servers.split(","),
                    key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
                    value_serializer=lambda v: v.encode("utf-8") if isinstance(v, str) else v,
                    acks=1,
                    retries=5,
                    linger_ms=50,
                    max_block_ms=60000,
                )
                log.info("KafkaProducer connected to %s", bootstrap_servers)
                return p
            except Exception as exc:  # NoBrokersAvailable etc.
                last_err = exc
                log.warning(
                    "kafka connect failed (%s), retry %d/%d in %.1fs",
                    exc,
                    i + 1,
                    retries,
                    delay,
                )
                time.sleep(delay)
        raise RuntimeError(f"could not connect to Kafka at {bootstrap_servers}: {last_err}")

    def emit_telemetry(self, t: Telemetry) -> None:
        payload = json.dumps(t.to_dict())
        self._producer.send(
            self.telemetry_topic,
            key=t.vehicle_id,
            value=payload,
        )

    def emit_event(self, e: DrivingEvent) -> None:
        payload = json.dumps(e.to_dict())
        self._producer.send(
            self.events_topic,
            key=e.vehicle_id,
            value=payload,
        )

    def close(self) -> None:
        try:
            if self._producer is not None:
                self._producer.flush(timeout=10)
                self._producer.close()
        except Exception:
            log.warning("error closing kafka producer", exc_info=True)


def make_sink() -> StdoutSink | KafkaSink:
    sink_kind = config.OUTPUT_SINK
    if sink_kind == "stdout":
        return StdoutSink()
    if sink_kind == "kafka":
        return KafkaSink(
            bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
            telemetry_topic=config.KAFKA_TELEMETRY_TOPIC,
            events_topic=config.KAFKA_EVENTS_TOPIC,
        )
    raise ValueError(f"unknown OUTPUT_SINK={sink_kind!r}")
