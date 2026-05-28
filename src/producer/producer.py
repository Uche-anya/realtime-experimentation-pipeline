import json
import os
import random
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from confluent_kafka import Producer
from dotenv import load_dotenv


load_dotenv(override=True)

EXPERIMENT_ID = os.getenv("EXPERIMENT_ID", "EXP_CHECKOUT_FAST_TRACK_V2")
SCHEMA_VERSION = "v1"

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
EXPOSURE_TOPIC = os.getenv("EXPOSURE_TOPIC", "experiment_exposures")
BEHAVIOR_TOPIC = os.getenv("BEHAVIOR_TOPIC", "user_events")

VARIANTS = ["control", "treatment"]
DEVICES = ["ios", "android", "web"]
COUNTRIES = ["GB", "NG", "NL", "DE", "FR"]
TRAFFIC_SOURCES = ["organic", "paid_search", "email", "affiliate", "direct"]
APP_VERSIONS = ["4.1.0", "4.1.1", "4.2.0-beta"]

FUNNEL_EVENTS = [
    "page_view",
    "product_view",
    "add_to_cart",
    "checkout_started",
    "payment_failed",
    "purchase",
    "refund",
]

KAFKA_SECURITY_PROTOCOL = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
KAFKA_SASL_MECHANISM = os.getenv("KAFKA_SASL_MECHANISM", "")
KAFKA_SASL_USERNAME = os.getenv("KAFKA_SASL_USERNAME", "")
KAFKA_SASL_PASSWORD = os.getenv("KAFKA_SASL_PASSWORD", "")
KAFKA_COMPRESSION_TYPE = os.getenv("KAFKA_COMPRESSION_TYPE", "lz4")
KAFKA_ENABLE_IDEMPOTENCE = os.getenv("KAFKA_ENABLE_IDEMPOTENCE", "true").lower() == "true"
KAFKA_LINGER_MS = int(os.getenv("KAFKA_LINGER_MS", "50"))
KAFKA_MAX_IN_FLIGHT = int(os.getenv("KAFKA_MAX_IN_FLIGHT", "1"))
KAFKA_RETRIES = int(os.getenv("KAFKA_RETRIES", "5"))
KAFKA_CLIENT_ID = os.getenv("KAFKA_CLIENT_ID", "ab-test-telemetry-producer")
EXPERIMENT_STARTED_AT = datetime.now(timezone.utc).isoformat()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class ExperimentTelemetryGenerator:
    def __init__(
        self,
        anomaly_rate: float = 0.05,
        max_active_users: int = 50_000,
    ):
        self.anomaly_rate = anomaly_rate
        self.max_active_users = max_active_users
        self.users: dict[str, dict[str, Any]] = {}

    def _maybe_late_timestamp(self) -> str:
        event_time = datetime.now(timezone.utc)

        if random.random() < self.anomaly_rate:
            event_time -= timedelta(minutes=random.randint(5, 240))

        return event_time.isoformat()

    def _base_user_context(self) -> dict[str, Any]:
        device = random.choice(DEVICES)

        return {
            "session_id": str(uuid.uuid4()),
            "device": device,
            "platform": "mobile" if device in ["ios", "android"] else "web",
            "country": random.choice(COUNTRIES),
            "traffic_source": random.choice(TRAFFIC_SOURCES),
            "app_version": random.choice(APP_VERSIONS),
            "user_segment": random.choice(["new_user", "returning_user", "high_intent", "low_intent"]),
            "is_bot": random.random() < 0.02,
        }

    def _keep_user_cache_bounded(self) -> None:
        if len(self.users) <= self.max_active_users:
            return

        users_to_remove = len(self.users) - self.max_active_users
        for user_id in list(self.users.keys())[:users_to_remove]:
            self.users.pop(user_id, None)

    def generate_exposure(self) -> list[dict[str, Any]]:
        user_id = str(uuid.uuid4())
        variant = random.choice(VARIANTS)
        context = self._base_user_context()

        exposure = {
            "schema_version": SCHEMA_VERSION,
            "event_type": "exposure",
            "event_id": str(uuid.uuid4()),
            "experiment_id": EXPERIMENT_ID,
            "experiment_started_at": EXPERIMENT_STARTED_AT,
            "user_id": user_id,
            "session_id": context["session_id"],
            "assigned_variant": variant,
            "device": context["device"],
            "platform": context["platform"],
            "country": context["country"],
            "traffic_source": context["traffic_source"],
            "app_version": context["app_version"],
            "user_segment": context["user_segment"],
            "is_bot": context["is_bot"],
            "event_time": utc_now_iso(),
            "producer": "local_ab_test_simulator",
        }

        self.users[user_id] = {
            "experiment_id": EXPERIMENT_ID,
            "assigned_variant": variant,
            **context,
        }

        self._keep_user_cache_bounded()

        events = [exposure]

        # Simulate duplicate exposure bug
        if random.random() < self.anomaly_rate / 5:
            duplicate_exposure = exposure.copy()
            duplicate_exposure["event_id"] = str(uuid.uuid4())
            duplicate_exposure["duplicate_exposure_bug"] = True
            events.append(duplicate_exposure)

        # Simulate schema drift / missing field
        if random.random() < self.anomaly_rate:
            exposure.pop("device", None)

        return events

    def generate_behavior_events(self) -> list[dict[str, Any]]:
        # Rare behavior without exposure
        if not self.users or random.random() < self.anomaly_rate / 10:
            return [self._generate_orphan_behavior_event()]

        user_id = random.choice(list(self.users.keys()))
        ctx = self.users[user_id]
        variant = ctx["assigned_variant"]

        if variant == "treatment":
            weights = [0.32, 0.24, 0.18, 0.12, 0.04, 0.08, 0.02]
        else:
            weights = [0.35, 0.26, 0.18, 0.12, 0.05, 0.03, 0.01]

        event_name = random.choices(FUNNEL_EVENTS, weights=weights, k=1)[0]

        behavior = {
            "schema_version": SCHEMA_VERSION,
            "event_type": "behavior",
            "event_id": str(uuid.uuid4()),
            "experiment_id": ctx["experiment_id"],
            "user_id": user_id,
            "session_id": ctx["session_id"],
            "event_name": event_name,
            "device": ctx["device"],
            "platform": ctx["platform"],
            "country": ctx["country"],
            "traffic_source": ctx["traffic_source"],
            "app_version": ctx["app_version"],
            "user_segment": ctx["user_segment"],
            "event_time": self._maybe_late_timestamp(),
            "producer": "local_ab_test_simulator",
            "properties": {
                "page": f"/{event_name.replace('_', '-')}",
                "is_bot": ctx["is_bot"],
            },
        }

        if event_name == "purchase":
            behavior["properties"]["revenue_gbp"] = round(random.uniform(15.0, 180.0), 2)

        if event_name == "refund":
            behavior["properties"]["refund_gbp"] = round(random.uniform(5.0, 90.0), 2)

        if event_name == "payment_failed":
            behavior["properties"]["failure_reason"] = random.choice(
                ["card_declined", "network_error", "3ds_timeout"]
            )

        # Simulate producer/client retry duplicates
        if random.random() < self.anomaly_rate / 2:
            return [behavior.copy() for _ in range(random.randint(2, 5))]

        return [behavior]

    def _generate_orphan_behavior_event(self) -> dict[str, Any]:
        context = self._base_user_context()
        event_name = random.choice(["page_view", "purchase"])

        event = {
            "schema_version": SCHEMA_VERSION,
            "event_type": "behavior",
            "event_id": str(uuid.uuid4()),
            "experiment_id": EXPERIMENT_ID,
            "user_id": str(uuid.uuid4()),
            "session_id": context["session_id"],
            "event_name": event_name,
            "device": context["device"],
            "platform": context["platform"],
            "country": context["country"],
            "traffic_source": context["traffic_source"],
            "app_version": context["app_version"],
            "user_segment": context["user_segment"],
            "event_time": self._maybe_late_timestamp(),
            "producer": "local_ab_test_simulator",
            "orphan_behavior": True,
            "properties": {
                "page": f"/{event_name.replace('_', '-')}",
                "is_bot": context["is_bot"],
            },
        }

        if event_name == "purchase":
            event["properties"]["revenue_gbp"] = round(random.uniform(15.0, 180.0), 2)

        return event


def delivery_report(err, msg) -> None:
    if err is not None:
        print(f"[DELIVERY FAILED] {err}")
        return

    print(
        f"[DELIVERED] topic={msg.topic()} "
        f"partition={msg.partition()} offset={msg.offset()}"
    )


def build_producer_config() -> dict[str, Any]:
    config = {
        "bootstrap.servers": KAFKA_BROKER,
        "client.id": KAFKA_CLIENT_ID,
        "acks": "all",
        "retries": KAFKA_RETRIES,
        "linger.ms": KAFKA_LINGER_MS,
        "enable.idempotence": KAFKA_ENABLE_IDEMPOTENCE,
        "compression.type": KAFKA_COMPRESSION_TYPE,
        "max.in.flight.requests.per.connection": KAFKA_MAX_IN_FLIGHT,
    }

    if KAFKA_SECURITY_PROTOCOL != "PLAINTEXT":
        config["security.protocol"] = KAFKA_SECURITY_PROTOCOL
        if KAFKA_SECURITY_PROTOCOL.startswith("SASL"):
            config["sasl.mechanisms"] = KAFKA_SASL_MECHANISM
            config["sasl.username"] = KAFKA_SASL_USERNAME
            config["sasl.password"] = KAFKA_SASL_PASSWORD

    return config


def produce_json(
    producer: Producer,
    topic: str,
    key: str,
    payload: dict[str, Any],
) -> None:
    try:
        producer.produce(
            topic=topic,
            key=key,
            value=json.dumps(payload),
            callback=delivery_report,
        )
    except BufferError:
        print("[BACKPRESSURE] Producer queue full. Polling before retry...")
        producer.poll(1)
        producer.produce(
            topic=topic,
            key=key,
            value=json.dumps(payload),
            callback=delivery_report,
        )


def main() -> None:
    print("Starting A/B experimentation telemetry producer...")
    print(f"Kafka broker: {KAFKA_BROKER}")
    print(f"Exposure topic: {EXPOSURE_TOPIC}")
    print(f"Behavior topic: {BEHAVIOR_TOPIC}")

    producer = Producer(build_producer_config())

    generator = ExperimentTelemetryGenerator(anomaly_rate=0.08)

    try:
        while True:
            if random.random() < 0.35:
                exposure_events = generator.generate_exposure()

                for exposure in exposure_events:
                    produce_json(
                        producer=producer,
                        topic=EXPOSURE_TOPIC,
                        key=exposure["user_id"],
                        payload=exposure,
                    )

                    print(
                        f"[EXPOSURE] user={exposure['user_id'][:8]} "
                        f"variant={exposure.get('assigned_variant')}"
                    )
            else:
                behavior_events = generator.generate_behavior_events()

                for event in behavior_events:
                    produce_json(
                        producer=producer,
                        topic=BEHAVIOR_TOPIC,
                        key=event["user_id"],
                        payload=event,
                    )

                    print(
                        f"[BEHAVIOR] user={event['user_id'][:8]} "
                        f"event={event['event_name']}"
                    )

            producer.poll(0)
            time.sleep(random.uniform(0.05, 0.30))

    except KeyboardInterrupt:
        print("\nProducer stopped.")
    finally:
        producer.flush()


if __name__ == "__main__":
    main()