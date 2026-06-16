"""
Run a small state recovery scenario with stuck-processing diagnostics enabled.

This script is intentionally not a pytest test. It is a runnable app that prints the
diagnostic logging shape for source-offset-out-of-range recovery.

Requirements:
- A Kafka broker reachable via KAFKA_BOOTSTRAP_SERVERS, default localhost:9092.
- Broker support for DeleteRecords.

Example:
    KAFKA_BOOTSTRAP_SERVERS=localhost:9092 python scripts/recovery_diagnostics_demo.py
"""

import logging
import os
import shutil
import sys
import time
import uuid
from json import dumps
from pathlib import Path
from tempfile import gettempdir

from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from quixstreams import Application
from quixstreams.models import TopicConfig
from quixstreams.state import State

BROKER_ADDRESS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
MESSAGE_COUNT = int(os.getenv("RECOVERY_DEMO_MESSAGE_COUNT", "50"))
STATE_DIR = Path(
    os.getenv(
        "RECOVERY_DEMO_STATE_DIR", str(Path(gettempdir()) / "quix-recovery-demo-state")
    )
)
DIAGNOSTIC_TIMEOUT = float(os.getenv("RECOVERY_DEMO_DIAGNOSTIC_TIMEOUT", "120"))
PARTITION = 0
MESSAGE_KEY = b"diagnostic-key"


def wait_for_watermarks(topic_name: str, min_lowwater: int) -> tuple[int, int]:
    consumer = Consumer(
        {
            "bootstrap.servers": BROKER_ADDRESS,
            "group.id": f"recovery-diagnostic-watermark-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
        }
    )
    try:
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            lowwater, highwater = consumer.get_watermark_offsets(
                TopicPartition(topic_name, PARTITION),
                timeout=5,
            )
            if lowwater >= min_lowwater:
                return lowwater, highwater
            time.sleep(0.5)
    finally:
        consumer.close()

    raise TimeoutError(
        f"Timed out waiting for {topic_name}[{PARTITION}] lowwater >= {min_lowwater}"
    )


def committed_offset(consumer_group: str, topic_name: str) -> int:
    consumer = Consumer(
        {
            "bootstrap.servers": BROKER_ADDRESS,
            "group.id": consumer_group,
            "auto.offset.reset": "earliest",
        }
    )
    try:
        committed = consumer.committed([TopicPartition(topic_name, PARTITION)])[0]
        return committed.offset
    finally:
        consumer.close()


def create_source_topic(topic_name: str) -> None:
    admin = AdminClient({"bootstrap.servers": BROKER_ADDRESS})
    futures = admin.create_topics(
        [
            NewTopic(
                topic=topic_name,
                num_partitions=1,
                replication_factor=1,
            )
        ],
        operation_timeout=30,
    )
    for future in futures.values():
        try:
            future.result(timeout=30)
        except Exception as exc:
            if "already exists" not in str(exc):
                raise


def delete_source_records(topic_name: str, offset: int) -> None:
    admin = AdminClient({"bootstrap.servers": BROKER_ADDRESS})
    futures = admin.delete_records([TopicPartition(topic_name, PARTITION, offset)])
    for future in futures.values():
        future.result(timeout=30)


def build_app(
    consumer_group: str,
    topic_name: str,
    on_message_processed,
    *,
    diagnostics: bool,
) -> tuple[Application, object]:
    app = Application(
        broker_address=BROKER_ADDRESS,
        consumer_group=consumer_group,
        state_dir=STATE_DIR,
        auto_offset_reset="earliest",
        commit_every=1,
        commit_interval=999,
        loglevel="INFO",
        on_message_processed=on_message_processed,
        diagnose_stuck_processing=diagnostics,
        diagnostic_stuck_timeout=DIAGNOSTIC_TIMEOUT,
    )
    topic = app.topic(
        topic_name,
        config=TopicConfig(num_partitions=1, replication_factor=1),
    )

    def count(value: dict, state: State):
        state.set("seen", state.get("seen", 0) + 1)
        return value

    return app, app.dataframe(topic).update(count, stateful=True)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="[%(levelname)s] %(name)s: %(message)s",
    )
    consumer_group = f"recovery-diagnostic-{uuid.uuid4()}"
    topic_name = f"recovery-diagnostic-source-{uuid.uuid4()}"

    shutil.rmtree(STATE_DIR / consumer_group, ignore_errors=True)
    create_source_topic(topic_name)

    first_run_offsets: list[int] = []

    def on_first_run_processed(topic: str, partition: int, offset: int) -> None:
        first_run_offsets.append(offset)
        app1.stop()

    app1, sdf1 = build_app(
        consumer_group,
        topic_name,
        on_first_run_processed,
        diagnostics=False,
    )
    with app1.get_producer() as producer:
        for index in range(MESSAGE_COUNT):
            producer.produce(
                topic=topic_name,
                key=MESSAGE_KEY,
                value=dumps({"value": index}).encode(),
                partition=PARTITION,
            )
        producer.flush()

    print("First run: process one message and commit local state/changelog.")
    app1.run(sdf1)
    if not first_run_offsets:
        raise RuntimeError("First run did not process any messages")

    committed = committed_offset(consumer_group, topic_name)
    print(f"Committed source offset after first run: {committed}")

    target_lowwater = committed + 5
    print(
        f"Deleting source records through offset {target_lowwater} to force recovery."
    )
    delete_source_records(topic_name, target_lowwater)
    lowwater, highwater = wait_for_watermarks(topic_name, target_lowwater)
    print(
        f"Source watermarks after delete_records: lowwater={lowwater} highwater={highwater}"
    )

    second_run_offsets: list[int] = []

    def on_second_run_processed(topic: str, partition: int, offset: int) -> None:
        second_run_offsets.append(offset)
        app2.stop()

    app2, sdf2 = build_app(
        consumer_group,
        topic_name,
        on_second_run_processed,
        diagnostics=True,
    )

    print("Second run: diagnostics enabled; expect destructive recovery logs.")
    app2.run(sdf2)
    if not second_run_offsets:
        raise RuntimeError("Second run did not process any messages after recovery")

    print(f"Recovered and processed source offset: {second_run_offsets[0]}")
    print(f"State directory kept for inspection: {STATE_DIR / consumer_group}")


if __name__ == "__main__":
    main()
