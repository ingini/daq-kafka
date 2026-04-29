# ============================================================
#  consumers/minio/minio_consumer.py
#  Confluent Kafka → MinIO S3 Object Storage
#
#  - cam topic  : binary [8B ts_ns][4B len][JPEG bytes] → .jpg
#  - imu/gnss   : JSON  → .json
#  - 오브젝트 경로 패턴:  {date}/{hour}/{timestamp}.{ext}
#
#  env:
#    KAFKA_BOOTSTRAP, KAFKA_GROUP_ID
#    KAFKA_TOPICS  (comma-separated)
#    KAFKA_AUTO_OFFSET_RESET
#    MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY
#    TOPIC_BUCKET_MAP  (JSON string)
#    OBJECT_PATH_PATTERN
#    BATCH_SIZE, FLUSH_INTERVAL
# ============================================================

import os
import json
import struct
import time
import logging
from datetime import datetime, timezone
from io import BytesIO

from confluent_kafka import Consumer, KafkaException, KafkaError
from minio import Minio
from minio.error import S3Error

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s"
)
log = logging.getLogger("minio-consumer")

# ── 환경변수 ─────────────────────────────────────────────────
KAFKA_BOOTSTRAP        = os.environ["KAFKA_BOOTSTRAP"]
KAFKA_GROUP_ID         = os.environ.get("KAFKA_GROUP_ID", "minio-consumer-group")
KAFKA_TOPICS           = os.environ["KAFKA_TOPICS"].split(",")
KAFKA_AUTO_OFFSET_RESET = os.environ.get("KAFKA_AUTO_OFFSET_RESET", "earliest")

MINIO_ENDPOINT  = os.environ["MINIO_ENDPOINT"].replace("http://", "").replace("https://", "")
MINIO_ACCESS_KEY = os.environ["MINIO_ACCESS_KEY"]
MINIO_SECRET_KEY = os.environ["MINIO_SECRET_KEY"]
MINIO_SECURE    = os.environ.get("MINIO_ENDPOINT", "").startswith("https")

TOPIC_BUCKET_MAP: dict[str, str] = json.loads(os.environ["TOPIC_BUCKET_MAP"])
OBJECT_PATH_PATTERN = os.environ.get("OBJECT_PATH_PATTERN", "{date}/{hour}/{timestamp}.{ext}")

BATCH_SIZE     = int(os.environ.get("BATCH_SIZE",     "100"))
FLUSH_INTERVAL = int(os.environ.get("FLUSH_INTERVAL", "5"))


# ── 오브젝트 경로 생성 ─────────────────────────────────────────
def make_object_path(topic: str, ts_ns: int, ext: str) -> str:
    ts = datetime.fromtimestamp(ts_ns / 1e9, tz=timezone.utc)
    return OBJECT_PATH_PATTERN.format(
        date=ts.strftime("%Y-%m-%d"),
        hour=ts.strftime("%H"),
        timestamp=f"{ts.strftime('%Y%m%dT%H%M%S%f')}_{ts_ns}",
        ext=ext,
        topic=topic.replace(".", "_"),
    )


# ── Payload 파서 ──────────────────────────────────────────────
def decode_camera_message(raw: bytes) -> tuple[bytes, int]:
    """Binary: [8B ts_ns][4B payload_len][JPEG bytes]"""
    if len(raw) < 12:
        raise ValueError("Camera message too short")
    ts_ns, payload_len = struct.unpack_from(">QI", raw, 0)
    jpeg_bytes = raw[12: 12 + payload_len]
    return jpeg_bytes, ts_ns


def is_camera_topic(topic: str) -> bool:
    return "cam" in topic.lower()


# ── MinIO 클라이언트 ──────────────────────────────────────────
def build_minio_client() -> Minio:
    log.info("Connecting MinIO  endpoint=%s  secure=%s", MINIO_ENDPOINT, MINIO_SECURE)
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )
    # 버킷 존재 확인
    for bucket in set(TOPIC_BUCKET_MAP.values()):
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)
            log.info("Created bucket: %s", bucket)
    return client


# ── Kafka Consumer ──────────────────────────────────────────
def build_consumer() -> Consumer:
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": KAFKA_GROUP_ID,
        "auto.offset.reset": KAFKA_AUTO_OFFSET_RESET,
        "enable.auto.commit": False,        # 수동 커밋 (MinIO 저장 후 커밋)
        "fetch.max.bytes": 52_428_800,      # 50 MB
        "max.partition.fetch.bytes": 5_242_880,
        "session.timeout.ms": 30_000,
        "heartbeat.interval.ms": 10_000,
    }
    c = Consumer(conf)
    c.subscribe(KAFKA_TOPICS)
    log.info("Subscribed topics: %s", KAFKA_TOPICS)
    return c


# ── 메인 루프 ─────────────────────────────────────────────────
def main():
    consumer  = build_consumer()
    minio_cli = build_minio_client()

    pending   = []          # (bucket, object_path, data_bytes, content_type)
    last_flush = time.monotonic()
    total_saved = 0

    log.info("Consumer loop started  batch=%d  flush_interval=%ds",
             BATCH_SIZE, FLUSH_INTERVAL)

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                pass
            elif msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    log.debug("EOF partition %d", msg.partition())
                elif msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    # kafka-init 토픽 생성 전에 뜬 경우 – 크래시 없이 대기
                    log.warning("Topic not ready yet, waiting... (%s)", msg.error())
                    time.sleep(2)
                else:
                    log.error("Kafka error: %s", msg.error())
            else:
                topic = msg.topic()
                raw   = msg.value()
                bucket = TOPIC_BUCKET_MAP.get(topic)

                if bucket is None:
                    log.warning("Unknown topic: %s", topic)
                else:
                    try:
                        if is_camera_topic(topic):
                            jpeg_bytes, ts_ns = decode_camera_message(raw)
                            obj_path = make_object_path(topic, ts_ns, "jpg")
                            pending.append((bucket, obj_path, jpeg_bytes, "image/jpeg"))
                        else:
                            # IMU / GNSS: JSON 그대로 저장
                            payload = json.loads(raw.decode())
                            ts_ns = payload.get("ts_ns", time.time_ns())
                            obj_path = make_object_path(topic, ts_ns, "json")
                            json_bytes = json.dumps(payload, ensure_ascii=False).encode()
                            pending.append((bucket, obj_path, json_bytes, "application/json"))
                    except Exception as e:
                        log.error("Decode error topic=%s: %s", topic, e)

            # ── flush 조건: 배치 사이즈 or 시간 초과 ──────────
            now = time.monotonic()
            if len(pending) >= BATCH_SIZE or (pending and now - last_flush >= FLUSH_INTERVAL):
                saved = 0
                for (bucket, obj_path, data, content_type) in pending:
                    try:
                        minio_cli.put_object(
                            bucket,
                            obj_path,
                            data=BytesIO(data),
                            length=len(data),
                            content_type=content_type,
                        )
                        saved += 1
                    except S3Error as e:
                        log.error("MinIO put_object failed: %s", e)

                consumer.commit(asynchronous=False)
                total_saved += saved
                log.info("Flushed %d objects  total=%d", saved, total_saved)
                pending.clear()
                last_flush = time.monotonic()

    except KeyboardInterrupt:
        log.info("Interrupted – flushing remaining %d items…", len(pending))
        for (bucket, obj_path, data, content_type) in pending:
            try:
                minio_cli.put_object(bucket, obj_path, BytesIO(data), len(data),
                                     content_type=content_type)
            except Exception as e:
                log.error("Final flush error: %s", e)
        consumer.commit(asynchronous=False)
    finally:
        consumer.close()
        log.info("Consumer closed.  total_saved=%d", total_saved)


if __name__ == "__main__":
    main()
