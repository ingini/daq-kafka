# ============================================================
#  consumers/minio/minio_consumer.py
#  Confluent Kafka → MinIO
#
#  - Camera (binary JPEG) → .jpg
#  - IMU/GNSS (JSON)      → .parquet  (IMU_STORAGE_FORMAT=parquet)
#                         → .json     (IMU_STORAGE_FORMAT=json)
#
#  Parquet 이점:
#    JSON  : 1000 rows ≈ 150~200 KB (텍스트 반복, 키 중복)
#    Parquet: 1000 rows ≈  15~25 KB (컬럼 압축, snappy)
#    → 약 8~10배 용량 절감, pandas/DuckDB/Spark 직접 쿼리 가능
# ============================================================

import os, json, struct, time, logging, io
from datetime import datetime, timezone
from confluent_kafka import Consumer, KafkaException, KafkaError
from minio import Minio
from minio.error import S3Error

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [minio-consumer] %(levelname)s: %(message)s")
log = logging.getLogger("minio-consumer")

# ── 환경변수 ─────────────────────────────────────────────────
KAFKA_BOOTSTRAP         = os.environ["KAFKA_BOOTSTRAP"]
KAFKA_GROUP_ID          = os.environ.get("KAFKA_GROUP_ID", "minio-consumer-group")
KAFKA_TOPICS            = os.environ["KAFKA_TOPICS"].split(",")
KAFKA_AUTO_OFFSET_RESET = os.environ.get("KAFKA_AUTO_OFFSET_RESET", "earliest")

MINIO_ENDPOINT   = os.environ["MINIO_ENDPOINT"].replace("http://","").replace("https://","")
MINIO_ACCESS_KEY = os.environ["MINIO_ACCESS_KEY"]
MINIO_SECRET_KEY = os.environ["MINIO_SECRET_KEY"]
MINIO_SECURE     = os.environ.get("MINIO_ENDPOINT","").startswith("https")

TOPIC_BUCKET_MAP: dict = json.loads(os.environ["TOPIC_BUCKET_MAP"])
OBJECT_PATH_PATTERN     = os.environ.get("OBJECT_PATH_PATTERN", "{date}/{hour}/{timestamp}.{ext}")

BATCH_SIZE      = int(os.environ.get("BATCH_SIZE",     "100"))
FLUSH_INTERVAL  = int(os.environ.get("FLUSH_INTERVAL", "5"))

# IMU/GNSS 저장 포맷: parquet | json
IMU_STORAGE_FORMAT  = os.environ.get("IMU_STORAGE_FORMAT", "parquet").lower()
IMU_PARQUET_BATCH   = int(os.environ.get("IMU_PARQUET_BATCH", "1000"))

# ── Parquet 가용성 확인 ───────────────────────────────────────
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    PARQUET_AVAILABLE = True
except ImportError:
    PARQUET_AVAILABLE = False
    log.warning("pyarrow not installed — falling back to JSON for IMU/GNSS")

USE_PARQUET = (IMU_STORAGE_FORMAT == "parquet") and PARQUET_AVAILABLE


def make_object_path(topic: str, ts_ns: int, ext: str) -> str:
    ts = datetime.fromtimestamp(ts_ns / 1e9, tz=timezone.utc)
    return OBJECT_PATH_PATTERN.format(
        date=ts.strftime("%Y-%m-%d"),
        hour=ts.strftime("%H"),
        timestamp=f"{ts.strftime('%Y%m%dT%H%M%S%f')}_{ts_ns}",
        ext=ext, topic=topic.replace(".","_"),
    )


def decode_camera(raw: bytes) -> tuple[bytes, int]:
    if len(raw) < 12:
        raise ValueError("too short")
    ts_ns, plen = struct.unpack_from(">QI", raw, 0)
    return raw[12:12+plen], ts_ns


def is_camera_topic(topic: str) -> bool:
    return "cam" in topic.lower()


def build_minio() -> Minio:
    client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY,
                   secret_key=MINIO_SECRET_KEY, secure=MINIO_SECURE)
    for bucket in set(TOPIC_BUCKET_MAP.values()):
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)
            log.info("Created bucket: %s", bucket)
    return client


def build_consumer() -> Consumer:
    c = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": KAFKA_GROUP_ID,
        "auto.offset.reset": KAFKA_AUTO_OFFSET_RESET,
        "enable.auto.commit": False,
        "fetch.max.bytes": 52_428_800,
        "max.partition.fetch.bytes": 5_242_880,
    })
    c.subscribe(KAFKA_TOPICS)
    log.info("Subscribed topics: %s", KAFKA_TOPICS)
    return c


def flush_parquet(rows: list[dict], bucket: str, topic: str,
                  minio_cli: Minio) -> int:
    """IMU/GNSS rows → Parquet → MinIO"""
    if not rows:
        return 0
    try:
        table = pa.Table.from_pylist(rows)
        buf = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        buf.seek(0)
        ts_ns = rows[0].get("ts_ns", time.time_ns())
        obj_path = make_object_path(topic, ts_ns, "parquet")
        data = buf.getvalue()
        minio_cli.put_object(bucket, obj_path, io.BytesIO(data), len(data),
                             content_type="application/octet-stream")
        log.info("Parquet flush  bucket=%s  rows=%d  size=%d B  path=%s",
                 bucket, len(rows), len(data), obj_path)
        return 1
    except Exception as e:
        log.error("Parquet flush error: %s", e)
        return 0


def main():
    consumer  = build_consumer()
    minio_cli = build_minio()

    pending_cam  = []  # (bucket, obj_path, bytes, content_type)
    pending_imu  = {}  # topic → list[dict]  (parquet 누적)
    last_flush   = time.monotonic()
    total_saved  = 0

    log.info("Consumer loop started  batch=%d  flush_interval=%ds  imu_format=%s",
             BATCH_SIZE, FLUSH_INTERVAL, "parquet" if USE_PARQUET else "json")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                pass
            elif msg.error():
                code = msg.error().code()
                if code == KafkaError._PARTITION_EOF:
                    pass
                elif code == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    log.warning("Topic not ready yet: %s", msg.error())
                    time.sleep(2)
                else:
                    log.error("Kafka error: %s", msg.error())
            else:
                topic  = msg.topic()
                raw    = msg.value()
                bucket = TOPIC_BUCKET_MAP.get(topic)
                if bucket is None:
                    continue

                try:
                    if is_camera_topic(topic):
                        jpeg, ts_ns = decode_camera(raw)
                        obj_path = make_object_path(topic, ts_ns, "jpg")
                        pending_cam.append((bucket, obj_path, jpeg, "image/jpeg"))
                    else:
                        payload = json.loads(raw.decode())
                        ts_ns = payload.get("ts_ns", time.time_ns())

                        if USE_PARQUET:
                            if topic not in pending_imu:
                                pending_imu[topic] = []
                            pending_imu[topic].append(payload)
                        else:
                            obj_path = make_object_path(topic, ts_ns, "json")
                            pending_cam.append((bucket, obj_path,
                                json.dumps(payload).encode(), "application/json"))
                except Exception as e:
                    log.error("Decode error topic=%s: %s", topic, e)

            now = time.monotonic()
            cam_full    = len(pending_cam) >= BATCH_SIZE
            imu_full    = USE_PARQUET and any(
                len(v) >= IMU_PARQUET_BATCH for v in pending_imu.values())
            time_up     = (pending_cam or pending_imu) and (now - last_flush >= FLUSH_INTERVAL)

            if cam_full or imu_full or time_up:
                saved = 0
                # Camera JPEG flush
                for (bkt, path, data, ctype) in pending_cam:
                    try:
                        minio_cli.put_object(bkt, path, io.BytesIO(data), len(data),
                                             content_type=ctype)
                        saved += 1
                    except S3Error as e:
                        log.error("MinIO error: %s", e)

                # IMU Parquet flush
                for topic_key, rows in list(pending_imu.items()):
                    if rows:
                        bkt = TOPIC_BUCKET_MAP.get(topic_key, "daq-imu")
                        saved += flush_parquet(rows, bkt, topic_key, minio_cli)
                        pending_imu[topic_key] = []

                consumer.commit(asynchronous=False)
                total_saved += saved
                log.info("Flushed %d objects  total=%d", saved, total_saved)
                pending_cam.clear()
                last_flush = time.monotonic()

    except KeyboardInterrupt:
        pass
    finally:
        # 남은 데이터 flush
        for (bkt, path, data, ctype) in pending_cam:
            try:
                minio_cli.put_object(bkt, path, io.BytesIO(data), len(data),
                                     content_type=ctype)
            except Exception: pass
        for topic_key, rows in pending_imu.items():
            if rows:
                bkt = TOPIC_BUCKET_MAP.get(topic_key, "daq-imu")
                flush_parquet(rows, bkt, topic_key, minio_cli)
        consumer.commit(asynchronous=False)
        consumer.close()
        log.info("Consumer closed.  total_saved=%d", total_saved)


if __name__ == "__main__":
    main()
