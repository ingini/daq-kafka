# ============================================================
#  consumers/minio/minio_consumer.py
#  Confluent Kafka → MinIO (단일 버킷, 계층형 경로)
#
#  오브젝트 경로:
#    DAQ/{year}/{month}/{day}/{hour}/{vehicle_id}/{sensor}/{timestamp}.{ext}
#
#  예) DAQ/2026/04/30/09/KOR-1234/cam0/20260430T091530_1777505277.jpg
#      DAQ/2026/04/30/09/KOR-1234/imu/20260430T091530_1777505277.parquet
#      DAQ/2026/04/30/09/KOR-1234/gnss/20260430T091530_1777505277.parquet
#
#  - Camera (binary JPEG) → .jpg
#  - IMU/GNSS (JSON)      → .parquet  (IMU_STORAGE_FORMAT=parquet)
#                         → .json     (IMU_STORAGE_FORMAT=json)
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

MINIO_BUCKET = os.environ.get("MINIO_BUCKET",  "DAQ")
VEHICLE_ID   = os.environ.get("VEHICLE_ID",    "UNKNOWN")

# topic → sensor 폴더명 매핑
TOPIC_SENSOR_MAP: dict[str, str] = json.loads(
    os.environ.get("TOPIC_SENSOR_MAP", json.dumps({
        "sensor.cam0.jpeg": "cam0",
        "sensor.cam1.jpeg": "cam1",
        "sensor.cam2.jpeg": "cam2",
        "sensor.imu":       "imu",
        "sensor.gnss":      "gnss",
    }))
)

BATCH_SIZE     = int(os.environ.get("BATCH_SIZE",     "100"))
FLUSH_INTERVAL = int(os.environ.get("FLUSH_INTERVAL", "5"))

IMU_STORAGE_FORMAT = os.environ.get("IMU_STORAGE_FORMAT", "parquet").lower()
IMU_PARQUET_BATCH  = int(os.environ.get("IMU_PARQUET_BATCH", "1000"))

# ── Parquet 가용성 확인 ───────────────────────────────────────
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    PARQUET_AVAILABLE = True
except ImportError:
    PARQUET_AVAILABLE = False
    log.warning("pyarrow not installed — falling back to JSON for IMU/GNSS")

USE_PARQUET = (IMU_STORAGE_FORMAT == "parquet") and PARQUET_AVAILABLE


# ── 오브젝트 경로 생성 ────────────────────────────────────────
# DAQ/{year}/{month}/{day}/{hour}/{vehicle_id}/{sensor}/{timestamp}.{ext}
def make_object_path(sensor: str, ts_ns: int, ext: str) -> str:
    ts = datetime.fromtimestamp(ts_ns / 1e9, tz=timezone.utc)
    timestamp = f"{ts.strftime('%Y%m%dT%H%M%S')}_{ts_ns}"
    return (
        f"{ts.strftime('%Y')}/"
        f"{ts.strftime('%m')}/"
        f"{ts.strftime('%d')}/"
        f"{ts.strftime('%H')}/"
        f"{VEHICLE_ID}/"
        f"{sensor}/"
        f"{timestamp}.{ext}"
    )


def decode_camera(raw: bytes) -> tuple[bytes, int]:
    if len(raw) < 12:
        raise ValueError("too short")
    ts_ns, plen = struct.unpack_from(">QI", raw, 0)
    return raw[12:12 + plen], ts_ns


def is_camera_topic(topic: str) -> bool:
    return "cam" in topic.lower()


def build_minio() -> Minio:
    client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY,
                   secret_key=MINIO_SECRET_KEY, secure=MINIO_SECURE)
    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)
        log.info("Created bucket: %s", MINIO_BUCKET)
    else:
        log.info("Bucket exists: %s", MINIO_BUCKET)
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


def flush_parquet(rows: list[dict], sensor: str, minio_cli: Minio) -> int:
    """IMU/GNSS rows → Parquet (snappy) → MinIO"""
    if not rows:
        return 0
    try:
        table = pa.Table.from_pylist(rows)
        buf = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        buf.seek(0)
        ts_ns    = rows[0].get("ts_ns", time.time_ns())
        obj_path = make_object_path(sensor, ts_ns, "parquet")
        data     = buf.getvalue()
        minio_cli.put_object(
            MINIO_BUCKET, obj_path,
            io.BytesIO(data), len(data),
            content_type="application/octet-stream",
        )
        log.info("Parquet  bucket=%s  path=%s  rows=%d  size=%d B",
                 MINIO_BUCKET, obj_path, len(rows), len(data))
        return 1
    except Exception as e:
        log.error("Parquet flush error: %s", e)
        return 0


def main():
    log.info("Starting  bucket=%s  vehicle=%s  imu_format=%s",
             MINIO_BUCKET, VEHICLE_ID, "parquet" if USE_PARQUET else "json")

    consumer  = build_consumer()
    minio_cli = build_minio()

    pending_cam = []        # (obj_path, bytes, content_type)
    pending_imu = {}        # sensor → list[dict]
    last_flush  = time.monotonic()
    total_saved = 0

    log.info("Consumer loop started  batch=%d  flush_interval=%ds",
             BATCH_SIZE, FLUSH_INTERVAL)

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
                sensor = TOPIC_SENSOR_MAP.get(topic)
                if sensor is None:
                    log.warning("Unknown topic: %s", topic)
                    continue

                try:
                    if is_camera_topic(topic):
                        jpeg, ts_ns = decode_camera(raw)
                        obj_path = make_object_path(sensor, ts_ns, "jpg")
                        pending_cam.append((obj_path, jpeg, "image/jpeg"))

                    else:
                        payload = json.loads(raw.decode())
                        ts_ns   = payload.get("ts_ns", time.time_ns())

                        if USE_PARQUET:
                            pending_imu.setdefault(sensor, []).append(payload)
                        else:
                            obj_path = make_object_path(sensor, ts_ns, "json")
                            pending_cam.append((
                                obj_path,
                                json.dumps(payload, ensure_ascii=False).encode(),
                                "application/json",
                            ))
                except Exception as e:
                    log.error("Decode error topic=%s: %s", topic, e)

            # ── flush 조건 ────────────────────────────────────
            now      = time.monotonic()
            cam_full = len(pending_cam) >= BATCH_SIZE
            imu_full = USE_PARQUET and any(
                len(v) >= IMU_PARQUET_BATCH for v in pending_imu.values())
            time_up  = (pending_cam or pending_imu) and (now - last_flush >= FLUSH_INTERVAL)

            if cam_full or imu_full or time_up:
                saved = 0

                # Camera / JSON flush
                for (path, data, ctype) in pending_cam:
                    try:
                        minio_cli.put_object(
                            MINIO_BUCKET, path,
                            io.BytesIO(data), len(data),
                            content_type=ctype,
                        )
                        saved += 1
                    except S3Error as e:
                        log.error("MinIO put error: %s", e)

                # IMU Parquet flush
                for sensor_key, rows in list(pending_imu.items()):
                    if rows:
                        saved += flush_parquet(rows, sensor_key, minio_cli)
                        pending_imu[sensor_key] = []

                consumer.commit(asynchronous=False)
                total_saved += saved
                log.info("Flushed %d objects  total=%d", saved, total_saved)
                pending_cam.clear()
                last_flush = time.monotonic()

    except KeyboardInterrupt:
        pass
    finally:
        log.info("Shutdown — flushing remaining data...")
        for (path, data, ctype) in pending_cam:
            try:
                minio_cli.put_object(MINIO_BUCKET, path,
                    io.BytesIO(data), len(data), content_type=ctype)
            except Exception as e:
                log.error("Final flush error: %s", e)

        for sensor_key, rows in pending_imu.items():
            if rows:
                flush_parquet(rows, sensor_key, minio_cli)

        consumer.commit(asynchronous=False)
        consumer.close()
        log.info("Consumer closed.  total_saved=%d", total_saved)


if __name__ == "__main__":
    main()
