# ============================================================
#  consumers/minio/minio_consumer.py
#  Confluent Kafka → Storage (MinIO or Local/USB)
#
#  STORAGE_BACKEND=minio  : MinIO S3 저장
#  STORAGE_BACKEND=local  : 로컬 디스크 / USB 마운트 경로 저장
#
#  경로 구조 (공통):
#    {root}/{year}/{month}/{day}/{hour}/{vehicle_id}/{sensor}/{timestamp}.{ext}
#
#  MinIO 예) daq/2026/04/30/09/KOR-1234/cam0/20260430T091530_xxx.jpg
#  Local  예) /mnt/usb/DAQ/2026/04/30/09/KOR-1234/cam0/20260430T091530_xxx.jpg
# ============================================================

import os, json, struct, time, logging, io
from datetime import datetime, timezone
from pathlib import Path
from confluent_kafka import Consumer, KafkaException, KafkaError

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [minio-consumer] %(levelname)s: %(message)s")
log = logging.getLogger("minio-consumer")

# ── 공통 환경변수 ─────────────────────────────────────────────
KAFKA_BOOTSTRAP         = os.environ["KAFKA_BOOTSTRAP"]
KAFKA_GROUP_ID          = os.environ.get("KAFKA_GROUP_ID", "minio-consumer-group")
KAFKA_TOPICS            = os.environ["KAFKA_TOPICS"].split(",")
KAFKA_AUTO_OFFSET_RESET = os.environ.get("KAFKA_AUTO_OFFSET_RESET", "earliest")

VEHICLE_ID   = os.environ.get("VEHICLE_ID", "UNKNOWN")
BATCH_SIZE   = int(os.environ.get("BATCH_SIZE",     "100"))
FLUSH_INTERVAL = int(os.environ.get("FLUSH_INTERVAL", "5"))

IMU_STORAGE_FORMAT = os.environ.get("IMU_STORAGE_FORMAT", "parquet").lower()
IMU_PARQUET_BATCH  = int(os.environ.get("IMU_PARQUET_BATCH", "1000"))

# ── 스토리지 백엔드 선택 ──────────────────────────────────────
# minio : MinIO S3
# local : 로컬 디스크 / USB 마운트
STORAGE_BACKEND = os.environ.get("STORAGE_BACKEND", "minio").lower()

# MinIO 설정
MINIO_ENDPOINT   = os.environ.get("MINIO_ENDPOINT", "http://minio:9000").replace("http://","").replace("https://","")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin123")
MINIO_SECURE     = os.environ.get("MINIO_ENDPOINT","").startswith("https")
MINIO_BUCKET     = os.environ.get("MINIO_BUCKET", "daq")

# Local 설정
# LOCAL_BASE_PATH = /media/$USER  형태로 설정하면 USB 이름 자동 탐지
LOCAL_BASE_PATH  = os.environ.get("LOCAL_BASE_PATH", f"/media/{os.environ.get('USER', 'user')}")

TOPIC_SENSOR_MAP: dict = json.loads(
    os.environ.get("TOPIC_SENSOR_MAP", json.dumps({
        "sensor.cam0.jpeg": "cam0",
        "sensor.cam1.jpeg": "cam1",
        "sensor.cam2.jpeg": "cam2",
        "sensor.imu":       "imu",
        "sensor.gnss":      "gnss",
    }))
)

# ── Parquet ───────────────────────────────────────────────────
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    PARQUET_AVAILABLE = True
except ImportError:
    PARQUET_AVAILABLE = False
    log.warning("pyarrow not installed — fallback to JSON")

USE_PARQUET = (IMU_STORAGE_FORMAT == "parquet") and PARQUET_AVAILABLE


# ── 경로 생성 (MinIO key / Local path 공통) ───────────────────
def make_rel_path(sensor: str, ts_ns: int, ext: str) -> str:
    """공통 상대경로: year/month/day/hour/vehicle_id/sensor/timestamp.ext"""
    ts = datetime.fromtimestamp(ts_ns / 1e9, tz=timezone.utc)
    ts_str = f"{ts.strftime('%Y%m%dT%H%M%S')}_{ts_ns}"
    return (
        f"{ts.strftime('%Y')}/"
        f"{ts.strftime('%m')}/"
        f"{ts.strftime('%d')}/"
        f"{ts.strftime('%H')}/"
        f"{VEHICLE_ID}/"
        f"{sensor}/"
        f"{ts_str}.{ext}"
    )


# ── MinIO 백엔드 ──────────────────────────────────────────────
class MinIOBackend:
    def __init__(self):
        from minio import Minio
        self.client = Minio(MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=MINIO_SECURE)
        if not self.client.bucket_exists(MINIO_BUCKET):
            self.client.make_bucket(MINIO_BUCKET)
            log.info("Created bucket: %s", MINIO_BUCKET)
        log.info("MinIO backend ready  bucket=%s", MINIO_BUCKET)

    def save(self, rel_path: str, data: bytes, content_type: str, **kwargs):
        from minio.error import S3Error
        try:
            self.client.put_object(
                MINIO_BUCKET, rel_path,
                io.BytesIO(data), len(data),
                content_type=content_type)
            log.debug("MinIO saved: %s (%d B)", rel_path, len(data))
        except S3Error as e:
            log.error("MinIO save error: %s", e)
            raise

    def info(self) -> str:
        return f"MinIO  bucket={MINIO_BUCKET}  endpoint={MINIO_ENDPOINT}"


# ── Local 백엔드 (USB 자동 탐지) ─────────────────────────────
class LocalBackend:
    """
    LOCAL_BASE_PATH=/media/$USER 로 설정하면
    해당 경로 아래 첫 번째 마운트된 USB 디렉토리를 자동 탐지.

    저장 경로:
      {LOCAL_BASE_PATH}/{usb_name}/{yyyymmdd}/{vehicle_id}/{sensor}/{timestamp}.{ext}
      예) /media/swm/USB_32G/20260430/KOR-1234/cam0/20260430T091530_xxx.jpg

    USB 가 없으면 {LOCAL_BASE_PATH}/DAQ/{yyyymmdd}/... 에 저장.
    """

    def __init__(self):
        self.base_mount = LOCAL_BASE_PATH
        self.usb_name   = self._find_usb()
        self.base_path  = Path(self.base_mount) / self.usb_name
        self.base_path.mkdir(parents=True, exist_ok=True)

        # 쓰기 가능 확인
        test_file = self.base_path / ".write_test"
        try:
            test_file.write_bytes(b"ok")
            test_file.unlink()
        except OSError as e:
            raise RuntimeError(f"Not writable: {self.base_path}  ({e})")

        log.info("Local backend ready  path=%s  usb=%s", self.base_path, self.usb_name)

    def _find_usb(self) -> str:
        """LOCAL_BASE_PATH 아래 첫 번째 디렉토리(USB 이름)를 반환."""
        base = Path(self.base_mount)
        if not base.exists():
            log.warning("LOCAL_BASE_PATH %s not found, using DAQ", self.base_mount)
            return "DAQ"
        dirs = [d for d in base.iterdir() if d.is_dir() and not d.name.startswith(".")]
        if not dirs:
            log.warning("No USB found under %s, using DAQ", self.base_mount)
            return "DAQ"
        usb = sorted(dirs)[0].name
        log.info("USB detected: %s", usb)
        return usb

    def make_path(self, sensor: str, ts_ns: int, ext: str) -> Path:
        """
        {base_path}/{yyyymmdd}/{vehicle_id}/{sensor}/{timestamp}.{ext}
        """
        ts = datetime.fromtimestamp(ts_ns / 1e9, tz=timezone.utc)
        date_str  = ts.strftime("%Y%m%d")
        ts_str    = f"{ts.strftime('%Y%m%dT%H%M%S')}_{ts_ns}"
        full_path = (
            self.base_path
            / date_str
            / VEHICLE_ID
            / sensor
            / f"{ts_str}.{ext}"
        )
        return full_path

    def save(self, rel_path: str, data: bytes, content_type: str,
             ts_ns: int = 0, sensor: str = ""):
        """rel_path 는 MinIO 호환용, Local은 make_path 로 직접 생성."""
        # rel_path 에서 sensor/timestamp 파싱해서 로컬 경로 재구성
        parts   = Path(rel_path)
        ext     = parts.suffix.lstrip(".")
        ts_ns_  = ts_ns if ts_ns else time.time_ns()
        sen_    = sensor if sensor else parts.parent.name

        full_path = self.make_path(sen_, ts_ns_, ext)
        full_path.parent.mkdir(parents=True, exist_ok=True)
        full_path.write_bytes(data)
        log.debug("Local saved: %s (%d B)", full_path, len(data))

    def info(self) -> str:
        import shutil
        try:
            stat     = shutil.disk_usage(str(self.base_path))
            free_gb  = stat.free  / 1024**3
            total_gb = stat.total / 1024**3
            return (f"Local  usb={self.usb_name}  "
                    f"path={self.base_path}  "
                    f"free={free_gb:.1f}GB/{total_gb:.1f}GB")
        except Exception:
            return f"Local  path={self.base_path}"


# ── 백엔드 초기화 ─────────────────────────────────────────────
def build_backend():
    if STORAGE_BACKEND == "local":
        return LocalBackend()
    elif STORAGE_BACKEND == "minio":
        return MinIOBackend()
    else:
        raise ValueError(f"Unknown STORAGE_BACKEND: {STORAGE_BACKEND}")


# ── Kafka Consumer ────────────────────────────────────────────
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
    log.info("Subscribed: %s", KAFKA_TOPICS)
    return c


def decode_camera(raw: bytes) -> tuple:
    if len(raw) < 12:
        raise ValueError("too short")
    ts_ns, plen = struct.unpack_from(">QI", raw, 0)
    return raw[12:12 + plen], ts_ns


def is_camera_topic(topic: str) -> bool:
    return "cam" in topic.lower()


def flush_parquet(rows: list, sensor: str, backend) -> int:
    if not rows:
        return 0
    try:
        table = pa.Table.from_pylist(rows)
        buf = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        ts_ns    = rows[0].get("ts_ns", time.time_ns())
        rel_path = make_rel_path(sensor, ts_ns, "parquet")
        data     = buf.getvalue()
        backend.save(rel_path, data, "application/octet-stream")
        log.info("Parquet  path=%s  rows=%d  size=%dB", rel_path, len(rows), len(data))
        return 1
    except Exception as e:
        log.error("Parquet flush error: %s", e)
        return 0


def main():
    log.info("Starting  backend=%s  vehicle=%s  imu=%s",
             STORAGE_BACKEND, VEHICLE_ID, "parquet" if USE_PARQUET else "json")

    backend  = build_backend()
    consumer = build_consumer()

    log.info("Storage: %s", backend.info())

    pending_cam = []        # (rel_path, bytes, content_type)
    pending_imu = {}        # sensor → list[dict]
    last_flush  = time.monotonic()
    total_saved = 0
    last_info   = time.monotonic()

    log.info("Loop started  batch=%d  flush=%ds", BATCH_SIZE, FLUSH_INTERVAL)

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
                    log.warning("Topic not ready: %s", msg.error())
                    time.sleep(2)
                else:
                    log.error("Kafka error: %s", msg.error())
            else:
                topic  = msg.topic()
                raw    = msg.value()
                sensor = TOPIC_SENSOR_MAP.get(topic)
                if sensor is None:
                    continue

                try:
                    if is_camera_topic(topic):
                        jpeg, ts_ns = decode_camera(raw)
                        rel_path = make_rel_path(sensor, ts_ns, "jpg")
                        pending_cam.append((rel_path, jpeg, "image/jpeg", sensor, ts_ns))
                    else:
                        payload = json.loads(raw.decode())
                        ts_ns   = payload.get("ts_ns", time.time_ns())
                        if USE_PARQUET:
                            pending_imu.setdefault(sensor, []).append(payload)
                        else:
                            rel_path = make_rel_path(sensor, ts_ns, "json")
                            pending_cam.append((rel_path,
                                json.dumps(payload, ensure_ascii=False).encode(),
                                "application/json", sensor, ts_ns))
                except Exception as e:
                    log.error("Decode error topic=%s: %s", topic, e)

            now      = time.monotonic()
            cam_full = len(pending_cam) >= BATCH_SIZE
            imu_full = USE_PARQUET and any(
                len(v) >= IMU_PARQUET_BATCH for v in pending_imu.values())
            time_up  = (pending_cam or pending_imu) and (now - last_flush >= FLUSH_INTERVAL)

            if cam_full or imu_full or time_up:
                saved = 0

                for item in pending_cam:
                    path, data, ctype = item[0], item[1], item[2]
                    sen  = item[3] if len(item) > 3 else ""
                    tsn  = item[4] if len(item) > 4 else 0
                    try:
                        backend.save(path, data, ctype, ts_ns=tsn, sensor=sen)
                        saved += 1
                    except Exception as e:
                        log.error("Save error: %s", e)

                for sensor_key, rows in list(pending_imu.items()):
                    if rows:
                        saved += flush_parquet(rows, sensor_key, backend)
                        pending_imu[sensor_key] = []

                consumer.commit(asynchronous=False)
                total_saved += saved
                log.info("Flushed %d  total=%d", saved, total_saved)
                pending_cam.clear()
                last_flush = time.monotonic()

            # 60초마다 스토리지 상태 로그
            if now - last_info >= 60:
                log.info("Storage status: %s", backend.info())
                last_info = now

    except KeyboardInterrupt:
        pass
    finally:
        log.info("Shutdown — flushing remaining...")
        for item in pending_cam:
            path, data, ctype = item[0], item[1], item[2]
            sen = item[3] if len(item) > 3 else ""
            tsn = item[4] if len(item) > 4 else 0
            try:
                backend.save(path, data, ctype, ts_ns=tsn, sensor=sen)
            except Exception as e:
                log.error("Final flush error: %s", e)
        for sensor_key, rows in pending_imu.items():
            if rows:
                flush_parquet(rows, sensor_key, backend)
        consumer.commit(asynchronous=False)
        consumer.close()
        log.info("Closed. total_saved=%d", total_saved)


if __name__ == "__main__":
    main()
