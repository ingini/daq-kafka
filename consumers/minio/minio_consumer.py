# ============================================================
#  consumers/minio/minio_consumer.py
#  Confluent Kafka → Storage (우선순위 Fallback)
#
#  저장 우선순위:
#    1. MinIO  (STORAGE_BACKEND=minio 또는 auto)
#    2. USB    (/media/$USER/{usb_name})
#    3. 내부   (LOCAL_FALLBACK_PATH=/data/DAQ)
#
#  STORAGE_BACKEND=auto  : MinIO 시도 → 실패 시 USB → 실패 시 내부
#  STORAGE_BACKEND=minio : MinIO 전용 (실패 시 에러)
#  STORAGE_BACKEND=local : USB → 실패 시 내부 (MinIO 사용 안 함)
#
#  저장 경로 (공통):
#    MinIO : {bucket}/{year}/{month}/{day}/{hour}/{vehicle_id}/{sensor}/{ts}.{ext}
#    Local : {base}/{usb_name}/{yyyymmdd}/{vehicle_id}/{sensor}/{ts}.{ext}
# ============================================================

import os, json, struct, time, logging, io, shutil
from datetime import datetime, timezone
from pathlib import Path
from confluent_kafka import Consumer, KafkaException, KafkaError

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [minio-consumer] %(levelname)s: %(message)s")
log = logging.getLogger("minio-consumer")

# ── 환경변수 ─────────────────────────────────────────────────
KAFKA_BOOTSTRAP         = os.environ["KAFKA_BOOTSTRAP"]
KAFKA_GROUP_ID          = os.environ.get("KAFKA_GROUP_ID", "minio-consumer-group")
KAFKA_TOPICS            = os.environ["KAFKA_TOPICS"].split(",")
KAFKA_AUTO_OFFSET_RESET = os.environ.get("KAFKA_AUTO_OFFSET_RESET", "earliest")

VEHICLE_ID     = os.environ.get("VEHICLE_ID",   "UNKNOWN")
BATCH_SIZE     = int(os.environ.get("BATCH_SIZE",     "100"))
FLUSH_INTERVAL = int(os.environ.get("FLUSH_INTERVAL", "5"))

IMU_STORAGE_FORMAT = os.environ.get("IMU_STORAGE_FORMAT", "parquet").lower()
IMU_PARQUET_BATCH  = int(os.environ.get("IMU_PARQUET_BATCH", "1000"))

# minio | local | auto
STORAGE_BACKEND = os.environ.get("STORAGE_BACKEND", "auto").lower()

# MinIO
MINIO_ENDPOINT   = os.environ.get("MINIO_ENDPOINT",   "http://minio:9000").replace("http://","").replace("https://","")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin123")
MINIO_SECURE     = os.environ.get("MINIO_ENDPOINT",   "").startswith("https")
MINIO_BUCKET     = os.environ.get("MINIO_BUCKET",     "daq")

# Local
LOCAL_BASE_PATH     = os.environ.get("LOCAL_BASE_PATH",     f"/media/{os.environ.get('USER','user')}")
LOCAL_FALLBACK_PATH = os.environ.get("LOCAL_FALLBACK_PATH", "/data/DAQ")  # 내부 디스크

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


# ── 경로 생성 헬퍼 ────────────────────────────────────────────
def make_minio_key(sensor: str, ts_ns: int, ext: str) -> str:
    ts     = datetime.fromtimestamp(ts_ns / 1e9, tz=timezone.utc)
    ts_str = f"{ts.strftime('%Y%m%dT%H%M%S')}_{ts_ns}"
    return (f"{ts.strftime('%Y')}/{ts.strftime('%m')}/{ts.strftime('%d')}/"
            f"{ts.strftime('%H')}/{VEHICLE_ID}/{sensor}/{ts_str}.{ext}")


def make_local_path(base: Path, sensor: str, ts_ns: int, ext: str) -> Path:
    ts     = datetime.fromtimestamp(ts_ns / 1e9, tz=timezone.utc)
    ts_str = f"{ts.strftime('%Y%m%dT%H%M%S')}_{ts_ns}"
    return base / ts.strftime("%Y%m%d") / VEHICLE_ID / sensor / f"{ts_str}.{ext}"


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
        log.info("[MinIO] ready  bucket=%s  endpoint=%s", MINIO_BUCKET, MINIO_ENDPOINT)

    def save(self, sensor: str, ts_ns: int, data: bytes, content_type: str):
        ext = "jpg" if content_type == "image/jpeg" else \
              "parquet" if "octet" in content_type else "json"
        key = make_minio_key(sensor, ts_ns, ext)
        self.client.put_object(MINIO_BUCKET, key,
            io.BytesIO(data), len(data), content_type=content_type)

    def info(self) -> str:
        return f"MinIO bucket={MINIO_BUCKET} endpoint={MINIO_ENDPOINT}"


# ── Local 백엔드 ──────────────────────────────────────────────
class LocalBackend:
    def __init__(self, base_path: str, label: str = "local"):
        self.label     = label
        self.base_path = self._resolve_path(base_path)
        self._check_writable()
        log.info("[%s] ready  path=%s", self.label, self.base_path)

    def _resolve_path(self, base: str) -> Path:
        """
        base=/media/$USER 형태면 USB 이름 자동 탐지.
        base=/data/DAQ 형태면 그대로 사용.
        """
        p = Path(base)
        if not p.exists():
            p.mkdir(parents=True, exist_ok=True)
            return p

        # /media/$USER 처럼 마운트 루트인지 확인
        dirs = [d for d in p.iterdir() if d.is_dir() and not d.name.startswith(".")]
        if dirs and str(p).startswith("/media"):
            usb = sorted(dirs)[0]
            log.info("[%s] USB detected: %s", self.label, usb.name)
            return usb
        return p

    def _check_writable(self):
        self.base_path.mkdir(parents=True, exist_ok=True)
        test = self.base_path / ".write_test"
        test.write_bytes(b"ok")
        test.unlink()

    def save(self, sensor: str, ts_ns: int, data: bytes, content_type: str):
        ext = "jpg" if content_type == "image/jpeg" else \
              "parquet" if "octet" in content_type else "json"
        path = make_local_path(self.base_path, sensor, ts_ns, ext)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)

    def info(self) -> str:
        try:
            stat = shutil.disk_usage(str(self.base_path))
            free_gb  = stat.free  / 1024**3
            total_gb = stat.total / 1024**3
            return (f"{self.label}  path={self.base_path}  "
                    f"free={free_gb:.1f}GB/{total_gb:.1f}GB")
        except Exception:
            return f"{self.label}  path={self.base_path}"


# ── Fallback 백엔드 체인 ──────────────────────────────────────
class FallbackBackend:
    """
    backends 리스트 순서대로 저장 시도.
    앞 순위 실패 시 다음 순위로 자동 전환.
    """
    def __init__(self, backends: list):
        self.backends = backends
        self.active   = backends[0] if backends else None

    def save(self, sensor: str, ts_ns: int, data: bytes, content_type: str):
        for i, backend in enumerate(self.backends):
            try:
                backend.save(sensor, ts_ns, data, content_type)
                # 성공한 백엔드가 현재 active 와 다르면 전환 알림
                if backend is not self.active:
                    log.warning("[Fallback] switched to %s", backend.info())
                    self.active = backend
                return
            except Exception as e:
                label = getattr(backend, "label", type(backend).__name__)
                log.warning("[Fallback] %s failed: %s — trying next...", label, e)
        log.error("[Fallback] ALL backends failed for sensor=%s ts=%d", sensor, ts_ns)

    def info(self) -> str:
        return " | ".join(b.info() for b in self.backends)


# ── 백엔드 초기화 ─────────────────────────────────────────────
def build_backend():
    if STORAGE_BACKEND == "minio":
        return MinIOBackend()

    elif STORAGE_BACKEND == "local":
        # USB → 내부 디스크 순서
        try:
            return LocalBackend(LOCAL_BASE_PATH, label="USB")
        except Exception as e:
            log.warning("USB init failed (%s), using internal disk", e)
            return LocalBackend(LOCAL_FALLBACK_PATH, label="internal")

    else:  # auto: USB → MinIO → 내부 디스크
        backends = []

        # 1순위: USB
        try:
            backends.append(LocalBackend(LOCAL_BASE_PATH, label="USB"))
            log.info("[auto] USB available")
        except Exception as e:
            log.warning("[auto] USB unavailable: %s", e)

        # 2순위: MinIO
        try:
            backends.append(MinIOBackend())
            log.info("[auto] MinIO available")
        except Exception as e:
            log.warning("[auto] MinIO unavailable: %s", e)

        # 3순위: 내부 디스크
        try:
            backends.append(LocalBackend(LOCAL_FALLBACK_PATH, label="internal"))
            log.info("[auto] Internal disk available")
        except Exception as e:
            log.warning("[auto] Internal disk unavailable: %s", e)

        if not backends:
            raise RuntimeError("No storage backend available!")

        return FallbackBackend(backends) if len(backends) > 1 else backends[0]


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
        buf   = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        ts_ns = rows[0].get("ts_ns", time.time_ns())
        data  = buf.getvalue()
        backend.save(sensor, ts_ns, data, "application/octet-stream")
        log.info("Parquet  sensor=%s  rows=%d  size=%dB", sensor, len(rows), len(data))
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

    # (sensor, ts_ns, data, content_type)
    pending_cam = []
    pending_imu = {}        # sensor → list[dict]
    last_flush  = time.monotonic()
    last_info   = time.monotonic()
    total_saved = 0

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
                        pending_cam.append((sensor, ts_ns, jpeg, "image/jpeg"))
                    else:
                        payload = json.loads(raw.decode())
                        ts_ns   = payload.get("ts_ns", time.time_ns())
                        if USE_PARQUET:
                            pending_imu.setdefault(sensor, []).append(payload)
                        else:
                            pending_cam.append((sensor, ts_ns,
                                json.dumps(payload, ensure_ascii=False).encode(),
                                "application/json"))
                except Exception as e:
                    log.error("Decode error topic=%s: %s", topic, e)

            now      = time.monotonic()
            cam_full = len(pending_cam) >= BATCH_SIZE
            imu_full = USE_PARQUET and any(
                len(v) >= IMU_PARQUET_BATCH for v in pending_imu.values())
            time_up  = (pending_cam or pending_imu) and (now - last_flush >= FLUSH_INTERVAL)

            if cam_full or imu_full or time_up:
                saved = 0
                for (sen, tsn, data, ctype) in pending_cam:
                    try:
                        backend.save(sen, tsn, data, ctype)
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

            # 60초마다 스토리지 상태 출력
            if now - last_info >= 60:
                log.info("Storage: %s", backend.info())
                last_info = now

    except KeyboardInterrupt:
        pass
    finally:
        log.info("Shutdown — flushing remaining...")
        for (sen, tsn, data, ctype) in pending_cam:
            try:
                backend.save(sen, tsn, data, ctype)
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
