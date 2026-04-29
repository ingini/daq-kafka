# ============================================================
#  producers/camera/cam_producer.py
#  V4L2 카메라 → JPEG 1fps → Confluent Kafka
#
#  env:
#    KAFKA_BOOTSTRAP, KAFKA_TOPIC
#    CAM_DEVICE, CAP_WIDTH, CAP_HEIGHT, CAP_FPS
#    OUT_WIDTH, OUT_HEIGHT, JPEG_QUALITY, PUBLISH_FPS
# ============================================================

import os
import time
import struct
import json
import logging
import cv2
from confluent_kafka import Producer, KafkaException

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s"
)
log = logging.getLogger("cam-producer")

# ── 환경변수 ─────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP"]
KAFKA_TOPIC     = os.environ["KAFKA_TOPIC"]
CAM_DEVICE      = os.environ.get("CAM_DEVICE", "/dev/video0")
CAP_WIDTH       = int(os.environ.get("CAP_WIDTH",  "1920"))
CAP_HEIGHT      = int(os.environ.get("CAP_HEIGHT", "1080"))
CAP_FPS         = int(os.environ.get("CAP_FPS",    "20"))
OUT_WIDTH       = int(os.environ.get("OUT_WIDTH",  "640"))
OUT_HEIGHT      = int(os.environ.get("OUT_HEIGHT", "360"))
JPEG_QUALITY    = int(os.environ.get("JPEG_QUALITY", "90"))
PUBLISH_FPS     = float(os.environ.get("PUBLISH_FPS", "1"))

PUBLISH_INTERVAL = 1.0 / PUBLISH_FPS   # seconds


def delivery_report(err, msg):
    if err:
        log.error("Delivery failed: %s", err)
    else:
        log.debug("Delivered offset=%d ts=%d", msg.offset(), msg.timestamp()[1])


def build_producer() -> Producer:
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "message.max.bytes": 5_242_880,       # 5 MB
        "compression.type": "none",            # JPEG은 이미 압축됨
        "linger.ms": 0,
        "acks": "1",
        "retries": 3,
        "retry.backoff.ms": 500,
    }
    return Producer(conf)


def build_header(device: str) -> dict:
    """Kafka message header (JSON-serializable metadata)"""
    return {
        "device": device,
        "topic": KAFKA_TOPIC,
        "width": OUT_WIDTH,
        "height": OUT_HEIGHT,
        "quality": JPEG_QUALITY,
        "fps": PUBLISH_FPS,
    }


def encode_message(frame_bytes: bytes, ts_ns: int) -> bytes:
    """
    Binary message format:
      [8B timestamp_ns][4B payload_len][payload_bytes]
    헤더를 Kafka headers 필드에 넣지 않고 payload 앞에 붙여
    consumer에서 struct 언팩으로 빠르게 파싱.
    """
    payload_len = len(frame_bytes)
    header = struct.pack(">QI", ts_ns, payload_len)
    return header + frame_bytes


def main():
    log.info("Opening %s  cap=%dx%d@%d  out=%dx%d  jpeg_q=%d  pub=%.1ffps",
             CAM_DEVICE, CAP_WIDTH, CAP_HEIGHT, CAP_FPS,
             OUT_WIDTH, OUT_HEIGHT, JPEG_QUALITY, PUBLISH_FPS)

    cap = cv2.VideoCapture(CAM_DEVICE, cv2.CAP_V4L2)
    cap.set(cv2.CAP_PROP_FRAME_WIDTH,  CAP_WIDTH)
    cap.set(cv2.CAP_PROP_FRAME_HEIGHT, CAP_HEIGHT)
    cap.set(cv2.CAP_PROP_FPS,          CAP_FPS)

    if not cap.isOpened():
        raise RuntimeError(f"Cannot open {CAM_DEVICE}")

    producer = build_producer()
    header_meta = json.dumps(build_header(CAM_DEVICE)).encode()

    encode_params = [cv2.IMWRITE_JPEG_QUALITY, JPEG_QUALITY]
    next_publish = time.monotonic()

    log.info("Starting capture loop  topic=%s", KAFKA_TOPIC)

    try:
        while True:
            ret, frame = cap.read()
            if not ret:
                log.warning("Frame read failed – retrying")
                time.sleep(0.1)
                continue

            now = time.monotonic()
            if now < next_publish:
                continue   # 1fps throttle – 나머지 프레임 drop

            next_publish = now + PUBLISH_INTERVAL

            # resize + JPEG 인코딩
            resized = cv2.resize(frame, (OUT_WIDTH, OUT_HEIGHT),
                                 interpolation=cv2.INTER_AREA)
            ok, buf = cv2.imencode(".jpg", resized, encode_params)
            if not ok:
                log.warning("JPEG encode failed")
                continue

            ts_ns = time.time_ns()
            payload = encode_message(buf.tobytes(), ts_ns)

            try:
                producer.produce(
                    KAFKA_TOPIC,
                    value=payload,
                    headers={"meta": header_meta},
                    on_delivery=delivery_report,
                )
                producer.poll(0)
                log.info("Produced  topic=%s  size=%d B  ts=%d",
                         KAFKA_TOPIC, len(payload), ts_ns)
            except KafkaException as e:
                log.error("Produce error: %s", e)

    except KeyboardInterrupt:
        pass
    finally:
        log.info("Flushing producer…")
        producer.flush(timeout=10)
        cap.release()
        log.info("Shutdown complete.")


if __name__ == "__main__":
    main()
