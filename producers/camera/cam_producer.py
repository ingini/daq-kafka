# ============================================================
#  producers/camera/cam_producer.py
#  Tegra gw5300 (UYVY) → GStreamer → JPEG → Confluent Kafka
#
#  gw5300 은 V4L2 표준 캡처(cv2.VideoCapture)로는 프레임을
#  못 받음. GStreamer appsink 방식으로 교체.
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
import gi

gi.require_version("Gst", "1.0")
gi.require_version("GstApp", "1.0")
from gi.repository import Gst, GstApp, GLib

import numpy as np
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
CAM_DEVICE      = os.environ.get("CAM_DEVICE",    "/dev/video0")
CAP_WIDTH       = int(os.environ.get("CAP_WIDTH",  "1920"))
CAP_HEIGHT      = int(os.environ.get("CAP_HEIGHT", "1080"))
CAP_FPS         = int(os.environ.get("CAP_FPS",    "20"))
OUT_WIDTH       = int(os.environ.get("OUT_WIDTH",  "640"))
OUT_HEIGHT      = int(os.environ.get("OUT_HEIGHT", "360"))
JPEG_QUALITY    = int(os.environ.get("JPEG_QUALITY", "90"))
PUBLISH_FPS     = float(os.environ.get("PUBLISH_FPS", "1"))

PUBLISH_INTERVAL = 1.0 / PUBLISH_FPS


def delivery_report(err, msg):
    if err:
        log.error("Delivery failed: %s", err)
    else:
        log.debug("Delivered offset=%d", msg.offset())


def build_producer() -> Producer:
    return Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "message.max.bytes": 5_242_880,
        "compression.type": "none",
        "linger.ms": 0,
        "acks": "1",
        "retries": 3,
        "retry.backoff.ms": 500,
    })


def encode_message(frame_bytes: bytes, ts_ns: int) -> bytes:
    """[8B ts_ns][4B len][JPEG bytes]"""
    return struct.pack(">QI", ts_ns, len(frame_bytes)) + frame_bytes


def build_pipeline() -> str:
    """
    gw5300 UYVY 캡처 → BGRx 변환 → appsink
    resize 는 Python 에서 cv2 로 처리 (GPU 불필요)
    """
    return (
        f"v4l2src device={CAM_DEVICE} ! "
        f"video/x-raw,format=UYVY,width={CAP_WIDTH},height={CAP_HEIGHT},"
        f"framerate={CAP_FPS}/1 ! "
        f"videoconvert ! "
        f"video/x-raw,format=BGRx ! "
        f"appsink name=sink emit-signals=true max-buffers=2 drop=true sync=false"
    )


def uyvy_appsink_to_bgr(sample) -> np.ndarray | None:
    """GStreamer sample → OpenCV BGR ndarray"""
    buf = sample.get_buffer()
    caps = sample.get_caps()
    structure = caps.get_structure(0)
    w = structure.get_value("width")
    h = structure.get_value("height")

    ok, mapinfo = buf.map(Gst.MapFlags.READ)
    if not ok:
        return None
    try:
        # BGRx (4채널) → BGR (3채널)
        arr = np.frombuffer(mapinfo.data, dtype=np.uint8).reshape(h, w, 4)
        return arr[:, :, :3].copy()
    finally:
        buf.unmap(mapinfo)


def main():
    Gst.init(None)

    log.info("Opening %s  cap=%dx%d@%d  out=%dx%d  jpeg_q=%d  pub=%.1ffps",
             CAM_DEVICE, CAP_WIDTH, CAP_HEIGHT, CAP_FPS,
             OUT_WIDTH, OUT_HEIGHT, JPEG_QUALITY, PUBLISH_FPS)

    pipeline_str = build_pipeline()
    log.info("GStreamer pipeline: %s", pipeline_str)

    pipeline = Gst.parse_launch(pipeline_str)
    sink: GstApp.AppSink = pipeline.get_by_name("sink")

    pipeline.set_state(Gst.State.PLAYING)
    log.info("Pipeline PLAYING")

    producer   = build_producer()
    encode_params = [cv2.IMWRITE_JPEG_QUALITY, JPEG_QUALITY]
    header_meta   = json.dumps({
        "device": CAM_DEVICE, "topic": KAFKA_TOPIC,
        "width": OUT_WIDTH, "height": OUT_HEIGHT,
        "quality": JPEG_QUALITY, "fps": PUBLISH_FPS,
    }).encode()

    next_publish = time.monotonic()
    frame_count  = 0
    pub_count    = 0

    try:
        while True:
            # appsink 에서 샘플 pull (100ms timeout)
            sample = sink.try_pull_sample(100 * Gst.MSECOND)
            if sample is None:
                # timeout – 버스 에러 체크
                bus = pipeline.get_bus()
                msg = bus.timed_pop_filtered(
                    0, Gst.MessageType.ERROR | Gst.MessageType.EOS)
                if msg:
                    if msg.type == Gst.MessageType.ERROR:
                        err, dbg = msg.parse_error()
                        log.error("GStreamer error: %s / %s", err, dbg)
                    elif msg.type == Gst.MessageType.EOS:
                        log.warning("GStreamer EOS")
                    break
                continue

            frame_count += 1
            now = time.monotonic()
            if now < next_publish:
                continue   # 1fps throttle

            next_publish = now + PUBLISH_INTERVAL

            bgr = uyvy_appsink_to_bgr(sample)
            if bgr is None:
                log.warning("Frame decode failed")
                continue

            # resize + JPEG 인코딩
            resized = cv2.resize(bgr, (OUT_WIDTH, OUT_HEIGHT),
                                 interpolation=cv2.INTER_AREA)
            ok, buf = cv2.imencode(".jpg", resized, encode_params)
            if not ok:
                log.warning("JPEG encode failed")
                continue

            ts_ns   = time.time_ns()
            payload = encode_message(buf.tobytes(), ts_ns)

            try:
                producer.produce(
                    KAFKA_TOPIC,
                    value=payload,
                    headers={"meta": header_meta},
                    on_delivery=delivery_report,
                )
                producer.poll(0)
                pub_count += 1
                log.info("Produced  topic=%s  size=%d B  ts=%d  [frames=%d pub=%d]",
                         KAFKA_TOPIC, len(payload), ts_ns, frame_count, pub_count)
            except KafkaException as e:
                log.error("Produce error: %s", e)

    except KeyboardInterrupt:
        pass
    finally:
        log.info("Shutting down… frames=%d published=%d", frame_count, pub_count)
        pipeline.set_state(Gst.State.NULL)
        producer.flush(timeout=10)
        log.info("Done.")


if __name__ == "__main__":
    main()
