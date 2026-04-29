# ============================================================
#  producers/camera/cam_producer.py
#  stdin(JPEG stream) → Confluent Kafka
#
#  호스트의 gst-launch 가 JPEG 을 stdout 으로 출력하고
#  이 프로세스가 stdin 으로 읽어서 Kafka 로 전송.
#
#  프레임 구분: [4B Big-Endian uint32: JPEG 길이][JPEG bytes]
#
#  env: KAFKA_BOOTSTRAP, KAFKA_TOPIC, CAM_DEVICE,
#       OUT_WIDTH, OUT_HEIGHT, JPEG_QUALITY
# ============================================================

import os, sys, struct, time, json, logging
from confluent_kafka import Producer, KafkaException

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")
log = logging.getLogger("cam-producer")

KAFKA_BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP"]
KAFKA_TOPIC     = os.environ["KAFKA_TOPIC"]
CAM_DEVICE      = os.environ.get("CAM_DEVICE", "/dev/video0")
OUT_WIDTH       = int(os.environ.get("OUT_WIDTH",  "640"))
OUT_HEIGHT      = int(os.environ.get("OUT_HEIGHT", "360"))
JPEG_QUALITY    = int(os.environ.get("JPEG_QUALITY", "90"))

def delivery_report(err, msg):
    if err: log.error("Delivery failed: %s", err)
    else:   log.debug("Delivered offset=%d", msg.offset())

def build_producer():
    return Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "message.max.bytes": 5_242_880,
        "compression.type": "none",
        "linger.ms": 0, "acks": "1", "retries": 3,
    })

def encode_message(jpeg_bytes, ts_ns):
    return struct.pack(">QI", ts_ns, len(jpeg_bytes)) + jpeg_bytes

def read_exactly(stream, n):
    buf = b""
    while len(buf) < n:
        chunk = stream.read(n - len(buf))
        if not chunk: return None
        buf += chunk
    return buf

def main():
    log.info("cam-producer starting  topic=%s  device=%s", KAFKA_TOPIC, CAM_DEVICE)
    producer = build_producer()
    header_meta = json.dumps({
        "device": CAM_DEVICE, "topic": KAFKA_TOPIC,
        "width": OUT_WIDTH, "height": OUT_HEIGHT, "quality": JPEG_QUALITY,
    }).encode()

    stdin = sys.stdin.buffer
    pub_count = 0
    try:
        while True:
            hdr = read_exactly(stdin, 4)
            if hdr is None:
                log.warning("stdin EOF")
                break
            jpeg_len = struct.unpack(">I", hdr)[0]
            if jpeg_len == 0 or jpeg_len > 5_000_000:
                log.warning("Invalid frame length: %d", jpeg_len)
                continue
            jpeg_bytes = read_exactly(stdin, jpeg_len)
            if jpeg_bytes is None:
                log.warning("stdin EOF while reading frame")
                break
            ts_ns   = time.time_ns()
            payload = encode_message(jpeg_bytes, ts_ns)
            try:
                producer.produce(KAFKA_TOPIC, value=payload,
                    headers={"meta": header_meta}, on_delivery=delivery_report)
                producer.poll(0)
                pub_count += 1
                log.info("Produced topic=%s size=%d B count=%d",
                         KAFKA_TOPIC, len(payload), pub_count)
            except KafkaException as e:
                log.error("Produce error: %s", e)
    except KeyboardInterrupt:
        pass
    finally:
        log.info("Flushing… total=%d", pub_count)
        producer.flush(timeout=10)
        log.info("Done.")

if __name__ == "__main__":
    main()
