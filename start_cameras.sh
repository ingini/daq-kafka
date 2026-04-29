#!/bin/bash
# ============================================================
#  start_cameras.sh
#  호스트에서 직접 실행 (컨테이너 불필요)
#  GStreamer → Python framer+producer → Kafka
#  파이프 끊기면 자동 재연결
# ============================================================

DEVICES=("/dev/video0" "/dev/video1" "/dev/video2")
TOPICS=("sensor.cam0.jpeg" "sensor.cam1.jpeg" "sensor.cam2.jpeg")

KAFKA_BOOTSTRAP="localhost:29092"
CAP_W=1920
CAP_H=1080
CAP_FPS=20
OUT_W=640
OUT_H=360
JPEG_Q=90
PUBLISH_FPS=1

PRODUCER_PY='
import sys, struct, time, json, os
from confluent_kafka import Producer, KafkaException

topic   = os.environ["KAFKA_TOPIC"]
device  = os.environ.get("CAM_DEVICE", "unknown")
bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "localhost:29092")

import logging
logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [cam-producer] %(levelname)s: %(message)s")
log = logging.getLogger()

producer = Producer({
    "bootstrap.servers": bootstrap,
    "message.max.bytes": 5242880,
    "linger.ms": 0,
    "acks": "1",
})

def delivery_report(err, msg):
    if err: log.error("Delivery failed: %s", err)

stdin  = sys.stdin.buffer
count  = 0

log.info("Starting  topic=%s  device=%s", topic, device)

try:
    while True:
        hdr = stdin.read(4)
        if len(hdr) < 4:
            break
        jpeg_len = struct.unpack(">I", hdr)[0]
        if jpeg_len == 0 or jpeg_len > 5000000:
            log.warning("Bad frame len=%d, skip", jpeg_len)
            continue
        jpeg = stdin.read(jpeg_len)
        if len(jpeg) < jpeg_len:
            break
        ts_ns = time.time_ns()
        payload = struct.pack(">QI", ts_ns, len(jpeg)) + jpeg
        producer.produce(topic, value=payload, on_delivery=delivery_report)
        producer.poll(0)
        count += 1
        log.info("Produced topic=%s size=%d B count=%d", topic, len(payload), count)
except KeyboardInterrupt:
    pass
finally:
    producer.flush(10)
    log.info("Done. total=%d", count)
'

FRAMER_PY='
import sys, struct

stdin  = sys.stdin.buffer
stdout = sys.stdout.buffer

while True:
    byte = stdin.read(1)
    if not byte: break
    if byte != b"\xff": continue
    nb = stdin.read(1)
    if not nb: break
    if nb != b"\xd8": continue
    frame = bytearray(b"\xff\xd8")
    while True:
        chunk = stdin.read(8192)
        if not chunk: break
        frame.extend(chunk)
        if b"\xff\xd9" in frame:
            idx = frame.rfind(b"\xff\xd9") + 2
            frame = frame[:idx]
            break
    if len(frame) < 100: continue
    stdout.write(struct.pack(">I", len(frame)))
    stdout.write(bytes(frame))
    stdout.flush()
'

run_pipe() {
    local IDX=$1
    local DEV="${DEVICES[$IDX]}"
    local TOPIC="${TOPICS[$IDX]}"

    while true; do
        if [ ! -e "$DEV" ]; then
            echo "[$TOPIC] $DEV not found, waiting 3s..."
            sleep 3
            continue
        fi

        echo "[$TOPIC] starting pipe $DEV → Kafka"

        gst-launch-1.0 -q \
            v4l2src device="$DEV" ! \
            "video/x-raw,format=UYVY,width=${CAP_W},height=${CAP_H},framerate=${CAP_FPS}/1" ! \
            videorate ! "video/x-raw,framerate=${PUBLISH_FPS}/1" ! \
            videoscale ! "video/x-raw,width=${OUT_W},height=${OUT_H}" ! \
            videoconvert ! \
            jpegenc quality="${JPEG_Q}" ! \
            fdsink fd=1 2>/dev/null \
        | python3 -c "$FRAMER_PY" \
        | KAFKA_TOPIC="$TOPIC" KAFKA_BOOTSTRAP="$KAFKA_BOOTSTRAP" \
          CAM_DEVICE="$DEV" python3 -c "$PRODUCER_PY"

        echo "[$TOPIC] pipe exited, reconnecting in 2s..."
        sleep 2
    done
}

cleanup() {
    echo ""
    echo "Stopping camera pipes..."
    # 자식 프로세스 그룹만 종료
    jobs -p | xargs -r kill 2>/dev/null
    wait 2>/dev/null
    echo "Done."
    exit 0
}
trap cleanup SIGINT SIGTERM

mkdir -p logs
echo "Starting camera pipes..."

for i in 0 1 2; do
    if [ ! -e "${DEVICES[$i]}" ]; then
        echo "[${TOPICS[$i]}] ${DEVICES[$i]} not found, skipping"
        continue
    fi
    run_pipe $i >> "logs/cam${i}.log" 2>&1 &
done

echo "Camera pipes running in background."
echo "  Logs: tail -f logs/cam0.log"
echo "  Stop: kill \$\$ or Ctrl+C"
wait
