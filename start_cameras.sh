#!/bin/bash
# ============================================================
#  start_cameras.sh
#  호스트 GStreamer → cam_producer.py → Kafka
#  - config.env 에서 설정값 로드
#  - 파이프 끊기면 자동 재연결
#  - 로그 LOG_RETENTION_DAYS 일 경과 시 자동 삭제
# ============================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG="${SCRIPT_DIR}/config.env"

if [ ! -f "$CONFIG" ]; then
    echo "ERROR: config.env not found at $CONFIG"
    exit 1
fi

# config.env 로드
set -a
source "$CONFIG"
set +a

KAFKA_BOOTSTRAP="${KAFKA_HOST}:${KAFKA_PORT}"
DEVICES=("$CAM0_DEVICE" "$CAM1_DEVICE" "$CAM2_DEVICE")
TOPICS=("$TOPIC_CAM0" "$TOPIC_CAM1" "$TOPIC_CAM2")

mkdir -p "$LOG_DIR"

# ── 오래된 로그 자동 삭제 ──────────────────────────────────────
cleanup_logs() {
    if [ -n "$LOG_DIR" ] && [ -d "$LOG_DIR" ]; then
        find "$LOG_DIR" -name "cam*.log" -mtime "+${LOG_RETENTION_DAYS}" -delete
        echo "[log-cleanup] Deleted logs older than ${LOG_RETENTION_DAYS} days"
    fi
}
cleanup_logs

# ── Python cam_producer (inline) ──────────────────────────────
PRODUCER_PY='
import sys, struct, time, json, os, logging
from confluent_kafka import Producer, KafkaException

topic     = os.environ["KAFKA_TOPIC"]
device    = os.environ.get("CAM_DEVICE", "unknown")
bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "localhost:29092")
out_w     = int(os.environ.get("OUT_WIDTH",  "640"))
out_h     = int(os.environ.get("OUT_HEIGHT", "360"))
quality   = int(os.environ.get("JPEG_QUALITY", "90"))

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [cam-producer] %(levelname)s: %(message)s")
log = logging.getLogger()

producer = Producer({
    "bootstrap.servers": bootstrap,
    "message.max.bytes": 5242880,
    "linger.ms": 0, "acks": "1", "retries": 3,
})

def delivery_report(err, msg):
    if err: log.error("Delivery failed: %s", err)

stdin = sys.stdin.buffer
count = 0
log.info("Starting  topic=%s  device=%s  bootstrap=%s", topic, device, bootstrap)

try:
    while True:
        hdr = stdin.read(4)
        if len(hdr) < 4: break
        jpeg_len = struct.unpack(">I", hdr)[0]
        if jpeg_len == 0 or jpeg_len > 5000000:
            log.warning("Bad frame len=%d", jpeg_len)
            continue
        jpeg = stdin.read(jpeg_len)
        if len(jpeg) < jpeg_len: break
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

# ── JPEG framer (SOI/EOI 경계 분리 + 4B 헤더) ────────────────
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

# ── 카메라 파이프 루프 (자동 재연결) ─────────────────────────
run_pipe() {
    local IDX=$1
    local DEV="${DEVICES[$IDX]}"
    local TOPIC="${TOPICS[$IDX]}"
    local LOGFILE="${LOG_DIR}/cam${IDX}.log"

    while true; do
        if [ ! -e "$DEV" ]; then
            echo "[${TOPIC}] $DEV not found, waiting 5s..."
            sleep 5
            continue
        fi

        echo "[${TOPIC}] connecting  $DEV → Kafka(${KAFKA_BOOTSTRAP})"

        gst-launch-1.0 -q \
            v4l2src device="$DEV" ! \
            "video/x-raw,format=UYVY,width=${CAP_WIDTH},height=${CAP_HEIGHT},framerate=${CAP_FPS}/1" ! \
            videorate ! "video/x-raw,framerate=${PUBLISH_FPS}/1" ! \
            videoscale ! "video/x-raw,width=${OUT_WIDTH},height=${OUT_HEIGHT}" ! \
            videoconvert ! \
            jpegenc quality="${JPEG_QUALITY}" ! \
            fdsink fd=1 2>/dev/null \
        | python3 -c "$FRAMER_PY" \
        | KAFKA_TOPIC="$TOPIC" \
          KAFKA_BOOTSTRAP="$KAFKA_BOOTSTRAP" \
          CAM_DEVICE="$DEV" \
          OUT_WIDTH="$OUT_WIDTH" \
          OUT_HEIGHT="$OUT_HEIGHT" \
          JPEG_QUALITY="$JPEG_QUALITY" \
          python3 -c "$PRODUCER_PY"

        EXIT=$?
        echo "[${TOPIC}] pipe exited (code=$EXIT), reconnecting in 2s..."
        sleep 2
    done
}

cleanup() {
    echo ""
    echo "Stopping camera pipes..."
    jobs -p | xargs -r kill 2>/dev/null
    wait 2>/dev/null
    echo "Done."
    exit 0
}
trap cleanup SIGINT SIGTERM

echo "=== DAQ Camera Pipeline ==="
echo "  Kafka  : ${KAFKA_BOOTSTRAP}"
echo "  Config : ${CONFIG}"
echo "  Logs   : ${LOG_DIR}  (retention: ${LOG_RETENTION_DAYS} days)"
echo ""

for i in 0 1 2; do
    if [ ! -e "${DEVICES[$i]}" ]; then
        echo "[SKIP] ${DEVICES[$i]} not found"
        continue
    fi
    run_pipe $i >> "${LOG_DIR}/cam${i}.log" 2>&1 &
    echo "[START] ${TOPICS[$i]}  device=${DEVICES[$i]}  log=${LOG_DIR}/cam${i}.log"
done

echo ""
echo "Camera pipes running. Ctrl+C to stop."
echo "  tail -f ${LOG_DIR}/cam0.log"
wait
