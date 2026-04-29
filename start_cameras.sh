#!/bin/bash
# ============================================================
#  start_cameras.sh
#  호스트 GStreamer → Python framer → docker exec stdin pipe
#  - 파이프 끊기면 자동 재연결 (무한 retry)
#  - Kafka offset 은 consumer group 이 관리하므로 재연결해도 유실 없음
# ============================================================

DEVICES=("/dev/video0" "/dev/video1" "/dev/video2")
CONTAINERS=("cam0-producer" "cam1-producer" "cam2-producer")

CAP_W=1920
CAP_H=1080
CAP_FPS=20
OUT_W=640
OUT_H=360
JPEG_Q=90
PUBLISH_FPS=1

FRAMER_PY='
import sys, struct

stdin  = sys.stdin.buffer
stdout = sys.stdout.buffer

while True:
    # JPEG SOI (0xFF 0xD8) 탐색
    byte = stdin.read(1)
    if not byte:
        break
    if byte != b"\xff":
        continue
    next_b = stdin.read(1)
    if not next_b:
        break
    if next_b != b"\xd8":
        continue

    # SOI 발견 → EOI(0xFF 0xD9) 까지 누적
    frame = bytearray(b"\xff\xd8")
    while True:
        chunk = stdin.read(8192)
        if not chunk:
            break
        frame.extend(chunk)
        if b"\xff\xd9" in frame:
            idx = frame.rfind(b"\xff\xd9") + 2
            frame = frame[:idx]
            break

    if len(frame) < 100:
        continue

    # 4B 길이 헤더 + JPEG
    stdout.write(struct.pack(">I", len(frame)))
    stdout.write(frame)
    stdout.flush()
'

run_pipe() {
    local IDX=$1
    local DEV="${DEVICES[$IDX]}"
    local CTR="${CONTAINERS[$IDX]}"

    while true; do
        # 컨테이너 실행 중인지 확인
        STATUS=$(docker inspect -f '{{.State.Status}}' "$CTR" 2>/dev/null)
        if [ "$STATUS" != "running" ]; then
            echo "[$CTR] container not running (status=$STATUS), waiting 3s..."
            sleep 3
            continue
        fi

        echo "[$CTR] connecting pipe $DEV → $CTR"

        # GStreamer → framer → docker exec stdin
        gst-launch-1.0 -q \
            v4l2src device="$DEV" ! \
            "video/x-raw,format=UYVY,width=${CAP_W},height=${CAP_H},framerate=${CAP_FPS}/1" ! \
            videorate ! "video/x-raw,framerate=${PUBLISH_FPS}/1" ! \
            videoscale ! "video/x-raw,width=${OUT_W},height=${OUT_H}" ! \
            videoconvert ! \
            jpegenc quality="${JPEG_Q}" ! \
            fdsink fd=1 \
        | python3 -c "$FRAMER_PY" \
        | docker exec -i "$CTR" python -u cam_producer.py

        EXIT=$?
        echo "[$CTR] pipe exited (code=$EXIT), reconnecting in 2s..."
        sleep 2
    done
}

cleanup() {
    echo ""
    echo "Stopping all camera pipes..."
    kill 0
    wait
    echo "Done."
}
trap cleanup SIGINT SIGTERM

echo "Starting camera pipes..."
for i in 0 1 2; do
    if [ ! -e "${DEVICES[$i]}" ]; then
        echo "[${CONTAINERS[$i]}] ${DEVICES[$i]} not found, skipping"
        continue
    fi
    run_pipe $i &
done

echo "Camera pipes running. Ctrl+C to stop."
wait
