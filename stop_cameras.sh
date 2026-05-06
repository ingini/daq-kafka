#!/bin/bash
# ============================================================
#  stop_cameras.sh
#  start_cameras.sh 로 실행된 카메라 파이프 전체 종료
# ============================================================

echo "=== DAQ Camera Pipeline Stop ==="

# start_cameras.sh 프로세스 종료
PIDS=$(pgrep -f "start_cameras.sh" 2>/dev/null)
if [ -n "$PIDS" ]; then
    echo "Stopping start_cameras.sh (PIDs: $PIDS)..."
    kill $PIDS 2>/dev/null
else
    echo "start_cameras.sh not running"
fi

# 하위 프로세스 정리 (gst-launch, framer, cam_producer)
sleep 1
pkill -f "gst-launch-1.0.*v4l2src" 2>/dev/null && echo "Stopped gst-launch"
pkill -f "python3.*FRAMER_PY"       2>/dev/null
pkill -f "python3.*PRODUCER_PY"     2>/dev/null
pkill -f "python3.*KAFKA_TOPIC=sensor.cam" 2>/dev/null && echo "Stopped cam_producer"

sleep 1

# /dev/video* 점유 프로세스 확인 및 강제 종료
for DEV in /dev/video0 /dev/video1 /dev/video2; do
    if [ -e "$DEV" ]; then
        FUSER=$(fuser "$DEV" 2>/dev/null)
        if [ -n "$FUSER" ]; then
            echo "Force releasing $DEV (PIDs: $FUSER)..."
            fuser -k "$DEV" 2>/dev/null
        fi
    fi
done

echo ""
echo "Camera pipes stopped."
echo "  Docker services (Kafka, MinIO, consumer) are still running."
echo "  To restart cameras: ./start_cameras.sh"
