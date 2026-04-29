# ============================================================
#  producers/imu/imu_producer.py
#  BynavX1 UDP → Confluent Kafka
#
#  BynavX1 이 전송하는 Bynav 바이너리 프레임을 파싱해
#  IMU topic 과 GNSS topic 으로 분리 발행.
#
#  env:
#    KAFKA_BOOTSTRAP, KAFKA_TOPIC_IMU, KAFKA_TOPIC_GNSS
#    IMU_SRC_IP, IMU_DST_PORT, IMU_PROTOCOL
# ============================================================

import os
import json
import socket
import struct
import time
import logging
from confluent_kafka import Producer, KafkaException

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s"
)
log = logging.getLogger("imu-producer")

# ── 환경변수 ─────────────────────────────────────────────────
KAFKA_BOOTSTRAP  = os.environ["KAFKA_BOOTSTRAP"]
TOPIC_IMU        = os.environ.get("KAFKA_TOPIC_IMU",  "sensor.imu")
TOPIC_GNSS       = os.environ.get("KAFKA_TOPIC_GNSS", "sensor.gnss")
IMU_SRC_IP       = os.environ.get("IMU_SRC_IP",  "192.168.20.50")
IMU_DST_PORT     = int(os.environ.get("IMU_DST_PORT", "1111"))

# BynavX1 Binary Protocol 상수
BYNAV_SYNC0 = 0xAA
BYNAV_SYNC1 = 0x44
BYNAV_SYNC2 = 0x13
HEADER_LEN  = 28      # Short header 기준 (Bynav OEM binary)

# Message ID (Bynav 기준, 실제 장비 firmware 에 맞게 조정)
MSG_RAWIMUX   = 268   # Raw IMU data
MSG_INSPVAXB  = 1465  # INS Position Velocity Attitude extended
MSG_BESTGNSSPOS = 1429  # Best available GNSS position


def delivery_report(err, msg):
    if err:
        log.error("Delivery failed: %s", err)


def build_producer() -> Producer:
    return Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "linger.ms": 5,
        "acks": "1",
        "retries": 3,
    })


# ── Bynav Binary 파서 ─────────────────────────────────────
def parse_bynav_frame(data: bytes) -> dict | None:
    """
    Bynav OEM Short Binary Header (28 bytes) + payload 파싱.
    sync(3B) + header_len(1B) + msg_len(2B) + msg_id(2B) +
    msg_type(1B) + port_addr(1B) + seq(2B) + idle_time(1B) +
    time_status(1B) + week(2B) + ms(4B) + rx_status(4B) +
    reserved(2B) + sw_ver(2B)
    """
    if len(data) < HEADER_LEN:
        return None
    if data[0] != BYNAV_SYNC0 or data[1] != BYNAV_SYNC1 or data[2] != BYNAV_SYNC2:
        return None

    hdr_len, msg_len, msg_id = struct.unpack_from("<BBHH", data, 3)
    week, ms = struct.unpack_from("<HI", data, 14)

    payload = data[hdr_len: hdr_len + msg_len]
    gps_ts = week * 604800.0 + ms / 1000.0

    return {
        "msg_id": msg_id,
        "gps_week": week,
        "gps_ms": ms,
        "gps_ts": gps_ts,
        "payload": payload,
    }


def parse_rawimux(payload: bytes) -> dict:
    """RAWIMUX: imu_type(4B)+gnss_week(4B)+gnss_secs(8B)+
       z_accel(8B)+y_accel(8B)+x_accel(8B)+
       z_gyro(8B)+y_gyro(8B)+x_gyro(8B)"""
    if len(payload) < 52:
        return {}
    (imu_type, gnss_week, gnss_secs,
     az, ay, ax, gz, gy, gx) = struct.unpack_from("<IIdddddddd", payload)
    return {
        "type": "imu_raw",
        "imu_type": imu_type,
        "gnss_week": gnss_week,
        "gnss_secs": gnss_secs,
        "accel_x": ax, "accel_y": ay, "accel_z": az,
        "gyro_x": gx,  "gyro_y": gy,  "gyro_z": gz,
    }


def parse_inspvaxb(payload: bytes) -> dict:
    """INSPVAXB: ins_status(4B)+pos_type(4B)+lat(8B)+lon(8B)+
       height(8B)+undulation(4B)+vn(8B)+ve(8B)+vu(8B)+
       roll(8B)+pitch(8B)+azimuth(8B)+status_ext(4B)"""
    if len(payload) < 88:
        return {}
    (ins_status, pos_type, lat, lon, hgt, undulation,
     vn, ve, vu, roll, pitch, azimuth) = struct.unpack_from(
        "<IIdddddddddd", payload)
    return {
        "type": "ins_pvax",
        "ins_status": ins_status,
        "pos_type": pos_type,
        "latitude": lat, "longitude": lon, "height": hgt,
        "vel_north": vn, "vel_east": ve, "vel_up": vu,
        "roll": roll, "pitch": pitch, "azimuth": azimuth,
    }


def parse_bestgnsspos(payload: bytes) -> dict:
    """BESTGNSSPOS: sol_status(4B)+pos_type(4B)+lat(8B)+lon(8B)+
       height(8B)+undulation(4B)+datum(4B)+lat_std(4B)+lon_std(4B)+
       hgt_std(4B)+stn_id(4B)+diff_age(4B)+sol_age(4B)+
       num_svs(1B)+num_sv_used(1B)..."""
    if len(payload) < 44:
        return {}
    sol_status, pos_type, lat, lon, hgt, undulation = struct.unpack_from(
        "<IIdddf", payload)
    return {
        "type": "gnss_pos",
        "sol_status": sol_status,
        "pos_type": pos_type,
        "latitude": lat, "longitude": lon, "height": hgt,
    }


# ── 토픽 라우터 ──────────────────────────────────────────────
GNSS_MSG_IDS = {MSG_BESTGNSSPOS, MSG_INSPVAXB}

def route_message(frame: dict) -> str:
    return TOPIC_GNSS if frame["msg_id"] in GNSS_MSG_IDS else TOPIC_IMU


def parse_payload(frame: dict) -> dict:
    mid = frame["msg_id"]
    if mid == MSG_RAWIMUX:
        parsed = parse_rawimux(frame["payload"])
    elif mid == MSG_INSPVAXB:
        parsed = parse_inspvaxb(frame["payload"])
    elif mid == MSG_BESTGNSSPOS:
        parsed = parse_bestgnsspos(frame["payload"])
    else:
        parsed = {"type": "unknown", "raw_hex": frame["payload"].hex()[:64]}

    parsed["gps_ts"]   = frame["gps_ts"]
    parsed["gps_week"] = frame["gps_week"]
    parsed["gps_ms"]   = frame["gps_ms"]
    parsed["ts_ns"]    = time.time_ns()
    return parsed


def main():
    log.info("IMU producer starting  src=%s  port=%d", IMU_SRC_IP, IMU_DST_PORT)

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4 * 1024 * 1024)
    sock.bind(("0.0.0.0", IMU_DST_PORT))
    log.info("Listening UDP 0.0.0.0:%d", IMU_DST_PORT)

    producer = build_producer()
    rx_count = 0
    err_count = 0

    try:
        while True:
            raw, addr = sock.recvfrom(65535)

            # IP 필터 – 설정된 IMU_SRC_IP 만 수용
            if addr[0] != IMU_SRC_IP:
                continue

            frame = parse_bynav_frame(raw)
            if frame is None:
                err_count += 1
                if err_count % 100 == 0:
                    log.warning("Parse errors: %d", err_count)
                continue

            topic   = route_message(frame)
            payload = parse_payload(frame)
            msg_bytes = json.dumps(payload, ensure_ascii=False).encode()

            try:
                producer.produce(
                    topic,
                    value=msg_bytes,
                    key=str(payload["gps_ts"]).encode(),
                    on_delivery=delivery_report,
                )
                producer.poll(0)
                rx_count += 1
                if rx_count % 500 == 0:
                    log.info("Produced %d messages", rx_count)
            except KafkaException as e:
                log.error("Produce error: %s", e)

    except KeyboardInterrupt:
        pass
    finally:
        log.info("Flushing…  total produced=%d", rx_count)
        producer.flush(timeout=10)
        sock.close()
        log.info("Shutdown complete.")


if __name__ == "__main__":
    main()
