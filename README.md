# DAQ Kafka-MinIO Pipeline

Camera(V4L2/Tegra) / IMU(BynavX1) → Confluent Kafka → MinIO S3

**Platform:** AP500L / NVIDIA Orin (aarch64, Tegra ISP 카메라)
**Cameras:** `/dev/video0~2` (gw5300, UYVY) — 1920×1080 20fps 캡처 → 1fps JPEG Q90 640×360 Kafka 발행
**IMU/GNSS:** BynavX1 UDP `192.168.20.50:1111`

---

## 아키텍처

```
┌─────────────────────────────────────────────────────────────┐
│  HOST (Tegra Orin)                                          │
│                                                             │
│  start_cameras.sh                                           │
│  ┌──────────┐   ┌──────────┐   ┌─────────────────────┐      │
│  │gst-launch│──▶│  framer  │──▶│  cam_producer.py  │      │
│  │/dev/video│   │(4B hdr)  │   │  (confluent-kafka)  │──┐  │
│  └──────────┘   └──────────┘   └─────────────────────┘  │  │
│                                                         │  │
│  ┌──────────────────────────────────────────────────┐   │  │
│  │  Docker Compose (daq-net)                        │   │  │
│  │                                                  │   │  │
│  │  imu-producer ──────────────────────────────┐    │   │  │
│  │  (BynavX1 UDP:1111)                         │    │   │  │
│  │                                             ▼    ▼   │  │
│  │  zookeeper ── kafka-broker ── schema-registry        │  │
│  │                   │                                  │  │
│  │                   ▼                                  │  │
│  │  minio-consumer ──────────────▶ MinIO               │  │
│  │                                (9000/9001)           │  │
│  │  control-center (9021)                               │  │
│  └──────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

> **카메라 수집은 호스트에서 직접 실행합니다.**
> gw5300은 Tegra ISP 전용 카메라라 Docker 컨테이너 안에서 GStreamer를 사용할 수 없습니다.
> `start_cameras.sh` 가 호스트 GStreamer로 캡처하여 직접 Kafka로 produce합니다.

---

## 디렉토리 구조

```
daq-kafka/
├── docker-compose.yml       ← 인프라 + IMU + MinIO Consumer
├── start_cameras.sh         ← 호스트에서 직접 실행 (카메라 → Kafka)
├── README.md
├── logs/                    ← start_cameras.sh 실행 로그 (자동 생성)
│   ├── cam0.log
│   ├── cam1.log
│   └── cam2.log
├── producers/
│   ├── camera/
│   │   ├── Dockerfile       ← 현재 미사용 (호스트 직접 실행)
│   │   ├── requirements.txt
│   │   └── cam_producer.py  ← start_cameras.sh 에서 호출
│   └── imu/
│       ├── Dockerfile
│       ├── requirements.txt
│       └── imu_producer.py
└── consumers/
    └── minio/
        ├── Dockerfile
        ├── requirements.txt
        └── minio_consumer.py
```

---

## 서비스 목록

### Docker Compose 서비스

| 컨테이너 | 이미지 | 포트 | 역할 |
|---|---|---|---|
| zookeeper | cp-zookeeper:7.6.1 | 2181 | Kafka 코디네이터 |
| kafka-broker | cp-kafka:7.6.1 | 9092 / 29092 | Confluent Kafka 브로커 |
| schema-registry | cp-schema-registry:7.6.1 | 8081 | 스키마 관리 |
| control-center | cp-enterprise-control-center:7.6.1 | 9021 | Kafka Web UI |
| kafka-init | cp-kafka:7.6.1 | — | Topic 사전 생성 후 종료 |
| minio | minio/minio:latest | 9000 / 9001 | Object Storage / Console |
| minio-init | minio/mc:latest | — | Bucket 생성 후 종료 |
| imu-producer | (build) | 1111/udp | BynavX1 UDP → `sensor.imu` / `sensor.gnss` |
| minio-consumer | (build) | — | 전체 토픽 → MinIO 저장 |

### 호스트 프로세스

| 프로세스 | 실행 방식 | 역할 |
|---|---|---|
| start_cameras.sh | `./start_cameras.sh` | gst-launch + cam_producer.py × 3 |
| cam_producer.py | start_cameras.sh 내부에서 호출 | JPEG stdin → `sensor.camN.jpeg` |

---

## Kafka Topic 설계

| Topic | 데이터 타입 | MinIO Bucket | 보존 |
|---|---|---|---|
| sensor.cam0.jpeg | Binary (JPEG) | daq-cam0 | 6시간 / 10GB |
| sensor.cam1.jpeg | Binary (JPEG) | daq-cam1 | 6시간 / 10GB |
| sensor.cam2.jpeg | Binary (JPEG) | daq-cam2 | 6시간 / 10GB |
| sensor.imu | JSON | daq-imu | 6시간 / 10GB |
| sensor.gnss | JSON | daq-gnss | 6시간 / 10GB |

---

## 메시지 포맷

### Camera (Binary)

```
[8B  ts_ns     Big-Endian uint64]   ← 수집 시각 (nanoseconds)
[4B  length    Big-Endian uint32]   ← JPEG payload 길이
[N B JPEG bytes                 ]   ← 640×360 Q90 JPEG
```

### IMU/GNSS (JSON)

```json
{
  "type": "imu_raw",
  "gps_ts": 1234567890.123,
  "gps_week": 2315,
  "gps_ms": 345600000,
  "accel_x": 0.012, "accel_y": -0.003, "accel_z": 9.807,
  "gyro_x": 0.001,  "gyro_y": 0.002,   "gyro_z": -0.001,
  "ts_ns": 1714557330123456789
}
```

### MinIO 오브젝트 경로

```
{bucket}/{date}/{hour}/{timestamp_ns}.{ext}

예) daq-cam0/2026-04-29/23/20260429T232757014532_1777505277014531634.jpg
    daq-imu/2026-04-29/23/20260429T232757014532_1777505277014531634.json
```

---

## 실행

### 사전 요구사항

```bash
# 호스트에 Python confluent-kafka 설치 (카메라 produce용)
curl https://bootstrap.pypa.io/pip/3.8/get-pip.py | python3
python3 -m pip install confluent-kafka
```

### 1. 인프라 + IMU + Consumer 시작

```bash
cd daq-kafka
docker compose up -d
docker compose ps
```

### 2. 카메라 수집 시작 (호스트에서 직접 실행)

```bash
# 포그라운드 실행 (로그 직접 확인)
./start_cameras.sh

# 백그라운드 실행
nohup ./start_cameras.sh &
echo "PID: $!"

# 백그라운드 실행 후 로그 확인
tail -f logs/cam0.log
tail -f logs/cam1.log
tail -f logs/cam2.log
```

### 카메라 수집 중지

```bash
# PID로 종료
kill <PID>

# 또는
pkill -f start_cameras.sh
```

### 개발 PC (카메라 없는 경우)

```bash
# cam-producer 관련 서비스 없으므로 그냥 올리면 됨
docker compose up -d
# start_cameras.sh 는 /dev/video* 없으면 자동 skip
```

---

## 로그 확인

```bash
# 카메라 파이프 로그
tail -f logs/cam0.log
tail -f logs/cam1.log
tail -f logs/cam2.log

# Docker 서비스 로그
docker compose logs -f minio-consumer
docker compose logs -f imu-producer
docker logs kafka-init
```

---

## Web UI

| 서비스 | URL (서버 IP 기준) | 계정 |
|---|---|---|
| Confluent Control Center | http://192.168.10.141:9021 | — |
| MinIO Console | http://192.168.10.141:9001 | minioadmin / minioadmin123 |
| Schema Registry API | http://192.168.10.141:8081 | — |

> ⚠️ 원격 서버 접속 시 `localhost` 대신 **서버 IP**(`192.168.10.141`)로 접속해야 합니다.
> MinIO Object Browser에서 버킷 클릭 후 날짜 폴더(`2026-04-30/`)를 클릭하면 파일 목록이 보입니다.

---

## 운영 명령어

```bash
# Kafka topic 목록
docker exec kafka-broker kafka-topics --bootstrap-server localhost:9092 --list

# topic 메시지 수 확인
docker exec kafka-broker kafka-run-class kafka.tools.GetOffsetShell \
  --bootstrap-server localhost:9092 --topic sensor.cam0.jpeg

# MinIO 파일 목록 (CLI)
docker exec minio mc ls local/daq-cam0 --recursive | head -20
docker exec minio mc ls local/daq-imu  --recursive | head -20

# 특정 컨테이너 재시작
docker compose restart minio-consumer
docker compose restart imu-producer

# 전체 중지 (데이터 볼륨 유지)
docker compose down

# 전체 초기화 (볼륨 포함 삭제)
docker compose down -v
```

---

## 네트워크 구성

```
호스트 (192.168.10.141)
 ├── daq-net (172.28.0.0/24)  ← Docker 내부 통신
 │    ├── zookeeper:2181
 │    ├── kafka-broker:9092
 │    ├── schema-registry:8081
 │    ├── minio:9000
 │    └── control-center:9021
 │
 ├── 9092/tcp  → kafka-broker        (내부용)
 ├── 29092/tcp → kafka-broker        (호스트 → Kafka, start_cameras.sh 사용)
 ├── 9000/tcp  → minio               (S3 API)
 ├── 9001/tcp  → minio               (Console UI)
 ├── 9021/tcp  → control-center      (Kafka UI)
 ├── 8081/tcp  → schema-registry
 └── 1111/udp  → imu-producer        (BynavX1 패킷 수신)
```

---

## 알려진 제약사항 및 트러블슈팅

| 항목 | 내용 | 해결 |
|---|---|---|
| 카메라 컨테이너화 불가 | gw5300은 Tegra ISP 전용 — 컨테이너 내 GStreamer 플러그인 스캐너 충돌 | `start_cameras.sh` 호스트 직접 실행으로 우회 |
| MinIO UI 빈 화면 | 날짜 폴더 구조라 최상위가 비어 보임 | 버킷 클릭 후 날짜 폴더(`YYYY-MM-DD/`) 클릭 |
| localhost 접속 불가 | 원격 서버라 localhost는 본인 PC를 가리킴 | 서버 IP(`192.168.10.141`)로 접속 |
| BynavX1 MSG_ID | RAWIMUX=268, INSPVAXB=1465 — 펌웨어마다 다를 수 있음 | `imu_producer.py` 상수 확인 후 수정 |
| Kafka replication | replication.factor=1 (단일 브로커) | 멀티 브로커 구성 시 조정 필요 |
| MinIO 인증 | minioadmin/minioadmin123 | 실 배포 시 반드시 변경 |
| video GID | `"44"` 하드코딩 (Ubuntu 표준) | 다른 OS: `stat -c %g /dev/video0` 로 확인 |
