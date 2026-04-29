# DAQ Kafka-MinIO Pipeline

file:///home/swm/%EB%8B%A4%EC%9A%B4%EB%A1%9C%EB%93%9C/dghan/daq-kafka/kafka_minio_pipeline_architecture.svg

Camera(V4L2) / IMU(BynavX1) → Confluent Kafka → MinIO S3

**Platform:** AP500L / NVIDIA Orin  
**Cameras:** `/dev/video0~2` — 1920×1080 20fps 캡처 → 1fps JPEG Q90 640×360 Kafka 발행  
**IMU/GNSS:** BynavX1 UDP `192.168.20.50:1111`

---

## 디렉토리 구조

```
daq-kafka/
├── docker-compose.yml
├── README.md
├── producers/
│   ├── camera/
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   └── cam_producer.py
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

| 컨테이너 | 이미지 | 포트 | 역할 |
|---|---|---|---|
| zookeeper | cp-zookeeper:7.6.1 | 2181 | Kafka 코디네이터 |
| kafka-broker | cp-kafka:7.6.1 | 9092 / 29092 | Confluent Kafka 브로커 |
| schema-registry | cp-schema-registry:7.6.1 | 8081 | Avro/JSON 스키마 관리 |
| control-center | cp-enterprise-control-center:7.6.1 | 9021 | Kafka Web UI |
| kafka-init | cp-kafka:7.6.1 | — | Topic 사전 생성 후 종료 |
| minio | minio/minio:latest | 9000 / 9001 | S3 Object Storage / Console |
| minio-init | minio/mc:latest | — | Bucket 사전 생성 후 종료 |
| cam0-producer | (build) | — | `/dev/video0` → `sensor.cam0.jpeg` |
| cam1-producer | (build) | — | `/dev/video1` → `sensor.cam1.jpeg` |
| cam2-producer | (build) | — | `/dev/video2` → `sensor.cam2.jpeg` |
| imu-producer | (build) | 1111/udp | BynavX1 UDP → `sensor.imu` / `sensor.gnss` |
| minio-consumer | (build) | — | 전체 토픽 → MinIO 저장 |

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

## Camera 메시지 바이너리 포맷

```
[8B  ts_ns     Big-Endian uint64]   ← 수집 시각 (nanoseconds)
[4B  length    Big-Endian uint32]   ← JPEG payload 길이
[N B JPEG bytes                 ]   ← 640×360 Q90 JPEG
```

## IMU/GNSS 메시지 포맷 (JSON)

```json
{
  "type": "imu_raw",          // imu_raw | ins_pvax | gnss_pos
  "gps_ts": 1234567890.123,
  "gps_week": 2315,
  "gps_ms": 345600000,
  "accel_x": 0.012, "accel_y": -0.003, "accel_z": 9.807,
  "gyro_x": 0.001,  "gyro_y": 0.002,   "gyro_z": -0.001,
  "ts_ns": 1714557330123456789
}
```

## MinIO 오브젝트 경로 패턴

```
{date}/{hour}/{timestamp}.{ext}

예) 2024-05-01/09/20240501T091530123456_1714557330123456789.jpg
    2024-05-01/09/20240501T091530123456_1714557330123456789.json
```

---

## 실행

### 실제 장비 (AP500L / Orin, 카메라 + IMU 연결된 경우)

```bash
cd daq-kafka
docker compose up -d --build
docker compose ps
```

### 개발 PC (카메라 `/dev/video*` 없는 경우)

카메라 디바이스가 없으면 cam-producer 3개가 시작 불가 (`no such file or directory`).  
인프라 + IMU + Consumer만 먼저 올릴 때:

```bash
docker compose up -d --build \
  --scale cam0-producer=0 \
  --scale cam1-producer=0 \
  --scale cam2-producer=0
```

IMU 장비 미연결 시에도 `imu-producer` 는 정상 기동됩니다. UDP 소켓을 열고 패킷 대기 상태로 idle 유지하며, 장비 연결 시 자동으로 수신을 시작합니다.

---

## 로그 확인

```bash
# 전체
docker compose logs -f

# 서비스별
docker compose logs -f cam0-producer
docker compose logs -f imu-producer
docker compose logs -f minio-consumer
docker compose logs -f kafka-init
```

---

## Web UI

| 서비스 | URL | 계정 |
|---|---|---|
| Confluent Control Center | http://localhost:9021 | — |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin123 |
| Schema Registry API | http://localhost:8081 | — |

---

## 유용한 운영 명령어

```bash
# Kafka topic 목록 확인
docker exec kafka-broker kafka-topics --bootstrap-server localhost:9092 --list

# topic 메시지 수 확인
docker exec kafka-broker kafka-run-class kafka.tools.GetOffsetShell \
  --bootstrap-server localhost:9092 --topic sensor.cam0.jpeg

# MinIO bucket 파일 목록
docker exec minio mc ls local/daq-cam0 --recursive

# 특정 컨테이너 재시작
docker compose restart minio-consumer

# 전체 중지 (데이터 볼륨 유지)
docker compose down

# 전체 중지 + 볼륨 삭제 (초기화)
docker compose down -v
```

---

## 네트워크 구성

```
호스트
 ├── daq-net (172.28.0.0/24)  ← 모든 컨테이너 내부 통신
 │    ├── zookeeper:2181
 │    ├── kafka-broker:9092
 │    ├── schema-registry:8081
 │    ├── minio:9000
 │    └── ...
 │
 ├── 9092/tcp  → kafka-broker   (컨테이너 → Kafka)
 ├── 29092/tcp → kafka-broker   (호스트 디버깅용)
 ├── 9000/tcp  → minio          (S3 API)
 ├── 9001/tcp  → minio          (Console)
 ├── 9021/tcp  → control-center
 ├── 8081/tcp  → schema-registry
 └── 1111/udp  → imu-producer   (BynavX1 패킷 수신)
```

> `imu-producer` 는 `network_mode: host` 없이 `ports: 1111:1111/udp` 로 호스트 UDP를 수신합니다.  
> `network_mode: host` 와 `networks:` 는 Docker Compose에서 동시 사용 불가합니다.

---

## 알려진 제약사항

| 항목 | 내용 |
|---|---|
| cam-producer | `/dev/video*` 디바이스가 호스트에 없으면 컨테이너 시작 불가 |
| group_add | `"44"` (Ubuntu video GID) 하드코딩 — 다른 OS에서는 `stat -c %g /dev/video0` 로 확인 후 변경 |
| BynavX1 MSG_ID | RAWIMUX=268, INSPVAXB=1465 — 펌웨어 버전에 따라 다를 수 있음 |
| Kafka replication | replication.factor=1 (단일 브로커) — 멀티 브로커 구성 시 조정 필요 |
| MinIO 인증 | minioadmin/minioadmin123 — 실 배포 시 반드시 변경 |
