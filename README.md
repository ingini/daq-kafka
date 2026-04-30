# DAQ Kafka-MinIO Pipeline

Camera(V4L2/Tegra) / IMU(BynavX1) → Confluent Kafka → MinIO S3

**Platform:** AP500L / NVIDIA Orin (aarch64, Tegra ISP)
**Cameras:** `/dev/video0~2` (gw5300, UYVY) — 1920×1080 20fps → 1fps JPEG 640×360 Kafka 발행
**IMU/GNSS:** BynavX1 UDP `192.168.20.50:1111` → Parquet 저장

---

## 아키텍처

<img width="1358" height="1158" alt="image" src="https://github.com/user-attachments/assets/359c6ada-9197-4db5-b666-bc8215b6b0c3" />


> **카메라는 호스트에서 직접 실행합니다.**
> gw5300은 Tegra ISP 전용 카메라로 Docker 컨테이너 내 GStreamer 플러그인 스캐너와 충돌합니다.
> `start_cameras.sh` 가 호스트 GStreamer로 캡처 → Python이 직접 Kafka로 produce합니다.

---

## 디렉토리 구조

```
daq-kafka/
├── config.env               ← 모든 설정값 (IP, 포트, 해상도 등)
├── docker-compose.yml
├── start_cameras.sh         ← 호스트 직접 실행 (카메라 → Kafka)
├── README.md
├── logs/                    ← 자동 생성, 3일 경과 로그 자동 삭제
│   ├── cam0.log
│   ├── cam1.log
│   └── cam2.log
├── producers/
│   ├── camera/
│   │   ├── Dockerfile       ← 현재 미사용 (호스트 직접 실행)
│   │   ├── requirements.txt
│   │   └── cam_producer.py  ← start_cameras.sh 내부에서 호출
│   └── imu/
│       ├── Dockerfile
│       ├── requirements.txt
│       └── imu_producer.py
└── consumers/
    └── minio/
        ├── Dockerfile
        ├── requirements.txt  ← pyarrow 포함
        └── minio_consumer.py ← JPEG→jpg / IMU→parquet
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
| cam_producer.py | start_cameras.sh 내부 호출 | JPEG stdin → `sensor.camN.jpeg` |

---

## 설정 관리 (config.env)

모든 설정은 `config.env` 한 파일에서 관리합니다.
`start_cameras.sh` 는 실행 시 자동으로 `source config.env` 합니다.

```bash
# 주요 설정 항목
KAFKA_HOST=localhost
KAFKA_PORT=29092              # 호스트 → Kafka (start_cameras.sh)

IMU_SRC_IP=192.168.20.50
IMU_DST_PORT=1111

CAP_WIDTH=1920   CAP_HEIGHT=1080   CAP_FPS=20
OUT_WIDTH=640    OUT_HEIGHT=360
JPEG_QUALITY=90
PUBLISH_FPS=1

IMU_STORAGE_FORMAT=parquet    # parquet | json
IMU_PARQUET_BATCH=1000        # N rows 누적 후 1 Parquet 파일

LOG_RETENTION_DAYS=3          # 3일 경과 로그 자동 삭제
```

---

## Kafka Topic 설계

| Topic | 데이터 타입 | MinIO Bucket | 파일 포맷 | 보존 |
|---|---|---|---|---|
| sensor.cam0.jpeg | Binary (JPEG) | daq-cam0 | .jpg | 6시간 / 10GB |
| sensor.cam1.jpeg | Binary (JPEG) | daq-cam1 | .jpg | 6시간 / 10GB |
| sensor.cam2.jpeg | Binary (JPEG) | daq-cam2 | .jpg | 6시간 / 10GB |
| sensor.imu | JSON | daq-imu | .parquet | 6시간 / 10GB |
| sensor.gnss | JSON | daq-gnss | .parquet | 6시간 / 10GB |

---

## 메시지 포맷

### Camera (Binary)

```
[8B  ts_ns    Big-Endian uint64]  ← 수집 시각 (nanoseconds)
[4B  length   Big-Endian uint32]  ← JPEG payload 길이
[N B JPEG bytes                ]  ← 640×360 Q90 JPEG
```

### IMU/GNSS (JSON → Parquet)

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

예) daq-cam0/2026-04-30/09/20260430T091530123456_1777505277014531634.jpg
    daq-imu/2026-04-30/09/20260430T091530000000_1777505277000000000.parquet
```

---

## IMU/GNSS Parquet 저장 이유

| 항목 | JSON | Parquet (snappy) |
|---|---|---|
| 1000 rows 크기 | ~180 KB | ~18 KB |
| 압축률 | 1x | **~10x** |
| pandas 쿼리 | `json.load` 후 처리 | `pd.read_parquet()` 직접 |
| DuckDB/Spark | 변환 필요 | 네이티브 지원 |

JSON은 키 이름이 매 row마다 반복되어 크기가 큽니다. Parquet은 컬럼 단위 저장 + snappy 압축으로 동일 데이터를 약 10배 작게 저장하며 분석 툴에서 직접 쿼리 가능합니다.
`pyarrow` 미설치 시 자동으로 JSON fallback합니다.


---

## 실행

### 사전 요구사항

```bash
# 호스트 Python confluent-kafka 설치 (최초 1회)
curl https://bootstrap.pypa.io/pip/3.8/get-pip.py | python3
python3 -m pip install confluent-kafka
```

### 1단계 — 인프라 + IMU + Consumer 시작

```bash
cd daq-kafka

# 최초 실행 또는 코드 변경 시
docker compose up -d --build

# 이후 재시작
docker compose up -d

docker compose ps
```

### 2단계 — 카메라 수집 시작

```bash
# 포그라운드 (로그 직접 확인)
./start_cameras.sh

# 백그라운드
nohup ./start_cameras.sh &
echo "PID: $!"
```

### 카메라 수집 중지

```bash
pkill -f start_cameras.sh
# 또는
kill <PID>
```

### 개발 PC (카메라 없는 경우)

```bash
# 인프라만 올리기 (cam 관련 컨테이너 없음)
docker compose up -d

# start_cameras.sh 는 /dev/video* 없으면 자동 skip
./start_cameras.sh
```

---

## 로그 확인

```bash
# 카메라 파이프 로그 (실시간)
tail -f logs/cam0.log
tail -f logs/cam1.log
tail -f logs/cam2.log

# Docker 서비스 로그
docker compose logs -f minio-consumer
docker compose logs -f imu-producer
docker logs kafka-init

# MinIO 적재 현황 확인
docker exec minio mc ls local/daq-cam0 --recursive | head -20
docker exec minio mc ls local/daq-imu  --recursive | head -20
```

> 로그는 `LOG_RETENTION_DAYS`(기본 3일) 경과 시 `start_cameras.sh` 실행 시점에 자동 삭제됩니다.

---

## Web UI

| 서비스 | URL | 계정 |
|---|---|---|
| Confluent Control Center | http://192.168.10.141:9021 | — |
| MinIO Console | http://192.168.10.141:9001 | minioadmin / minioadmin123 |
| Schema Registry API | http://192.168.10.141:8081 | — |

> ⚠️ 원격 서버 접속 시 `localhost` 가 아닌 **서버 IP** 로 접속해야 합니다.
> MinIO Object Browser에서 버킷 클릭 → 날짜 폴더(`2026-04-30/`) 클릭해야 파일 목록이 보입니다.

---

## 운영 명령어

```bash
# Kafka topic 목록
docker exec kafka-broker kafka-topics --bootstrap-server localhost:9092 --list

# topic offset(메시지 수) 확인
docker exec kafka-broker kafka-run-class kafka.tools.GetOffsetShell \
  --bootstrap-server localhost:9092 --topic sensor.cam0.jpeg

# MinIO 파일 목록
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
 ├── 29092/tcp → kafka-broker   (호스트 → Kafka, start_cameras.sh 사용)
 ├── 9092/tcp  → kafka-broker   (컨테이너 내부용)
 ├── 9000/tcp  → minio          (S3 API)
 ├── 9001/tcp  → minio          (Console UI)
 ├── 9021/tcp  → control-center (Kafka UI)
 ├── 8081/tcp  → schema-registry
 └── 1111/udp  → imu-producer   (BynavX1 패킷 수신)
```

---

## 트러블슈팅

| 증상 | 원인 | 해결 |
|---|---|---|
| cam-producer `GStreamer plugin loader failed` | 컨테이너 내 Tegra GStreamer 충돌 | `start_cameras.sh` 호스트 직접 실행으로 해결됨 |
| cam-producer `select() timeout` | cv2.VideoCapture로 gw5300 접근 불가 | GStreamer appsink → stdin pipe 방식으로 교체됨 |
| `UNKNOWN_TOPIC_OR_PART` | kafka-init 완료 전 consumer 기동 | 에러 무시 후 재시도 루프로 처리됨 |
| MinIO UI 빈 화면 | 날짜 폴더 구조 — 최상위가 비어 보임 | 버킷 클릭 후 날짜 폴더(`YYYY-MM-DD/`) 클릭 |
| `localhost` 접속 불가 | 원격 서버라 localhost는 본인 PC를 가리킴 | 서버 IP(`192.168.10.141`)로 접속 |
| `docker attach` pipe 끊김 재연결 불가 | attach는 PID 1 stdin에 붙어 재연결 불가 | `docker exec -i` → 호스트 직접 실행으로 전환 |
| `group_add: video` 컨테이너 오류 | python:slim 내부에 video 그룹 없음 | GID `"44"` 숫자로 지정 |
| cam-producer `numpy _ARRAY_API not found` | OpenCV가 NumPy 1.x 기준 컴파일 | `requirements.txt` 에 `numpy<2` 핀 추가 |
| BynavX1 MSG_ID 불일치 | 펌웨어 버전마다 ID 다름 | `imu_producer.py` 상수 확인 후 수정 |

---

## Camera capture 과정
<img width="1358" height="838" alt="image" src="https://github.com/user-attachments/assets/20037cc5-7b78-42f6-826e-625322a9e8c4" />

