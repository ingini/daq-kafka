# DAQ Kafka Pipeline

Camera(V4L2/Tegra) / IMU(NovAtel) → Confluent Kafka → Storage

**Platform:** AP500L / NVIDIA Orin (aarch64, Tegra ISP)
**Cameras:** `/dev/video0~2` (gw5300, UYVY) — 1920×1080 20fps → 1fps JPEG 640×360 Kafka 발행
**IMU/GNSS:** NovAtel PwrPak7D-E1 UDP `192.168.20.50:1111` → Parquet 저장

---

## 아키텍처

<img width="1358" height="1198" alt="image" src="https://github.com/user-attachments/assets/54c5c04c-3899-4e3d-ba06-0254f18cb994" />


> **카메라는 호스트에서 직접 실행합니다.**
> gw5300은 Tegra ISP 전용 카메라로 컨테이너 내 GStreamer 플러그인 스캐너와 충돌합니다.
> `start_cameras.sh` 가 호스트 GStreamer로 캡처 → Python이 직접 Kafka로 produce합니다.

---

## 디렉토리 구조

```
daq-kafka/
├── config.env               ← 모든 설정값 (IP, 포트, 해상도, 스토리지 등)
├── docker-compose.yml
├── start_cameras.sh         ← 호스트 직접 실행 (카메라 → Kafka)
├── stop_cameras.sh          ← 카메라 수집 중지
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
│       └── imu_producer.py  ← NovAtel OEM7 Binary 파서
└── consumers/
    └── minio/
        ├── Dockerfile
        ├── requirements.txt  ← pyarrow 포함
        └── minio_consumer.py ← Fallback 스토리지 (USB→MinIO→내부)
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
| minio-init | minio/mc:latest | — | `daq` 버킷 생성 후 종료 |
| imu-producer | (build) | 1111/udp | NovAtel UDP → `sensor.imu` / `sensor.gnss` |
| minio-consumer | (build) | — | 전체 토픽 → Fallback 스토리지 저장 |

### 호스트 프로세스

| 프로세스 | 실행 방식 | 역할 |
|---|---|---|
| start_cameras.sh | `./start_cameras.sh` | gst-launch + cam_producer.py × 3 |
| stop_cameras.sh | `./stop_cameras.sh` | 카메라 파이프 전체 종료 |

---

## 설정 관리 (config.env)

모든 설정은 `config.env` 한 파일에서 관리합니다.

```bash
# ── 차량 식별 ──────────────────────────────────────────────────
VEHICLE_ID=KOR-1234              # MinIO/Local 경로에 포함

# ── Kafka ─────────────────────────────────────────────────────
KAFKA_HOST=localhost
KAFKA_PORT=29092                 # 호스트 → Kafka

# ── 카메라 ────────────────────────────────────────────────────
CAP_WIDTH=1920  CAP_HEIGHT=1080  CAP_FPS=20
OUT_WIDTH=640   OUT_HEIGHT=360
JPEG_QUALITY=90
PUBLISH_FPS=1

# ── IMU ───────────────────────────────────────────────────────
IMU_SRC_IP=192.168.20.50
IMU_DST_PORT=1111
IMU_STORAGE_FORMAT=parquet       # parquet | json
IMU_PARQUET_BATCH=1000

# ── 스토리지 백엔드 ────────────────────────────────────────────
STORAGE_BACKEND=auto             # auto | minio | local
#   auto  : USB → MinIO → 내부 디스크 순서로 자동 Fallback
#   minio : MinIO 전용
#   local : USB → 내부 디스크 (MinIO 사용 안 함)

LOCAL_BASE_PATH=/media/$USER     # USB 마운트 루트 (USB 이름 자동 탐지)
LOCAL_FALLBACK_PATH=/data/DAQ    # USB 없을 때 차량 내부 디스크

# ── 로그 ──────────────────────────────────────────────────────
LOG_RETENTION_DAYS=3             # N일 경과 로그 자동 삭제
```

---

## 스토리지 Fallback 구조

```
minio-consumer 시작 시 순서대로 초기화 시도:

  ① USB  (/media/$USER/{usb_name})    ← 최우선
      ↓ 실패 (USB 없음 / 마운트 안됨)
  ② MinIO (http://minio:9000)         ← 2순위
      ↓ 실패 (서버 없음 / 네트워크 끊김)
  ③ 내부 디스크 (/data/DAQ)           ← 최후 fallback
      ↓ 실패
  ✗ RuntimeError — 컨테이너 종료 (수집 불가 명시)

수집 중 실시간 fallback:
  저장 중 현재 백엔드 실패 → 즉시 다음 순위 백엔드로 전환
  로그: [Fallback] switched to MinIO
```

| STORAGE_BACKEND | 동작 |
|---|---|
| `auto` | USB → MinIO → 내부 디스크 자동 fallback |
| `minio` | MinIO 전용, 실패 시 에러 |
| `local` | USB → 내부 디스크, MinIO 사용 안 함 |

---

## Kafka Topic 설계

| Topic | 데이터 타입 | 파일 포맷 | 보존 |
|---|---|---|---|
| sensor.cam0.jpeg | Binary (JPEG) | .jpg | 6시간 / 10GB |
| sensor.cam1.jpeg | Binary (JPEG) | .jpg | 6시간 / 10GB |
| sensor.cam2.jpeg | Binary (JPEG) | .jpg | 6시간 / 10GB |
| sensor.imu | JSON | .parquet | 6시간 / 10GB |
| sensor.gnss | JSON | .parquet | 6시간 / 10GB |

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

### 저장 경로

```
MinIO:
  daq/{year}/{month}/{day}/{hour}/{vehicle_id}/{sensor}/{timestamp}.{ext}
  예) daq/2026/04/30/09/KOR-1234/cam0/20260430T091530_1777505277.jpg
      daq/2026/04/30/09/KOR-1234/imu/20260430T091530_1777505277.parquet

USB / 내부 디스크:
  {base}/{usb_name}/{yyyymmdd}/{vehicle_id}/{sensor}/{timestamp}.{ext}
  예) /media/swm/USB_32G/20260430/KOR-1234/cam0/20260430T091530_1777505277.jpg
      /data/DAQ/20260430/KOR-1234/imu/20260430T091530_1777505277.parquet
```

---

## IMU Parquet 저장 이유

| 항목 | JSON | Parquet (snappy) |
|---|---|---|
| 1000 rows 크기 | ~180 KB | ~18 KB |
| 압축률 | 1x | **~10x** |
| pandas 쿼리 | `json.load` 후 처리 | `pd.read_parquet()` 직접 |
| DuckDB/Spark | 변환 필요 | 네이티브 지원 |

`pyarrow` 미설치 시 자동으로 JSON fallback합니다.
1000줄 미만으로 수집 종료해도 `finally` 블록에서 잔여 rows를 Parquet으로 저장합니다.

### JPEG 추가 압축이 불가한 이유

JPEG은 이미 DCT 주파수 압축이 적용된 바이너리입니다. gzip/zstd를 추가해도 1~3% 절감에 그칩니다.
용량을 줄이려면 `config.env` 의 `JPEG_QUALITY` 를 낮추는 것(예: 90→80)이 유일한 현실적 방법입니다.

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

### 2단계 — 카메라 수집 시작 / 중지

```bash
# 포그라운드
./start_cameras.sh

# 백그라운드
nohup ./start_cameras.sh &
echo "PID: $!"

# 중지
./stop_cameras.sh
```

### MinIO 버킷 이름 주의

MinIO S3 규격상 버킷 이름은 **소문자만** 허용됩니다.
`config.env` 의 `MINIO_BUCKET` 은 반드시 소문자로 설정하세요.

```bash
MINIO_BUCKET=daq   # O
MINIO_BUCKET=DAQ   # X  (InvalidBucketName 에러)
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

# MinIO 적재 확인
docker exec minio mc ls local/daq --recursive | head -20

# USB 저장 확인
ls /media/$USER/
ls /media/$USER/{usb_name}/20260430/KOR-1234/
```

> 로그는 `LOG_RETENTION_DAYS`(기본 3일) 경과 시 `start_cameras.sh` 실행 시점에 자동 삭제됩니다.
> minio-consumer는 60초마다 현재 스토리지 상태(백엔드, 남은 용량)를 로그에 출력합니다.

---

## Web UI

| 서비스 | URL (서버 IP 기준) | 계정 |
|---|---|---|
| Confluent Control Center | http://192.168.10.141:9021 | — |
| MinIO Console | http://192.168.10.141:9001 | minioadmin / minioadmin123 |
| Schema Registry API | http://192.168.10.141:8081 | — |

> ⚠️ 원격 서버 접속 시 `localhost` 가 아닌 **서버 IP** 로 접속해야 합니다.
> MinIO Object Browser: 버킷 클릭 → 연도 폴더(`2026/`) → 월 → 일 → 시 → 차량번호 → 센서 순으로 탐색합니다.

---

## 운영 명령어

```bash
# Kafka topic 목록
docker exec kafka-broker kafka-topics --bootstrap-server localhost:9092 --list

# topic offset(메시지 수) 확인
docker exec kafka-broker kafka-run-class kafka.tools.GetOffsetShell \
  --bootstrap-server localhost:9092 --topic sensor.cam0.jpeg

# consumer group lag 확인
docker exec kafka-broker kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group minio-consumer-group

# MinIO 파일 목록
docker exec minio mc ls local/daq --recursive | head -20

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
 └── 1111/udp  → imu-producer   (NovAtel 패킷 수신)
```

---

## 트러블슈팅

| 증상 | 원인 | 해결 |
|---|---|---|
| cam-producer `GStreamer plugin loader failed` | 컨테이너 내 Tegra GStreamer 충돌 | `start_cameras.sh` 호스트 직접 실행으로 해결됨 |
| cam-producer `select() timeout` | cv2.VideoCapture로 gw5300 접근 불가 | GStreamer → stdin pipe 방식으로 교체됨 |
| `UNKNOWN_TOPIC_OR_PART` | kafka-init 완료 전 consumer 기동 | 에러 무시 후 재시도 루프로 처리됨 |
| `InvalidBucketName` | MinIO 버킷 이름에 대문자 사용 | `MINIO_BUCKET=daq` 소문자로 변경 |
| MinIO UI 빈 화면 | 계층형 폴더 구조 — 최상위가 비어 보임 | `2026/` → `04/` → `30/` → ... 순으로 탐색 |
| `localhost` 접속 불가 | 원격 서버라 localhost는 본인 PC | 서버 IP(`192.168.10.141`)로 접속 |
| `docker attach` pipe 끊김 | attach는 재연결 불가 | 호스트 직접 실행 방식으로 전환 |
| `group_add: video` 오류 | python:slim 내부에 video 그룹 없음 | GID `"44"` 숫자로 지정 |
| cam-producer `numpy _ARRAY_API` | OpenCV가 NumPy 1.x 기준 컴파일 | `requirements.txt` 에 `numpy<2` 핀 추가 |
| NovAtel 파싱 실패 | Sync byte 불일치 | `imu_producer.py` `NOVATEL_SYNC2=0x12` 확인 |
| USB 미탐지 | `/media/$USER` 아래 디렉토리 없음 | `lsblk` 로 마운트 확인, `LOCAL_BASE_PATH` 조정 |
| 모든 스토리지 실패 | USB/MinIO/내부 디스크 모두 불가 | consumer 종료 — 원인 확인 후 재시작 |

---

## Camera capture 과정
<img width="1358" height="838" alt="image" src="https://github.com/user-attachments/assets/20037cc5-7b78-42f6-826e-625322a9e8c4" />

<img width="1358" height="598" alt="image" src="https://github.com/user-attachments/assets/e6a6d787-3278-4c9d-8e09-de76a9f64b53" />

<img width="1358" height="758" alt="image" src="https://github.com/user-attachments/assets/d074efd9-2fbe-4f27-99dd-7a2054937254" />

