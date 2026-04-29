# DAQ Kafka-MinIO Pipeline

## 디렉토리 구조

```
.
├── docker-compose.yml
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

## 서비스 목록

| 서비스 | 포트 | 설명 |
|--------|------|------|
| zookeeper | 2181 | Kafka 코디네이터 |
| kafka-broker | 9092 / 29092 | Confluent Kafka |
| schema-registry | 8081 | Avro/JSON 스키마 관리 |
| control-center | 9021 | Kafka Web UI |
| minio | 9000 / 9001 | S3 Object Storage / Console |
| cam0-producer | - | /dev/video0 → sensor.cam0.jpeg |
| cam1-producer | - | /dev/video1 → sensor.cam1.jpeg |
| cam2-producer | - | /dev/video2 → sensor.cam2.jpeg |
| imu-producer | - | BynavX1 UDP:1111 → sensor.imu / sensor.gnss |
| minio-consumer | - | 전체 토픽 → MinIO |

## Kafka Topic 설계

| Topic | 데이터 타입 | Bucket |
|-------|------------|--------|
| sensor.cam0.jpeg | Binary (JPEG) | daq-cam0 |
| sensor.cam1.jpeg | Binary (JPEG) | daq-cam1 |
| sensor.cam2.jpeg | Binary (JPEG) | daq-cam2 |
| sensor.imu | JSON | daq-imu |
| sensor.gnss | JSON | daq-gnss |

## MinIO 오브젝트 경로 패턴

```
{date}/{hour}/{timestamp}.{ext}
예: 2024-05-01/09/20240501T091530123456_1714557330123456789.jpg
```

## 실행

```bash
# 전체 실행
docker compose up -d

# 로그 확인
docker compose logs -f cam0-producer
docker compose logs -f imu-producer
docker compose logs -f minio-consumer

# Kafka UI
http://localhost:9021

# MinIO Console
http://localhost:9001  (minioadmin / minioadmin123)

# 특정 서비스만 재시작
docker compose restart cam0-producer
```

## IMU/GNSS imu-producer 주의사항

imu-producer 는 `network_mode: host` 로 동작합니다.
BynavX1 이 192.168.20.50 에서 UDP:1111 로 전송하는 바이너리를
호스트 네트워크에서 직접 수신하기 때문입니다.

이 경우 Kafka 연결은 `localhost:29092` (PLAINTEXT_HOST listener) 를 사용합니다.

## Camera Producer 메시지 포맷

```
[8B  ts_ns  Big-Endian uint64]
[4B  len    Big-Endian uint32]
[N B JPEG bytes               ]
```

## 프로덕션 체크리스트

- [ ] MinIO 자격증명을 환경변수 또는 secrets 로 분리
- [ ] Kafka replication.factor 를 클러스터 구성에 맞게 조정
- [ ] 카메라 권한: 컨테이너 실행 유저가 `video` 그룹에 포함되어야 함
- [ ] BynavX1 MSG_ID 확인: 펌웨어 버전에 따라 RAWIMUX/INSPVAXB ID가 다를 수 있음
- [ ] MinIO retention policy 및 lifecycle rule 설정 권장
