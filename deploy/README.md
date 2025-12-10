# Yeosu Deploy 모듈 컨테이너 실행 가이드

이 리포지토리는 `flowpop.py`, `LocalEconomy.py`, `pop.py`, `predict_model/wifi_predict.py` 네 가지 파이썬 스크립트를 Docker 컨테이너에서 실행하기 위한 설정을 제공합니다. 각 서비스는 공통 이미지를 사용하며, 실행 시 필요한 인자를 셸 스크립트로 래핑해 간편하게 실행할 수 있습니다.

## 1. 사전 준비

- Docker, Docker Compose v2 이상
- 원본 데이터 경로 및 로그 저장 경로 준비
- DB 접속 정보 및 스크립트별 인자 준비

## 2. 디렉터리 구조

```
deploy/
├─ Dockerfile
├─ docker-compose.yml          # 빌드용 (로컬 개발)
├─ docker-compose.deploy.yml   # 배포용 (이미 빌드된 이미지 사용)
├─ requirements.txt            # 파이썬 의존성
├─ scripts/
│  ├─ run_flowpop.sh           # flowpop.py(유동인구) 실행 script
│  ├─ run_localeconomy.sh      # localeco.py(지역경제) 실행 script
│  ├─ run_pop.sh               # pop.py(인구) 실행 script
│  └─ run_wifi_predict.sh      # wifi_predict.py(와이파이 예측) 실행 script
└─ module/
   ├─ flowpop.py
   ├─ localeco.py
   ├─ pop.py
   ├─ utils.py
   └─ predict_model/
      └─ wifi_predict.py
```

## 3. 환경 변수 설정

`docker-compose.yml`과 동일한 디렉터리에 `.env` 파일을 만들고 아래 항목을 채워주세요.

```
# 볼륨 마운트 경로 (호스트 기준)
DATA_DIR=/DATA/jupyter_WorkingDirectory/notebook/yeosu/deploy/data
LOG_DIR=/home/axdev3/deploy/logs

# DB 접속 정보
DB_HOST=your-db-host
DB_PORT=5432
DB_USER=your-db-user
DB_PASS=your-db-pass
DB_NAME=your-db-name
DATA_BASE_DIR=/data

# WIFI DB 접속 정보
WIFI_DB_HOST=your-wifi-db-host
WIFI_DB_PORT=5432
WIFI_DB_USER=your-wifi-db-user
WIFI_DB_PASS=your-wifi-db-pass
WIFI_DB_NAME=your-wifi-db-name

# 필요 시 아래 경로 환경변수로 재정의
# WIFI_MODEL_BASE_DIR=/app/module/predict_model
# WIFI_MODEL_BUNDLE_PATH=/app/module/predict_model/xgb_quantile_bundle.joblib
# WIFI_MODEL_METADATA_PATH=/app/module/predict_model/xgb_quantile_metadata.json
# WIFI_GRID_MAPPING_PATH=/data/json/wifi_grid_id.json

# 서비스별 실행 인자 (필요한 서비스만)
FLOWPOP_YM=202501        # flowpop 서비스용
LOCALECONOMY_TARGET=kcb  # localeconomy 서비스용 (kcb | local)
```

> ⚠️ `.env` 파일은 `docker-compose.yml`이 있는 `deploy/` 디렉터리에 위치해야 하며, 반드시 해당 디렉터리에서 Compose 명령을 실행해야 합니다.

## 4. 이미지 빌드

### 4.1 레지스트리 푸시용 빌드

레지스트리에 이미지를 빌드하고 푸시할 때 사용합니다.

#### default

```bash
cd /home/axdev3/deploy
docker buildx build --platform linux/amd64 -t repo.iris.tools/lodp/yeosu-analysis --push .
```

#### with tag

```bash
docker buildx build --platform linux/amd64 -t repo.iris.tools/lodp/yeosu-analysis:$(date +%Y%m%d-%H%M)  --push .
```

## 5. 서비스 실행

### 5.1 배포 이미지로 실행

이미 빌드되어 레지스트리에 푸시된 이미지(`repo.iris.tools/lodp/yeosu-analysis:latest`)를 사용하여 실행합니다.

```bash
# 배포(실행) - flowpop 예시
export FLOWPOP_YM=202501
docker compose -f docker-compose.deploy.yml run --rm flowpop

# LocalEconomy 스크립트 실행 (LOCALECONOMY_TARGET 필요)
export LOCALECONOMY_TARGET=kcb
docker compose -f docker-compose.deploy.yml run --rm localeconomy

# pop 스크립트 실행 (POP_TARGET 필요)
docker compose -f docker-compose.deploy.yml run --rm pop

# wifi 예측 스크립트 실행 (추가 인자 없음)
docker compose -f docker-compose.deploy.yml run --rm wifi_predict
```

각 컨테이너는 `/app/scripts/run_*.sh` 스크립트를 통해 내부에서 파이썬 코드를 실행합니다. 로그는 호스트의 `LOG_DIR` 환경변수로 지정된 경로에 저장됩니다.

## 6. 데이터 & 볼륨

### 볼륨 마운트

- `DATA_DIR` (환경변수): 호스트의 데이터 디렉터리 → 컨테이너 `/data` (읽기 전용)
  - 기본값 예시: `/DATA/jupyter_WorkingDirectory/notebook/yeosu/deploy/data`
- `LOG_DIR` (환경변수): 호스트의 로그 디렉터리 → 컨테이너 `/app/logs`
  - 기본값 예시: `/home/axdev3/deploy/logs`

### 환경 변수 설명

- `DATA_BASE_DIR` (기본 `/data`): `flowpop.py`, `LocalEconomy.py`, `wifi_predict.py`에서 데이터 루트를 가리킵니다. 컨테이너 내부 경로입니다.
- `wifi_predict.py`는 와이파이 DB 접속 정보(`WIFI_DB_*`)와 모델/메타데이터/격자 매핑 경로(`WIFI_MODEL_*`, `WIFI_GRID_MAPPING_PATH`)를 환경 변수로 읽습니다.
  4
  > 💡 **볼륨 경로 변경**: 데이터나 로그 경로가 다른 경우 `.env` 파일에서 `DATA_DIR`, `LOG_DIR` 값을 수정하면 됩니다.

### Dockerfile 애플리케이션 복사
`deploy/`를 기준으로 `module`, `logs`, `scripts`, `data/json`, `sql`을 복사합니다.
```
COPY module /app/module
COPY logs /app/logs
COPY scripts /app/scripts
COPY data/json /app/data/json
COPY sql /app/sql
```

## 7. 상용 배포 방법
### 1. 이미지 빌드 & 레지스트리 push
```bash 
docker buildx build --platform linux/amd64 -t repo.iris.tools/lodp/yeosu-analysis:${tag_name | latest} --push .
```
### 2. 상용에서 docker pull
```bash
docker pull repo.iris.tools/lodp/yeosu-analysis:${tag_name}
```

### 3. 원하는 서비스 실행
```bash
docker compose -f docker-compose.deploy.yml run --rm ${service_name}
```
#### service_name  
- `flowpop`: 유동인구, `FLOWPOP_YM` 변수 필요 ex) 202501
- `localeconomy`: 지역경제, `LOCALECONOMY_TARGET` 변수 필요 ex) all/kcb/local/local2
- `pop`: 인구
- `wifi_predict`: WIFI 예측


## 8. 자주 묻는 질문

### Q. `.env`에 값이 있는데도 "required variable ... missing" 오류가 뜹니다.

- Compose 명령을 실행하는 현재 위치(`pwd`)가 `deploy/`인지 확인하세요.
- 파일명이 `.env`인지 확인하세요 (`.env.txt` 등 다른 확장자가 있으면 안 됩니다).
- 터미널에서 `docker compose config` 명령으로 실제 적용되는 값을 확인할 수 있습니다.

### Q. 다른 인자를 넣고 싶어요.

- `scripts/run_*.sh` 파일을 수정해 원하는 플래그를 추가하거나 기본값을 바꿀 수 있습니다.

---

문의사항이나 추가 수정이 필요하면 알려주세요.
