# 베이스 이미지
FROM python:3.10-slim

# 환경 설정
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/module
ENV TZ=Asia/Seoul

# 작업 디렉토리 생성
WORKDIR /app

# 시스템 패키지 및 파이썬 의존성 설치
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

RUN pip install scikit-learn==1.5.1

RUN pip list | grep sci || true

# 애플리케이션 복사
COPY module /app/module
COPY logs /app/logs
COPY scripts /app/scripts
COPY data /app/data
COPY sql /app/sql
RUN chmod +x /app/scripts/*.sh

# 기본 명령 (서비스별로 docker-compose에서 덮어씀)
CMD ["sleep", "infinity"]
