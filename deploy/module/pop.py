import argparse
import pandas as pd
import json
import logging
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

# -----------------------------------------------------------
# 🪶 로깅 설정
# -----------------------------------------------------------
def setup_logger(script_name):
    log_dir = os.getenv("LOG_DIR", "./logs")  # 기본 로그 디렉토리
    log_file_path = os.path.join(log_dir, f"{script_name}.log")  # 파일명 동적 설정

    # 로그 디렉토리 생성
    if not os.path.exists(log_dir):
        os.makedirs(log_dir, exist_ok=True)

    logger = logging.getLogger(script_name)
    if not logger.handlers:  # 중복 방지
        logging.basicConfig(
            filename=log_file_path,
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)
        formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
        console.setFormatter(formatter)
        logger.addHandler(console)
        logger.info("📘 Logging initialized.")
    return logger

# ------------------------------------------------------------------------
# PostgreSQL 연결 생성
# ------------------------------------------------------------------------
def get_engine():
    db_config = {
        "DB_USER": os.getenv("DB_USER"),
        "DB_PASS": os.getenv("DB_PASS"),
        "DB_HOST": os.getenv("DB_HOST"),
        "DB_PORT": os.getenv("DB_PORT"),
        "DB_NAME": os.getenv("DB_NAME")
    }

    url = (
        f"postgresql+psycopg2://{db_config['DB_USER']}:{db_config['DB_PASS']}"
        f"@{db_config['DB_HOST']}:{db_config['DB_PORT']}/{db_config['DB_NAME']}"
    )
    return create_engine(url)