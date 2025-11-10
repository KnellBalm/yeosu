import argparse
import pandas as pd
import json
import logging
import os
from datetime import datetime
from utils import setup_logger, get_engine_from_env


def process_normal(logger, engine):
    logger.info("🚀 일반 인구 데이터 처리 시작")
    # 데이터 처리 로직 구현
    # 예: CSV 파일 로드, 전처리, DB 적재 등
    logger.info("✅ 일반 인구 데이터 처리 완료")    

def process_inout(logger, engine):
    logger.info("🚀 전출입 인구 데이터 처리 시작")
    # 데이터 처리 로직 구현
    # 예: CSV 파일 로드, 전처리, DB 적재 등
    logger.info("✅ 전출입 인구 데이터 처리 완료")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="인구 데이터 일반/전출입 처리 및 DB 적재")
    parser.add_argument("--target", type=str, required=True, choices=["normal", "inout"], help="처리할 데이터 종류 선택")
    args = parser.parse_args()
    logger = setup_logger(f"Popluation-{args.target.upper()}")
