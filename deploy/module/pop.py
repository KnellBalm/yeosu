import argparse
import pandas as pd
from datetime import datetime
from utils import setup_logger, get_engine_from_env

# =========================
# DB 연결 설정
engine = get_engine_from_env()

# -----------------------------------------------------------  
# 📥 일반 인구 데이터 처리 함수
def process_normal(logger, engine):
    logger.info("🚀 일반 인구 데이터 처리 시작")
    
    query = f"""
        SELECT *
        FROM public.tb_population_normal
        where etl_ymd >= '2023-01-01'
        """
    logger.debug(f"{query=}")
    df = pd.read_sql_query(query, engine)

    logger.info("✅ 일반 인구 데이터 처리 완료")    

# -----------------------------------------------------------
# 📥 전출입 인구 데이터 처리 함수
def process_inout(logger, engine):
    logger.info("🚀 전출입 인구 데이터 처리 시작")
    
    query = f"""
    SELECT *
    FROM public.tb_population_normal
    where etl_ymd >= '2023-01-01'
    """
    logger.debug(f"{query=}")

    df = pd.read_sql_query(query, engine)


    logger.info("✅ 전출입 인구 데이터 처리 완료")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="인구 데이터 일반/전출입 처리 및 DB 적재")
    parser.add_argument("--target", type=str, required=True, choices=["normal", "inout"], help="처리할 데이터 종류 선택")
    args = parser.parse_args()
    logger = setup_logger(f"Popluation-{args.target.upper()}")
