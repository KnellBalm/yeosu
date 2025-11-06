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

# ------------------------------------------------------------------------
# KCB 데이터 처리 및 적재
# ------------------------------------------------------------------------
def process_kcb(logger):
    logger.info("🚀 KCB 데이터 처리 시작")

    # Data Loading
    kcb = pd.read_csv('data/YEOSU_SOHO_STAT_2001-2507.txt', sep='|')
    ind_code = pd.read_csv('data/YEOSU_IND_CODE.txt', sep='|')

    # Cleaning & Merge

    kcb = pd.merge(kcb, ind_code, left_on='SIC_CD_LV4', right_on='SIC_CD', how='inner').drop(columns='SIC_CD')
    drop_cols = [
        'WGS84_X', 'WGS84_Y', 'UTMK_X', 'UTML_Y',
        'RUN_OUT2_CNT', 'TOT_SALES_AMT1_CNT', 'TOT_SALES_AMT2_CNT',
        'TOT_SALES_AMT3_CNT', 'TOT_SALES_AMT4_CNT'
    ]
    
    kcb.drop(columns=drop_cols, inplace=True)
    kcb = kcb[[
        'QID50', 'BS_YR_MON', 'SIC_CD_LV4','SIC_FST_CLSFY_ITM_NM', 'SIC_SCND_CLSFY_ITM_NM','SIC_TRD_CLSFY_ITM_NM', 'SIC_FOUR_CLSFY_ITM_NM',
        'SHOP_CNT', 'OP_CNT', 'NEW_OPN_CNT', 'RUN_OUT_CNT', 'TOT_SALE_AMT', 'TOT_SALES_AMT0_CNT',  'TOT_SALES_AMT5_CNT', ]]


    logger.info(f"KCB 데이터 정제 완료: {kcb.shape[0]} rows, {kcb.shape[1]} columns")

    # PostgreSQL Insert
    engine = get_engine()
    kcb.to_sql(name='tb_kcb_stat', con=engine, if_exists='append', index=False, chunksize=10000, method='multi')

    logger.info("✅ KCB 데이터 DB 적재 완료")

# ------------------------------------------------------------------------
# Local Pay 데이터 처리 및 적재
# ------------------------------------------------------------------------
def process_local(logger):
    logger.info("🚀 Local Pay 데이터 처리 시작")

    # Data Loading
    local_pay = pd.read_csv('data/local_pay_202501_09.csv')
    with open('data/json/local_grid_id.json', 'r', encoding='utf-8') as f:
        local_grid_id = json.load(f)

    # Cleaning
    local_pay['결제년월일'] = pd.to_datetime(local_pay['결제년월일'])
    local_pay['결제년월'] = local_pay['결제년월일'].dt.strftime('%Y-%m')
    local_pay['grid_id'] = local_pay['가맹점명'].map(local_grid_id)

    local_pay['std_ym'] = pd.to_datetime(local_pay['결제년월']).dt.strftime("%Y%m")
    local_pay_agg = local_pay.groupby(['업종', 'grid_id', 'std_ym'], as_index=False).agg(
        pay_cnt=('번호', 'count'),
        pay_amt=('결제금액', 'sum')
    )[['grid_id', 'std_ym', '업종', 'pay_cnt', 'pay_amt']]

    logger.info(f"Local Pay 집계 완료: {local_pay_agg.shape[0]} rows")

    # PostgreSQL Insert
    engine = get_engine()
    local_pay_agg.to_sql(name='tb_local_pay_agg', con=engine, if_exists='append', index=False, chunksize=10000, method='multi')

    logger.info("✅ Local Pay 데이터 DB 적재 완료")

# ------------------------------------------------------------------------
# Main Entry
# ------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="KCB / Local Pay 데이터 처리 및 DB 적재")
    parser.add_argument("--target", type=str, required=True, choices=["kcb", "local"], help="처리할 데이터 종류 선택")
    args = parser.parse_args()

    # 스크립트 이름에 따라 로거 설정
    logger = setup_logger("LocalEconomy")
    logger.info(f"▶ 실행 대상: {args.target.upper()}")

    try:
        if args.target == "kcb":
            process_kcb(logger)
        elif args.target == "local":
            process_local(logger)
    except Exception as e:
        logger.exception(f"❌ 실행 중 오류 발생: {e}")