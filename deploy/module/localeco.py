import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import argparse
import pandas as pd
import json
import os
from datetime import datetime
import glob
from utils import setup_logger, get_engine_from_env, get_src_dir

# =========================
# 📁 공통 경로 정의
# =========================
BASE_DIR = get_base_dir()
DATA_DIR = get_src_dir()
KCB_PATTERN = os.path.join(DATA_DIR, "YEOSU_SOHO_STAT_*")
IND_PATTERN = os.path.join(DATA_DIR, "YEOSU_IND_CODE*")
LOCAL_PAY_PATTERN = os.path.join(DATA_DIR, "local_pay_*")
LOCAL_GRID_JSON = os.path.join(DATA_DIR, "json/local_grid_id.json")

# ------------------------------------------------------------------------
# KCB 데이터 처리 및 적재
# ------------------------------------------------------------------------
def process_kcb(logger):
    logger.info("🚀 KCB 데이터 처리 시작")
    kcb_files = sorted(glob.glob(KCB_PATTERN))
    ind_files = sorted(glob.glob(IND_PATTERN))
    if not kcb_files or not ind_files:
        logger.error("❌ KCB 또는 업종코드 파일을 찾을 수 없습니다.")
        return
    kcb_file = kcb_files[-1]
    ind_file = ind_files[-1]
    logger.info(f"KCB 파일: {kcb_file}, 업종코드 파일: {ind_file}")

    kcb = pd.read_csv(kcb_file, sep='|')
    ind_code = pd.read_csv(ind_file, sep='|')
    kcb = pd.merge(kcb, ind_code, left_on='SIC_CD_LV4', right_on='SIC_CD', how='inner').drop(columns='SIC_CD')
    drop_cols = [
        'WGS84_X', 'WGS84_Y', 'UTMK_X', 'UTML_Y',
        'RUN_OUT2_CNT', 'TOT_SALES_AMT1_CNT', 'TOT_SALES_AMT2_CNT',
        'TOT_SALES_AMT3_CNT', 'TOT_SALES_AMT4_CNT'
    ]
    kcb.drop(columns=drop_cols, inplace=True)
    kcb = kcb[[
        'QID50', 'BS_YR_MON', 'SIC_CD_LV4','SIC_FST_CLSFY_ITM_NM', 'SIC_SCND_CLSFY_ITM_NM',
        'SHOP_CNT', 'OP_CNT', 'NEW_OPN_CNT', 'RUN_OUT_CNT', 'TOT_SALE_AMT', 'TOT_SALES_AMT0_CNT', 'TOT_SALES_AMT5_CNT',]]
    kcb.columns = [col.lower() for col in kcb.columns]
    kcb.rename(columns={
        'qid50': 'grid_id',
        'bs_yr_mon': 'std_ym',
        'tot_sales_amt5_cnt': 'tot_sales_amt5m_cnt'
    }, inplace=True)
    kcb['grid_id'] = kcb['grid_id'].astype(str)
    kcb['std_ym'] = kcb['std_ym'].astype(str)
    kcb["reg_dttm"] = datetime.now()
    logger.info(f"KCB 데이터 정제 완료: {kcb.shape[0]} rows, {kcb.shape[1]} columns")
    engine = get_engine_from_env()
    kcb.to_sql(name='tb_kcb_stat', con=engine, if_exists='append', index=False, method='multi')
    logger.info("✅ KCB 데이터 DB 적재 완료")

# ------------------------------------------------------------------------
# Local Pay 데이터 처리 및 적재
# ------------------------------------------------------------------------
def process_local(logger):
    logger.info("🚀 Local Pay 데이터 처리 시작")
    pay_files = sorted(glob.glob(LOCAL_PAY_PATTERN))
    if not pay_files or not os.path.exists(LOCAL_GRID_JSON):
        logger.error("❌ Local Pay 파일 또는 grid_id 파일을 찾을 수 없습니다.")
        return
    pay_file = pay_files[-1]
    logger.info(f"Local Pay 파일: {pay_file}, grid_id 파일: {LOCAL_GRID_JSON}")

    local_pay = pd.read_csv(pay_file)
    with open(LOCAL_GRID_JSON, 'r', encoding='utf-8') as f:
        local_grid_id = json.load(f)
    local_pay['결제년월일'] = pd.to_datetime(local_pay['결제년월일'])
    local_pay['결제년월'] = local_pay['결제년월일'].dt.strftime('%Y-%m')
    local_pay['grid_id'] = local_pay['가맹점명'].map(local_grid_id)
    local_pay['std_ym'] = pd.to_datetime(local_pay['결제년월']).dt.strftime("%Y%m")
    local_pay_agg = local_pay.groupby(['업종', 'grid_id', 'std_ym'], as_index=False).agg(
        pay_cnt=('번호', 'count'),
        pay_amt=('결제금액', 'sum')
    )[['grid_id', 'std_ym', '업종', 'pay_cnt', 'pay_amt']]
    local_pay_agg.rename(columns={'업종': 'ind_type'}, inplace=True)
    local_pay_agg['reg_dttm'] = datetime.now()
    local_pay_agg['grid_id'] = local_pay_agg['grid_id'].astype(str)
    logger.info(f"Local Pay 집계 완료: {local_pay_agg.shape[0]} rows")
    engine = get_engine_from_env()
    local_pay_agg.to_sql(name='tb_local_pay_agg', con=engine, if_exists='append', index=False, method='multi')
    logger.info("✅ Local Pay 데이터 DB 적재 완료")

# ------------------------------------------------------------------------
# Local Pay 데이터 처리 및 적재 (원본 그대로)
# ------------------------------------------------------------------------
def process_local2(logger):
    logger.info("🚀 Local Pay 데이터 처리 시작")
    pay_files = sorted(glob.glob(LOCAL_PAY_PATTERN))

    if not pay_files or not os.path.exists(LOCAL_GRID_JSON):
        logger.error("❌ Local Pay 파일 또는 grid_id 파일을 찾을 수 없습니다.")
        return

    pay_file = pay_files[-1]
    logger.info(f"Local Pay 파일: {pay_file}, grid_id 파일: {LOCAL_GRID_JSON}")

    local_pay = pd.read_csv(pay_file)

    # grid JSON 로드
    with open(LOCAL_GRID_JSON, 'r', encoding='utf-8') as f:
        local_grid_id = json.load(f)

    # 날짜 변환
    local_pay['결제년월일'] = pd.to_datetime(local_pay['결제년월일'], format='%Y-%m-%d', errors='coerce')
    local_pay['생년월일'] = pd.to_datetime(local_pay['생년월일'], format='%Y%m%d', errors='coerce')

    # 기본 전처리
    #local_pay['결제년월'] = local_pay['결제년월일'].dt.strftime('%Y-%m')
    local_pay['grid_id'] = local_pay['가맹점명'].map(local_grid_id)
    #local_pay['std_ym'] = pd.to_datetime(local_pay['결제년월']).dt.strftime("%Y%m")

    # ---------------------------------------------------------
    # 🔥 만 나이 계산 (정확하고 안정적인 pandas 공식)
    # ---------------------------------------------------------
    pay = local_pay["결제년월일"]
    birth = local_pay["생년월일"]

    local_pay["나이"] = (pay.dt.year - birth.dt.year - ((pay.dt.month < birth.dt.month) | ((pay.dt.month == birth.dt.month) & (pay.dt.day < birth.dt.day))).astype(int)).astype("Int64")
    # ---------------------------------------------------------

    # 연령대 구간화
    bins = [10, 20, 30, 40, 50, 60, 70, 200]
    labels = ["10", "20", "30", "40", "50", "60", "70"]
    local_pay["연령대"] = pd.cut(local_pay["나이"], bins=bins, labels=labels)

    # 추가 필드
    local_pay['grid_id'] = local_pay['grid_id'].astype(str)
    #local_pay['reg_dttm'] = datetime.now()

    # 성별
    local_pay['성별'] = local_pay['성별'].map({'남': 'M', '여': 'F'})

    # 컬럼명 변경
    local_pay.rename(columns={
        '회원ID' : 'mem_id',
        '연령대' : 'gens',
        '결제년월일': 'pay_date',
        '결제금액': 'pay_amt',
        '성별': 'gender',
        '업종': 'ind_type',
    }, inplace=True)
    # 결제일자 날짜만 추출
    local_pay['pay_date'] = local_pay['pay_date'].dt.date

    # 제거할 컬럼
    local_pay.drop(columns=['번호',"거주지주소","가맹점주소","생년월일","가맹점명","나이"], inplace=True, errors='ignore')

    # DB 적재
    engine = get_engine_from_env()
    local_pay.to_sql(name='tb_local_pay_raw', con=engine, if_exists='append', index=False, method='multi')

    logger.info("✅ Local Pay 데이터 DB 적재 완료")

# ------------------------------------------------------------------------
# Main Entry
# ------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="KCB / Local Pay 데이터 처리 및 DB 적재")
    parser.add_argument("target", type=str, choices=["kcb", "local","all","local2"], help="처리할 데이터 종류 선택")
    args = parser.parse_args()
    logger = setup_logger(f"LocalEconomy-{args.target.upper()}")
    logger.info(f"▶ 실행 대상: {args.target.upper()}")
    try:
        if args.target == "kcb":
            process_kcb(logger)
        elif args.target == "local":
            process_local(logger)
        elif args.target == "all":
            process_kcb(logger)
            process_local(logger)
        elif args.target == "local2":
            process_local2(logger)
    except Exception as e:
        logger.exception(f"❌ 실행 중 오류 발생: {e}")