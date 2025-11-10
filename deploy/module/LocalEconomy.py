import argparse
import pandas as pd
import json
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv
from datetime import datetime
import glob
from utils import setup_logger, get_engine_from_env

# =========================
# 📁 공통 경로 정의
# =========================
BASE_DIR = "/DATA/jupyter_WorkingDirectory/notebook/yeosu/deploy/data"
KCB_PATTERN = os.path.join(BASE_DIR, "YEOSU_SOHO_STAT_*.txt")
IND_PATTERN = os.path.join(BASE_DIR, "YEOSU_IND_CODE*.txt")
LOCAL_PAY_PATTERN = os.path.join(BASE_DIR, "local_pay_*.csv")
LOCAL_GRID_JSON = os.path.join(BASE_DIR, "json/local_grid_id.json")

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
        'SHOP_CNT', 'OP_CNT', 'NEW_OPN_CNT', 'RUN_OUT_CNT', 'TOT_SALE_AMT', 'TOT_SALES_AMT0_CNT',  'TOT_SALES_AMT5_CNT', ]]
    kcb.rename(columns={
        'QID50': 'grid_id',
        'BS_YR_MON': 'std_ym',
        'SIC_CD_LV4': 'sic_cd_lv4',
        'SIC_FST_CLSFY_ITM_NM': 'sic_fst_clsfy_itm_nm',
        'SIC_SCND_CLSFY_ITM_NM': 'sic_scnd_clsfy_itm_nm',
        'SHOP_CNT': 'shop_cnt',
        'OP_CNT': 'op_cnt',
        'NEW_OPN_CNT': 'new_opn_cnt',
        'RUN_OUT_CNT': 'run_out_cnt',
        'TOT_SALE_AMT': 'tot_sale_amt',
        'TOT_SALES_AMT0_CNT': 'tot_sales_amt0_cnt',
        'TOT_SALES_AMT5_CNT': 'tot_sales_amt5m_cnt'
    }, inplace=True)
    kcb["reg_dttm"] = datetime.now()
    logger.info(f"KCB 데이터 정제 완료: {kcb.shape[0]} rows, {kcb.shape[1]} columns")
    engine = get_engine_from_env()
    kcb.to_sql(name='tb_kcb_stat', con=engine, if_exists='append', index=False, chunksize=10000, method='multi')
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
    local_pay_agg['reg_dttm'] = datetime.now()
    logger.info(f"Local Pay 집계 완료: {local_pay_agg.shape[0]} rows")
    engine = get_engine_from_env()
    local_pay_agg.to_sql(name='tb_local_pay_agg', con=engine, if_exists='append', index=False, chunksize=100000, method='multi')
    logger.info("✅ Local Pay 데이터 DB 적재 완료")

# ------------------------------------------------------------------------
# Main Entry
# ------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="KCB / Local Pay 데이터 처리 및 DB 적재")
    parser.add_argument("--target", type=str, required=True, choices=["kcb", "local"], help="처리할 데이터 종류 선택")
    args = parser.parse_args()
    logger = setup_logger(f"LocalEconomy-{args.target.upper()}")
    logger.info(f"▶ 실행 대상: {args.target.upper()}")
    try:
        if args.target == "kcb":
            process_kcb(logger)
        elif args.target == "local":
            process_local(logger)
    except Exception as e:
        logger.exception(f"❌ 실행 중 오류 발생: {e}")