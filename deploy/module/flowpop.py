import csv
from io import StringIO
import argparse
import sys
import datetime
import os
import tempfile
import glob
from utils import setup_logger, get_engine_from_env, get_src_dir

# -----------------------------------------------------------
# ⚙️ 안전한 변환 함수
# -----------------------------------------------------------
def safe_float(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return 0.0

def normalize_date(etl_str: str) -> str:
    """etl_ymd 문자열을 YYYY-MM-DD 형태로 변환"""
    etl_str = etl_str.strip()
    if "-" in etl_str:
        return etl_str  # 이미 YYYY-MM-DD
    elif len(etl_str) == 8:
        return datetime.datetime.strptime(etl_str, "%Y%m%d").strftime("%Y-%m-%d")
    elif len(etl_str) == 6:
        return datetime.datetime.strptime(etl_str + "01", "%Y%m%d").strftime("%Y-%m-%d")
    else:
        raise ValueError(f"Unknown etl_ymd format: {etl_str}")
    
# -----------------------------------------------------------
# 📅 월별 파티션 자동 생성 함수
# -----------------------------------------------------------
def ensure_partition(cur, etl_ymd_str):
    """etl_ymd 값(YYYYMMDD 또는 YYYY-MM-DD)을 기준으로 월별 파티션 생성"""
    # 입력 문자열 정규화
    etl_ymd_str = etl_ymd_str.strip()

    # 형식 자동 판별
    if "-" in etl_ymd_str:
        ymd = datetime.datetime.strptime(etl_ymd_str, "%Y-%m-%d").date()
    elif len(etl_ymd_str) == 8:
        ymd = datetime.datetime.strptime(etl_ymd_str, "%Y%m%d").date()
    elif len(etl_ymd_str) == 6:
        # YYYYMM 형태만 있는 경우 (예: 202501)
        ymd = datetime.datetime.strptime(etl_ymd_str + "01", "%Y%m%d").date()
    else:
        raise ValueError(f"Unknown date format: {etl_ymd_str}")

    start = ymd.replace(day=1)
    next_month = (start.replace(day=28) + datetime.timedelta(days=4)).replace(day=1)

    partition_name = f"public.tb_flowpop_{start.strftime('%Y%m')}"
    sql = f"""
    CREATE TABLE IF NOT EXISTS {partition_name}
        PARTITION OF public.tb_flowpop
        FOR VALUES FROM ('{start}') TO ('{next_month}');
    CREATE INDEX IF NOT EXISTS idx_tb_flowpop_{start.strftime('%Y%m')}_timezn_ymd
        ON {partition_name} (etl_ymd, timezn_cd, id);
    """
    cur.execute(sql)
    logger.info(f"📦 파티션 확인/생성 완료: {partition_name}")
    return partition_name

# -----------------------------------------------------------
# 🚀 메인 ETL 로직
# -----------------------------------------------------------
def load_flowpop(input_file):
    logger.info(f"시작: {input_file} 파일을 PostgreSQL로 적재합니다.")

    engine = get_engine_from_env()
    conn = engine.raw_connection()
    cur = conn.cursor()

    # -----------------------------------------------------------
    # ⚙️ 제거할 컬럼 목록
    # -----------------------------------------------------------
    columns_to_exclude = [
        'x', 'y',
        'm00', 'm15', 'm25', 'm35', 'm45', 'm55', 'm65',
        'f00', 'f15', 'f25', 'f35', 'f45', 'f55', 'f65',
        'admi_cd'
    ]

    # -----------------------------------------------------------
    # 📥 CSV 읽기
    # -----------------------------------------------------------
    with tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp_file:
        reader = csv.DictReader(open(input_file, 'r', encoding='utf-8', newline=''), delimiter='|')
        all_columns = reader.fieldnames
        selected_columns = [c for c in all_columns if c not in columns_to_exclude]
        final_columns = selected_columns
        writer = csv.writer(temp_file, delimiter=',')

        first_etl_ymd = None
        row_count = 0

        for row in reader:
            # etl_ymd 추출 (최초 한 번만)
            row['etl_ymd'] = normalize_date(row['etl_ymd'])
            if first_etl_ymd is None:
                first_etl_ymd = row['etl_ymd']

            # 성별·연령대 합산 (float 기반)
            row['m10'] = safe_float(row['m00']) + safe_float(row['m10']) + safe_float(row['m15'])
            row['m20'] = safe_float(row['m20']) + safe_float(row['m25'])
            row['m30'] = safe_float(row['m30']) + safe_float(row['m35'])
            row['m40'] = safe_float(row['m40']) + safe_float(row['m45'])
            row['m50'] = safe_float(row['m50']) + safe_float(row['m55'])
            row['m60'] = safe_float(row['m60']) + safe_float(row['m65'])

            row['f10'] = safe_float(row['f00']) + safe_float(row['f10']) + safe_float(row['f15'])
            row['f20'] = safe_float(row['f20']) + safe_float(row['f25'])
            row['f30'] = safe_float(row['f30']) + safe_float(row['f35'])
            row['f40'] = safe_float(row['f40']) + safe_float(row['f45'])
            row['f50'] = safe_float(row['f50']) + safe_float(row['f55'])
            row['f60'] = safe_float(row['f60']) + safe_float(row['f65'])

            # 필요 없는 컬럼 제거
            for c in columns_to_exclude:
                row.pop(c, None)

            # CSV 버퍼 기록
            writer.writerow([row[c] for c in final_columns])

            row_count += 1
            if row_count % 5000000 == 0:
                logger.info(f"진행 중: {row_count:,}행 처리 완료")

        temp_file.flush()

    if not first_etl_ymd:
        logger.error("❌ etl_ymd 값을 찾을 수 없습니다. CSV 구조를 확인하세요.")
        return

    logger.info(f"총 {row_count:,}행 변환 완료 (etl_ymd={first_etl_ymd})")

    # -----------------------------------------------------------
    # 🧱 파티션 자동 생성
    # -----------------------------------------------------------
    partition_name = ensure_partition(cur, first_etl_ymd)

    # -----------------------------------------------------------
    # 📨 COPY 수행
    # -----------------------------------------------------------
    with open(temp_file.name, 'r') as temp_file_read:
        logger.info(f"PostgreSQL COPY 시작 → {partition_name}")
        cur.copy_expert(f"""
            COPY {partition_name} ({', '.join(final_columns)})
            FROM STDIN WITH (FORMAT CSV)
        """, temp_file_read)

    conn.commit()
    cur.close()
    conn.close()

    logger.info(f"✅ 데이터 적재 완료: {partition_name}, 총 {row_count:,}행")

# -----------------------------------------------------------
# 🧭 실행부
# -----------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="FLOWPOP 월별 데이터 적재 스크립트")
    parser.add_argument("--ym", required=True, help="적재할 월(YYYYMM)")
    logger = setup_logger("flowpop")
    logger.info("▶ 스크립트 시작")
    args = parser.parse_args()

    # 소스 디렉토리 
    SRC_DIR = get_src_dir()
    pattern = f"*flow_age_time*{args.ym}*.csv"
    matched_files = sorted(glob.glob(os.path.join(SRC_DIR, pattern)))

    # 파일 탐색
    if not matched_files:
        logger.error(f"❌ {args.ym}이(가) 포함된 CSV 파일을 {SRC_DIR}에서 찾을 수 없습니다.")
        sys.exit(1)

    input_file = matched_files[-1]
    logger.info(f"선택된 파일: {input_file}")
    try:
        load_flowpop(input_file)
    except Exception as e:
        logger.exception(f"❌ 오류 발생: {e}")
        sys.exit(1)
