import csv
import psycopg2
from psycopg2 import sql
from io import StringIO
import argparse
import logging
import sys
import datetime
import os
import tempfile
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

# -----------------------------------------------------------
# ⚙️ 안전한 float 변환 함수
# -----------------------------------------------------------
def safe_float(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return 0.0

# -----------------------------------------------------------
# 📅 월별 파티션 자동 생성 함수
# -----------------------------------------------------------
def ensure_partition(cur, etl_ymd_str):
    """etl_ymd 값(YYYY-MM-DD) 기준으로 월별 파티션 생성"""
    ymd = datetime.datetime.strptime(etl_ymd_str, "%Y-%m-%d").date()
    start = ymd.replace(day=1)
    next_month = (start.replace(day=28) + datetime.timedelta(days=4)).replace(day=1)

    partition_name = f"yeosu_dm.tb_flowpop_{start.strftime('%Y%m')}"
    sql_query = sql.SQL("""
    CREATE TABLE IF NOT EXISTS {partition_name}
        PARTITION OF yeosu_dm.tb_flowpop
        FOR VALUES FROM (%s) TO (%s);
    CREATE INDEX IF NOT EXISTS {index_name}
        ON {partition_name} (timezn_cd, etl_ymd);
    """).format(
        partition_name=sql.Identifier(partition_name),
        index_name=sql.Identifier(f"idx_tb_flowpop_{start.strftime('%Y%m')}_timezn_ymd")
    )

    cur.execute(sql_query, [str(start), str(next_month)])
    logger.info(f"📦 파티션 확인/생성 완료: {partition_name}")
    return partition_name

# -----------------------------------------------------------
# 🚀 메인 ETL 로직
# -----------------------------------------------------------
def load_flowpop(input_file):
    logger.info(f"시작: {input_file} 파일을 PostgreSQL로 적재합니다.")

    conn = psycopg2.connect(
        dbname=os.getenv("DB_NAME", "postgres"),
        user=os.getenv("DB_USER", "root"),
        password=os.getenv("DB_PASS", "password"),
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432")
    )
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
            if row_count % 500000 == 0:
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
    parser.add_argument("--input", required=True, help="입력 CSV 파일 경로")
    logger = setup_logger("flowpop")
    logger.info("▶ 스크립트 시작")
    args = parser.parse_args()

    try:
        load_flowpop(args.input)
    except Exception as e:
        logger.exception(f"❌ 오류 발생: {e}")
        sys.exit(1)