import csv
import argparse
import sys
import os
import tempfile
import glob
from datetime import datetime, timedelta
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
    etl_str = etl_str.strip()

    if "-" in etl_str:
        return etl_str  # 이미 YYYY-MM-DD

    if len(etl_str) == 8:
        return datetime.strptime(etl_str, "%Y%m%d").strftime("%Y-%m-%d")

    if len(etl_str) == 6:
        return datetime.strptime(etl_str + "01", "%Y%m%d").strftime("%Y-%m-%d")

    raise ValueError(f"Unknown etl_ymd format: {etl_str}")

def ensure_parent_table(cur):
    """
    public.tb_flowpop 부모 테이블이 없으면 자동 생성.
    있으면 아무 작업도 하지 않음.
    """

    # 1) 부모 테이블 존재 여부 체크
    cur.execute("""
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables 
            WHERE table_schema='public' 
              AND table_name='tb_flowpop'
        );
    """)
    exists = cur.fetchone()[0]

    if exists:
        logger.info("✔ 부모 테이블 tb_flowpop 이미 존재")
        return

    logger.info("⚠ 부모 테이블 tb_flowpop 없음 → 자동 생성 시작")

    # 2) 부모 테이블 생성
    create_sql = """
    CREATE TABLE IF NOT EXISTS public.tb_flowpop (
        id           bpchar(8),
        "type"       varchar(20),
        timezn_cd    varchar(10),
        m10 float8,
        m20 float8,
        m30 float8,
        m40 float8,
        m50 float8,
        m60 float8,
        m70 float8,
        f10 float8,
        f20 float8,
        f30 float8,
        f40 float8,
        f50 float8,
        f60 float8,
        f70 float8,
        total float8,
        admi_cd varchar(20),
        etl_ymd date NOT NULL
    )
    PARTITION BY RANGE (etl_ymd);
    """

    cur.execute(create_sql)
    logger.info("🎉 부모 테이블 tb_flowpop 생성 완료")    

# -----------------------------------------------------------
# 📅 월별 파티션 자동 생성 함수
# -----------------------------------------------------------
def ensure_partition(cur, etl_ymd_str):
    """etl_ymd 값(YYYYMMDD 또는 YYYY-MM-DD)을 기준으로 월별 파티션 생성"""
    # 입력 문자열 정규화
    etl_ymd_str = etl_ymd_str.strip()

    # 형식 자동 판별
    if "-" in etl_ymd_str:
        ymd = datetime.strptime(etl_ymd_str, "%Y-%m-%d").date()
    elif len(etl_ymd_str) == 8:
        ymd = datetime.strptime(etl_ymd_str, "%Y%m%d").date()
    elif len(etl_ymd_str) == 6:
        # YYYYMM 형태만 있는 경우 (예: 202501)
        ymd = datetime.strptime(etl_ymd_str + "01", "%Y%m%d").date()
    else:
        raise ValueError(f"Unknown date format: {etl_ymd_str}")

    start = ymd.replace(day=1)
    next_month = (start.replace(day=28) + timedelta(days=4)).replace(day=1)

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

# -------------------------------------------------------------------
# 🔧 집계 테이블 자동 생성 공통 함수
# -------------------------------------------------------------------
def ensure_table_exists(engine, table_name, create_sql):
    check_sql = f"""
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema='public' AND table_name='{table_name}'
        );
    """

    raw = engine.raw_connection()
    try:
        cur = raw.cursor()
        cur.execute(check_sql)
        exists = cur.fetchone()[0]

        if exists:
            logger.info(f"✔ {table_name} 이미 존재")
        else:
            logger.info(f"⚠ {table_name} 없음 → 생성")
            cur.execute(create_sql)
            raw.commit()
            logger.info(f"🎉 {table_name} 생성 완료")

    finally:
        cur.close()
        raw.close()

# -------------------------------------------------------------------
# 📊 집계 테이블 생성 SQL
# -------------------------------------------------------------------
CREATE_AGG_AGEGEN = """
CREATE TABLE public.tb_flowpop_agg_agegen (
    crtr_ym varchar(6),
    type varchar(20),
    gender varchar(1),
    age varchar(3),
    total_population numeric
);
"""

CREATE_AGG_WEEKDAY = """
CREATE TABLE public.tb_flowpop_agg_timezn (
    crtr_ym varchar(6),
    timezn_cd varchar(10),
    type varchar(20),
    total_population numeric
);
"""

CREATE_AGG_TMZONE = """
CREATE TABLE public.tb_flowpop_agg_dayname (
    crtr_ym varchar(6),
    dayname varchar(10),
    type varchar(20),
    total_population numeric
);
"""

CREATE_AGG_DAILY = """
CREATE TABLE public.tb_flowpop_agg_daily (
    crtr_ym varchar(6),
    admi_cd varchar(20),
    etl_ymd date,
    total_population numeric
);
"""

# -----------------------------------------------------------
# 📊 SQL 기반 집계 생성 함수
# -----------------------------------------------------------

def run_sql_aggregations(ym, engine):
    # 집계 테이블 자동 생성
    ensure_table_exists(engine, "tb_flowpop_agg_agegen", CREATE_AGG_AGEGEN)
    ensure_table_exists(engine, "tb_flowpop_agg_timezn", CREATE_AGG_WEEKDAY)
    ensure_table_exists(engine, "tb_flowpop_agg_dayname", CREATE_AGG_TMZONE)
    ensure_table_exists(engine, "tb_flowpop_agg_daily", CREATE_AGG_DAILY)

    tn = f"public.tb_flowpop_{ym}"

    start_date = datetime.strptime(ym, "%Y%m").date()
    if start_date.month == 12:
        next_date = start_date.replace(year=start_date.year + 1, month=1, day=1)
    else:
        next_date = start_date.replace(month=start_date.month + 1, day=1)

    # 날짜 문자열 변환
    start_s = start_date.strftime("%Y-%m-%d")
    next_s  = next_date.strftime("%Y-%m-%d")

    # 쿼리 리스트 
    sql_dict = {
        "tb_flowpop_agg_agegen" : f"""
        INSERT INTO tb_flowpop_agg_agegen (crtr_ym, type, gender, age, total_population)
        WITH agg AS (
            SELECT 
                type,
                SUM(m10) AS m10, SUM(m20) AS m20, SUM(m30) AS m30, SUM(m40) AS m40,
                SUM(m50) AS m50, SUM(m60) AS m60, SUM(m70) AS m70,
                SUM(f10) AS f10, SUM(f20) AS f20, SUM(f30) AS f30, SUM(f40) AS f40,
                SUM(f50) AS f50, SUM(f60) AS f60, SUM(f70) AS f70
            FROM {tn}
            WHERE etl_ymd >= '{start_s}' AND etl_ymd < '{next_s}'
            GROUP BY type
        ),
        unpivot AS (
            SELECT 
                type,
                gender[i] AS gender,
                age[i] AS age,
                population[i] AS total_population
            FROM (
                SELECT
                    type,
                    ARRAY['M','M','M','M','M','M','M',
                        'F','F','F','F','F','F','F'] AS gender,
                    ARRAY['10','20','30','40','50','60','70',
                        '10','20','30','40','50','60','70'] AS age,
                    ARRAY[
                        m10, m20, m30, m40, m50, m60, m70,
                        f10, f20, f30, f40, f50, f60, f70
                    ] AS population
                FROM agg
            ) t,
            generate_subscripts(gender, 1) AS i
        )
        SELECT  
            '{ym}' AS crtr_ym,
            type,
            gender,
            age,
            ROUND(total_population::numeric, 2)
        FROM unpivot;
        """,

        "tb_flowpop_agg_timezn":f"""
        INSERT INTO tb_flowpop_agg_timezn (crtr_ym, timezn_cd, type, total_population)
        SELECT '{ym}', timezn_cd, type, ROUND(SUM(total)::numeric,2)
        FROM {tn}
        WHERE etl_ymd >= '{start_s}' AND etl_ymd < '{next_s}'
        GROUP BY timezn_cd, type;
        """,

        "tb_flowpop_agg_dayname":f"""
        INSERT INTO tb_flowpop_agg_dayname (crtr_ym, dayname, type, total_population)
        SELECT '{ym}', to_char(etl_ymd,'dy') as dayname, type, ROUND(SUM(total)::numeric,2)
        FROM {tn}
        WHERE etl_ymd >= '{start_s}' AND etl_ymd < '{next_s}'
        GROUP BY to_char(etl_ymd,'dy'), type;
        """,

        "tb_flowpop_agg_daily":f"""
        INSERT INTO tb_flowpop_agg_daily (crtr_ym, admi_cd, etl_ymd, total_population)
        SELECT '{ym}', admi_cd, etl_ymd, ROUND(SUM(total)::numeric,2)
        FROM {tn}
        WHERE etl_ymd >= '{start_s}' AND etl_ymd < '{next_s}'
        GROUP BY admi_cd, etl_ymd;
        """
    }

    # psycopg2 raw cursor 사용
    raw = engine.raw_connection()
    try:
        cur = raw.cursor()

        for name, query in sql_dict.items():
            logger.info(f"▶ [집계 실행 시작] {name}")
            cur.execute(query)
            logger.info(f"✔ [집계 실행 완료] {name}")

        raw.commit()
        logger.info(f"📊 전체 SQL 집계 테이블 생성 완료: {ym}")

    except Exception as e:
        raw.rollback()
        logger.error(f"❌ 집계 실행 오류 발생: {e}")
        raise e

    finally:
        cur.close()
        raw.close()

# -----------------------------------------------------------
# 🚀 메인 ETL 로직
# -----------------------------------------------------------
def load_flowpop(input_file):
    logger.info(f"시작: {input_file} 파일을 PostgreSQL로 적재합니다.")

    engine = get_engine_from_env()
    conn = engine.raw_connection()
    cur = conn.cursor()

    columns_to_exclude = [
        'x', 'y',
        'm00', 'm15', 'm25', 'm35', 'm45', 'm55', 'm65',
        'f00', 'f15', 'f25', 'f35', 'f45', 'f55', 'f65',
    ]

    with tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp_file:

        reader = csv.DictReader(open(input_file, 'r', encoding='utf-8', newline=''), delimiter='|')
        all_columns = reader.fieldnames
        selected_columns = [c for c in all_columns if c not in columns_to_exclude]
        final_columns = selected_columns

        writer = csv.writer(temp_file, delimiter=',')

        first_etl_ymd = None
        row_count = 0

        for row in reader:
            row['etl_ymd'] = normalize_date(row['etl_ymd'])

            if first_etl_ymd is None:
                first_etl_ymd = row['etl_ymd']

            # 연령/성별 합산
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

            row['admi_cd'] = str(int(row['admi_cd']) * 100)

            for c in columns_to_exclude:
                row.pop(c, None)

            writer.writerow([row[c] for c in final_columns])
            row_count += 1

            if row_count % 5000000 == 0:
                logger.info(f"진행 중: {row_count:,}행 처리 완료")

        temp_file.flush()

    if not first_etl_ymd:
        logger.error("❌ etl_ymd 값을 찾을 수 없습니다.")
        return

    logger.info(f"총 {row_count:,}행 변환 완료 (etl_ymd={first_etl_ymd})")
    ensure_parent_table(cur)
    partition_name = ensure_partition(cur, first_etl_ymd)

    with open(temp_file.name, 'r') as temp_file_read:
        logger.info(f"COPY 시작 → {partition_name}")
        cur.copy_expert(
            f"""
            COPY {partition_name} ({', '.join(final_columns)})
            FROM STDIN WITH (FORMAT CSV)
            """,
            temp_file_read
        )

    conn.commit()
    cur.close()
    conn.close()

    logger.info(f"✅ 데이터 적재 완료: {partition_name}, 총 {row_count:,}행")


# -----------------------------------------------------------
# 🧭 실행부
# -----------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="FLOWPOP 월별 데이터 적재 스크립트")
    parser.add_argument("ym", help="적재할 월(YYYYMM)")
    logger = setup_logger("flowpop")

    args = parser.parse_args()
    logger.info("▶ 스크립트 시작")

    SRC_DIR = get_src_dir()
    pattern = f"*flow_age_time*{args.ym}*.csv"
    matched_files = sorted(glob.glob(os.path.join(SRC_DIR, pattern)))

    if not matched_files:
        logger.error(f"❌ {args.ym}이 포함된 CSV 파일을 찾을 수 없습니다.")
        sys.exit(1)

    input_file = matched_files[-1]
    logger.info(f"선택된 파일: {input_file}")

    try:
        load_flowpop(input_file)
        run_sql_aggregations(args.ym, get_engine_from_env())
        logger.info("▶ 스크립트 종료")
    except Exception as e:
        logger.exception(f"❌ 오류 발생: {e}")
        sys.exit(1)
