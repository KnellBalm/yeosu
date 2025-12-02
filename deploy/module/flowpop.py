import csv
import argparse
import sys
import os
import tempfile
import glob
from datetime import datetime, timedelta
from utils import *
import pandas as pd
from sqlalchemy import text

BASE_DIR = get_base_dir()

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

    start_month = ymd.replace(day=1)
    next_month = (start_month.replace(day=28) + timedelta(days=4)).replace(day=1)

    ym = start_month.strftime('%Y%m')
    partition_name = f"public.tb_flowpop_{ym}"

    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {partition_name} PARTITION OF public.tb_flowpop
        FOR VALUES FROM ('{start_month}') TO ('{next_month}');
    """)
    logger.info(f"📦 파티션 확인/생성 완료 (인덱스 제외): {partition_name}")
    return partition_name

# -----------------------------------------------------------
# 🚀 메인 ETL 로직
# -----------------------------------------------------------
def load_flowpop(input_file):
    logger.info(f"시작: {input_file} 파일을 PostgreSQL로 적재합니다.")

    engine = get_engine_from_env()
    conn = engine.raw_connection()
    cur = conn.cursor()

    columns_to_exclude = [
        'x', 'y','admi_cd',
        'm00', 'm15', 'm25', 'm35', 'm45', 'm55', 'm65',
        'f00', 'f15', 'f25', 'f35', 'f45', 'f55', 'f65',
    ]

    # 빠른 float 변환 함수
    def to_float(x):
        try:
            return float(x)
        except:
            return 0.0

    # -------------------------------
    # CSV 변환 단계 최적화
    # -------------------------------
    with tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp_file:

        # CSV reader 준비
        with open(input_file, 'r', encoding='utf-8', newline='') as f:
            reader = csv.reader(f, delimiter='|')
            header = next(reader)
            idx = {col: i for i, col in enumerate(header)}

            # 최종 컬럼 (exclude 제외)
            final_columns = [c for c in header if c not in columns_to_exclude]

            # writer 버퍼 최적화
            write = temp_file.write

            first_etl_ymd = None
            row_count = 0

            for r in reader:
                # 날짜 정규화
                etl_ymd = normalize_date(r[idx['etl_ymd']])
                if first_etl_ymd is None:
                    first_etl_ymd = etl_ymd

                # -------------------------
                # 연령/성별 합산 (빠르게)
                # -------------------------
                m10 = to_float(r[idx['m00']]) + to_float(r[idx['m10']]) + to_float(r[idx['m15']])
                m20 = to_float(r[idx['m20']]) + to_float(r[idx['m25']])
                m30 = to_float(r[idx['m30']]) + to_float(r[idx['m35']])
                m40 = to_float(r[idx['m40']]) + to_float(r[idx['m45']])
                m50 = to_float(r[idx['m50']]) + to_float(r[idx['m55']])
                m60 = to_float(r[idx['m60']]) + to_float(r[idx['m65']])

                f10 = to_float(r[idx['f00']]) + to_float(r[idx['f10']]) + to_float(r[idx['f15']])
                f20 = to_float(r[idx['f20']]) + to_float(r[idx['f25']])
                f30 = to_float(r[idx['f30']]) + to_float(r[idx['f35']])
                f40 = to_float(r[idx['f40']]) + to_float(r[idx['f45']])
                f50 = to_float(r[idx['f50']]) + to_float(r[idx['f55']])
                f60 = to_float(r[idx['f60']]) + to_float(r[idx['f65']])

                # 원본 row에서 필요한 컬럼만 빠르게 구성
                out = []
                for col in final_columns:
                    if col == 'etl_ymd':
                        out.append(etl_ymd)
                    elif col == 'm10': out.append(str(m10))
                    elif col == 'm20': out.append(str(m20))
                    elif col == 'm30': out.append(str(m30))
                    elif col == 'm40': out.append(str(m40))
                    elif col == 'm50': out.append(str(m50))
                    elif col == 'm60': out.append(str(m60))
                    elif col == 'f10': out.append(str(f10))
                    elif col == 'f20': out.append(str(f20))
                    elif col == 'f30': out.append(str(f30))
                    elif col == 'f40': out.append(str(f40))
                    elif col == 'f50': out.append(str(f50))
                    elif col == 'f60': out.append(str(f60))
                    else:
                        out.append(r[idx[col]])

                write(",".join(out) + "\n")
                row_count += 1

                if row_count % 5_000_000 == 0:
                    temp_file.flush()
                    logger.info(f"진행 중: {row_count:,}행 처리 완료")

            temp_file.flush()

    # -------------------------------
    # 이후 로직(파티션 구성, COPY, ANALYZE)은 동일
    # -------------------------------
    temp_filename = temp_file.name

    if not first_etl_ymd:
        logger.error("❌ etl_ymd 값을 찾을 수 없습니다.")
        return
    
    logger.info(f"총 {row_count:,}행 변환 완료 (etl_ymd={first_etl_ymd})")
    ensure_parent_table(cur)
    partition_name = ensure_partition(cur, first_etl_ymd)
    logger.info(f"COPY 시작: {partition_name}에 데이터 적재")

    # -------------------------------
    # COPY
    # -------------------------------
    with open(temp_file.name, 'r') as f:
        cur.copy_expert(
            f"COPY {partition_name} ({', '.join(final_columns)}) FROM STDIN WITH (FORMAT CSV)",
            f
        )
    logger.info("COPY 완료")
    
    try:
        cur.execute(f"ANALYZE {partition_name};")
        conn.commit()
        logger.info(f"ANALYZE 완료: {partition_name}")

        # -------------------------------
        # 인덱스 생성 (COPY 이후)
        # -------------------------------
        logger.info(f"인덱스 생성 시작: {partition_name}")
        ym = first_etl_ymd.replace('-', '')[:6]
        cur.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_fp_{ym}_web ON {partition_name} (etl_ymd, timezn_cd, id, type);
            CREATE INDEX IF NOT EXISTS idx_fp_{ym}_ymd ON {partition_name} (etl_ymd);
            CREATE INDEX IF NOT EXISTS idx_fp_{ym}_type ON {partition_name} (type);
        """)
        conn.commit()
        logger.info(f"인덱스 생성 완료: {partition_name}")


        logger.info(f"ETL 완료: {partition_name} / {row_count:,} rows")
    finally:
        cur.close()
        conn.close()
        # 임시 파일 삭제
        if os.path.exists(temp_filename):
            os.remove(temp_filename)
            logger.info(f"임시 파일 삭제 완료: {temp_filename}")

    ####################### 보존 ############################################## # logger.info(f"시작: {input_file} 파일을 PostgreSQL로 적재합니다.")

    # engine = get_engine_from_env()
    # conn = engine.raw_connection()
    # cur = conn.cursor()

    # columns_to_exclude = [
    #     'x', 'y',
    #     'm00', 'm15', 'm25', 'm35', 'm45', 'm55', 'm65',
    #     'f00', 'f15', 'f25', 'f35', 'f45', 'f55', 'f65',
    # ]

    # with tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp_file:
    #     reader = csv.DictReader(open(input_file, 'r', encoding='utf-8', newline=''), delimiter='|')
    #     all_columns = reader.fieldnames
    #     selected_columns = [c for c in all_columns if c not in columns_to_exclude]
    #     final_columns = selected_columns

    #     writer = csv.writer(temp_file, delimiter=',')

    #     first_etl_ymd = None
    #     row_count = 0

    #     for row in reader:
    #         row['etl_ymd'] = normalize_date(row['etl_ymd'])

    #         if first_etl_ymd is None:
    #             first_etl_ymd = row['etl_ymd']

    #         # 연령/성별 합산
    #         row['m10'] = safe_float(row['m00']) + safe_float(row['m10']) + safe_float(row['m15'])
    #         row['m20'] = safe_float(row['m20']) + safe_float(row['m25'])
    #         row['m30'] = safe_float(row['m30']) + safe_float(row['m35'])
    #         row['m40'] = safe_float(row['m40']) + safe_float(row['m45'])
    #         row['m50'] = safe_float(row['m50']) + safe_float(row['m55'])
    #         row['m60'] = safe_float(row['m60']) + safe_float(row['m65'])

    #         row['f10'] = safe_float(row['f00']) + safe_float(row['f10']) + safe_float(row['f15'])
    #         row['f20'] = safe_float(row['f20']) + safe_float(row['f25'])
    #         row['f30'] = safe_float(row['f30']) + safe_float(row['f35'])
    #         row['f40'] = safe_float(row['f40']) + safe_float(row['f45'])
    #         row['f50'] = safe_float(row['f50']) + safe_float(row['f55'])
    #         row['f60'] = safe_float(row['f60']) + safe_float(row['f65'])

    #         for c in columns_to_exclude:
    #             row.pop(c, None)

    #         writer.writerow([row[c] for c in final_columns])
    #         row_count += 1

    #         if row_count % 5000000 == 0:
    #             logger.info(f"진행 중: {row_count:,}행 처리 완료")

    #     temp_file.flush()

    # if not first_etl_ymd:
    #     logger.error("❌ etl_ymd 값을 찾을 수 없습니다.")
    #     return

    # logger.info(f"총 {row_count:,}행 변환 완료 (etl_ymd={first_etl_ymd})")
    # ensure_parent_table(cur)
    # partition_name = ensure_partition(cur, first_etl_ymd)

    # # -------------------------------
    # # COPY
    # # -------------------------------
    # with open(temp_file.name,'r') as f:
    #     cur.copy_expert(
    #         f"""
    #         COPY {partition_name} ({', '.join(final_columns)})
    #         FROM STDIN WITH (FORMAT CSV)
    #         """, f
    #     )
    # logger.info("COPY 완료")


    # # -------------------------------
    # # ANALYZE
    # # -------------------------------
    # cur.execute(f"ANALYZE {partition_name};")
    # conn.commit()
    # cur.close()
    # conn.close()

    # logger.info(f"ETL 완료: {partition_name} / {count:,} rows")
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
    type bpchar(1),
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

    logger.info(f"[집계] 대상 테이블: {tn}, 기간: {start_s} ~ {next_s}")

    # ---------------------------------------------------
    # Pandas 청크 기반 집계 (메모리 최적화)
    # ---------------------------------------------------
    select_sql = f"""
        SELECT
            type, timezn_cd, etl_ymd,
            m10, m20, m30, m40, m50, m60, m70,
            f10, f20, f30, f40, f50, f60, f70,
            total, to_char(etl_ymd, 'dy') AS dayname
        FROM {tn}
        WHERE etl_ymd >= %s AND etl_ymd < %s
    """

    chunksize = 1_000_000  # 한 번에 처리할 행의 수 (메모리 상황에 따라 조절)
    iterator = pd.read_sql_query(select_sql, engine, params=(start_s, next_s), chunksize=chunksize)

    # 각 집계 결과를 저장할 리스트
    agegen_chunks, daily_chunks, dayname_chunks, timezn_chunks = [], [], [], []
    total_rows = 0

    try:
        for i, chunk_df in enumerate(iterator):
            total_rows += len(chunk_df)
            logger.info(f"  - 청크 {i+1} 처리 중... ({len(chunk_df):,} rows, 총 {total_rows:,} rows)")

            # 1) 연령/성별 집계
            agegen_chunks.append(chunk_df.groupby(['type']).agg(
                m10=('m10','sum'), m20=('m20','sum'), m30=('m30','sum'), m40=('m40','sum'), m50=('m50','sum'), m60=('m60','sum'), m70=('m70','sum'),
                f10=('f10','sum'), f20=('f20','sum'), f30=('f30','sum'), f40=('f40','sum'), f50=('f50','sum'), f60=('f60','sum'), f70=('f70','sum')
            ))
            # 2) 일별 집계
            daily_chunks.append(chunk_df.groupby(['etl_ymd','type']).agg(total_population=('total','sum')))
            # 3) 요일별 집계
            dayname_chunks.append(chunk_df.groupby(['dayname','type']).agg(total_population=('total','sum')))
            # 4) 시간대별 집계
            timezn_chunks.append(chunk_df.groupby(['timezn_cd','type']).agg(total_population=('total','sum')))

        logger.info(f"✅ 데이터 조회 및 청크별 집계 완료: 총 {total_rows:,} rows")

        # ---------------------------------------------------
        # 최종 집계 및 DB 적재
        # ---------------------------------------------------
        with engine.connect() as conn:
            # 트랜잭션 시작
            with conn.begin():
                # 1) 연령/성별 최종 집계
                logger.info("▶ [최종 집계] tb_flowpop_agg_agegen")
                agegen_df = pd.concat(agegen_chunks).groupby(level=0).sum().reset_index()
                agegen_df = agegen_df.melt(id_vars=['type'], var_name='col', value_name='total_population')
                agegen_df['gender'] = agegen_df['col'].str[0].str.upper()
                agegen_df['age'] = agegen_df['col'].str[1:].astype(int)
                agegen_df['crtr_ym'] = ym
                agegen_df = agegen_df[['crtr_ym','type', 'gender', 'age', 'total_population']]
                conn.execute(text(f"DELETE FROM tb_flowpop_agg_agegen WHERE crtr_ym = '{ym}'"))
                agegen_df.to_sql('tb_flowpop_agg_agegen', conn, if_exists='append', index=False, method='multi')
                logger.info("✔ tb_flowpop_agg_agegen 적재 완료")

                # 2) 일별 최종 집계
                logger.info("▶ [최종 집계] tb_flowpop_agg_daily")
                daily_gb = pd.concat(daily_chunks).groupby(level=[0,1]).sum().reset_index()
                daily_gb['crtr_ym'] = ym
                daily_gb = daily_gb[['crtr_ym','type','etl_ymd','total_population']]
                conn.execute(text(f"DELETE FROM tb_flowpop_agg_daily WHERE crtr_ym = '{ym}'"))
                daily_gb.to_sql('tb_flowpop_agg_daily', conn, if_exists='append', index=False, method='multi')
                logger.info("✔ tb_flowpop_agg_daily 적재 완료")

                # 3) 요일별 최종 집계
                logger.info("▶ [최종 집계] tb_flowpop_agg_dayname")
                dayname_gb = pd.concat(dayname_chunks).groupby(level=[0,1]).sum().reset_index()
                dayname_gb['crtr_ym'] = ym
                dayname_gb = dayname_gb[['crtr_ym','dayname','type','total_population']]
                conn.execute(text(f"DELETE FROM tb_flowpop_agg_dayname WHERE crtr_ym = '{ym}'"))
                dayname_gb.to_sql('tb_flowpop_agg_dayname', conn, if_exists='append', index=False, method='multi')
                logger.info("✔ tb_flowpop_agg_dayname 적재 완료")

                # 4) 시간대별 최종 집계
                logger.info("▶ [최종 집계] tb_flowpop_agg_timezn")
                timezn_gb = pd.concat(timezn_chunks).groupby(level=[0,1]).sum().reset_index()
                timezn_gb['crtr_ym'] = ym
                timezn_gb = timezn_gb[['crtr_ym','timezn_cd','type','total_population']]
                conn.execute(text(f"DELETE FROM tb_flowpop_agg_timezn WHERE crtr_ym = '{ym}'"))
                timezn_gb.to_sql('tb_flowpop_agg_timezn', conn, if_exists='append', index=False, method='multi')
                logger.info("✔ tb_flowpop_agg_timezn 적재 완료")

        logger.info(f"🎉 전체 집계 완료: {ym}")

    except Exception as e:
        logger.error(f"❌ 집계 처리 중 오류 발생: {e}")
        raise e

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
