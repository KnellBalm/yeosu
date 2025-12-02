import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import pandas as pd
pd.set_option('mode.chained_assignment',  None) # <==== 경고를 끈다
from sqlalchemy import text, inspect, Date, Integer, String
from datetime import datetime, timedelta
from utils import *
import re
import json

BASE_DIR = get_base_dir()

# ===============================
# 📦 경로 설정
# ===============================
addr_id_map = json.load(open(f"{BASE_DIR}/data/json/addr_id_map.json"))
pop_grid_id = json.load(open(f"{BASE_DIR}/data/json/pop_grid_id.json"))

# ===============================
# 📘 SQL 파서
# ===============================
def load_sql_sections(file_path: str) -> dict[str, str]:
    """
    하나의 .sql 파일 안의 여러 쿼리를 주석 구분자(-- [쿼리명]) 기준으로 파싱합니다.
    """
    queries = {}
    current_name = None
    buffer = []
    pattern = re.compile(r"--\s*\[(.*?)\]")

    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            match = pattern.match(line.strip())
            if match:
                if current_name and buffer:
                    queries[current_name] = "\n".join(buffer).strip()
                    buffer = []
                current_name = match.group(1).strip()
            elif not line.strip().startswith("--"):
                buffer.append(line.rstrip())
        if current_name and buffer:
            queries[current_name] = "\n".join(buffer).strip()
    return queries

queries = load_sql_sections(f"{BASE_DIR}/sql/yeosu_query_251113.sql")

# ===============================
# 🧩 2. SQL 실행 / 적재 함수
# ===============================
def run_sql(engine, query: str, params: dict | None = None) -> pd.DataFrame:
    with engine.connect() as conn:
        result = conn.execute(text(query), params or {})
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
    return df

def write_to_db(df: pd.DataFrame, table_name: str, engine, schema: str = "public"):
    # --------------------------------------
    # 1) 테이블 존재 여부 확인
    # --------------------------------------
    inspector = inspect(engine)
    table_exists = inspector.has_table(table_name, schema=schema)

    # --------------------------------------
    # 2) 테이블이 없으면 → df 스키마로 생성 (reg_dttm 포함)
    # --------------------------------------
    if not table_exists:
        logger.info(f"⚠ 테이블 없음 → 생성: {schema}.{table_name}")
        df.head(0).to_sql(
            name=table_name,
            con=engine,
            schema=schema,
            if_exists='replace',   # 테이블 생성
            index=False,
            dtype={
            'reg_dttm': Date,
            'grid_id': String(8),
            'gens': String(2),
            }
        )
        # create만 하고 데이터가 없다면 종료
        if df.empty:
            return

        df.to_sql(
            name=table_name,
            con=engine,
            schema=schema,
            if_exists="append",
            index=False,
            method="multi",
            dtype={
                'reg_dttm': Date,
                'grid_id': String(8),
                'gens': String(2),
            }
        )
        return

    # --------------------------------------
    # 3) 테이블이 있는 경우 → 데이터 추가 (append)
    # --------------------------------------
    # logger.info(f"✔ 테이블 존재 → truncate 후 insert: {schema}.{table_name}")
    #
    # with engine.connect() as conn:
    #     conn.execution_options(isolation_level="AUTOCOMMIT").execute(
    #         text(f'TRUNCATE TABLE "{schema}"."{table_name}" RESTART IDENTITY CASCADE')
    #     )

    # 데이터가 없다면 종료
    if df.empty:
        logger.info(f"✔ 추가할 데이터 없음: {schema}.{table_name}")
        return

    # --------------------------------------
    # 4) 데이터 추가 (insert)
    # --------------------------------------
    logger.info(f"✔ 테이블 존재 → 데이터 추가: {schema}.{table_name}")
    df.to_sql(
        name=table_name,
        con=engine,
        schema=schema,
        if_exists="append",
        index=False,
        method="multi",
        dtype={
        'reg_dttm': Date,
        'grid_id': String(8),
        'gens': String(2),
            }
    )

# ===============================
# 🧹 3. 전처리 함수
# ===============================
def make_binding_key(row, rd_col, main_col, sub_col):
    rd_val = row[rd_col]
    main_val = row[main_col]
    sub_val = row[sub_col]

    # 🚨 본번이 없으면 즉시 undefined
    if pd.isna(main_val) or str(main_val).strip() == "":
        return "undefined"

    # 도로명코드가 없으면 undefined (법정동일 경우 regn_col을 전달함)
    if pd.isna(rd_val) or str(rd_val).strip() == "":
        return "undefined"

    def to_int_str(val):
        try:
            if pd.isna(val) or str(val).strip() == "":
                return None
            return str(int(float(val)))
        except:
            return None

    rd_str = to_int_str(rd_val)
    main_str = to_int_str(main_val)
    sub_str = to_int_str(sub_val)

    if rd_str is None or main_str is None:
        return "undefined"

    if sub_str:
        return f"{rd_str}-{main_str}-{sub_str}"
    else:
        return f"{rd_str}-{main_str}"

def find_full_addr_id(
    row,
    rd_col="jumin_rd_code",
    main_col="jumin_bdng_orgno",
    sub_col="jumin_bdng_subno",
    regn_col="jumin_regn_code",
    san_col="jumin_san"
):
    rd_val = row[rd_col]
    main_val = row[main_col]

    # 🚨 본번이 없으면 무조건 undefined
    if pd.isna(main_val) or str(main_val).strip() == "":
        return "undefined"

    # =============================
    # 1) 도로명 기반
    # =============================
    if not pd.isna(rd_val) and str(rd_val).strip() != "":
        base = make_binding_key(row, rd_col, main_col, sub_col)
    else:
        # =============================
        # 2) 도로명 없음 → 법정동 기반
        # =============================
        base = make_binding_key(row, regn_col, main_col, sub_col)

    if base == "undefined":
        return "undefined"

    # base: "code-main" 또는 "code-main-sub"
    parts = base.split("-")
    code = parts[0]
    main_sub = parts[1:]

    try:
        san = int(float(row[san_col]))
    except:
        san = 1

    # main-sub 구조 처리
    if len(main_sub) == 2:
        main, sub = main_sub
        main = f"S{main}" if san == 2 else main
        result = f"{code}-{main}-{sub}"
    else:
        main = main_sub[0]
        main = f"S{main}" if san == 2 else main
        result = f"{code}-{main}"

    return result

# ===============================
# 🧹 전처리 함수들
# ===============================
def preprocess_household(df: pd.DataFrame, addr_id_map: dict, pop_grid_id: dict) -> pd.DataFrame:
    # 1) 주소 매핑
    df['full_addr_id'] = df.apply(find_full_addr_id, axis=1)
    df['full_addr_name'] = df['full_addr_id'].map(addr_id_map)

    # 2) grid 매핑
    df['grid_id'] = df['full_addr_name'].map(pop_grid_id)

    # 3) 유효 grid만 남기기
    df = df[df['grid_id'].notnull() & (df['grid_id'].str.len() == 8)]
    df['grid_id'] = df['grid_id'].astype(str)

    # 4) 세대 규모 집계
    gb_df = df.groupby(['grid_id'], as_index=False).agg(
        total_household_cnt=('jumin_head_sid', 'count'),
        mem_cnt1=('member_count', lambda x: (x == 1).sum()),
        mem_cnt2=('member_count', lambda x: (x == 2).sum()),
        mem_cnt3=('member_count', lambda x: (x == 3).sum()),
        mem_cnt4=('member_count', lambda x: (x >= 4).sum()),
    )
    gb_df['reg_dttm'] = datetime.now().strftime('%Y-%m-%d')
    return gb_df

def preprocess_inflow(df: pd.DataFrame, addr_id_map: dict, pop_grid_id: dict) -> pd.DataFrame:
    df['full_addr_id'] = df.apply(
        lambda row: find_full_addr_id(
            row,
            rd_col="jumin_inr_rd_code",
            main_col="jumin_inr_bdng_orgno",
            sub_col="jumin_inr_bdng_subno",
            regn_col="jumin_inr_regn_code",
            san_col="jumin_inr_san"
        ),
        axis=1
    )
    df['full_addr_name'] = df['full_addr_id'].map(addr_id_map)
    df['grid_id'] = df['full_addr_name'].map(pop_grid_id)
    df['gens'] = (df['age'] // 10 * 10).astype(int)
    df = df[df['grid_id'].notnull() & (df['grid_id'].str.len() == 8)]
    df['grid_id'] = df['grid_id'].astype(str)
    df['gender'] = df['gender'].astype(str)
    df['gens'] = df['gens'].astype(str)
    df = df.groupby(['grid_id', 'gender', 'gens'], as_index=False).agg(
        member_cnt=('jumin_sid', 'count')
    )
    df['reg_dttm'] = datetime.now().strftime('%Y-%m-%d')
    return df

def preprocess_outflow(df: pd.DataFrame, addr_id_map: dict, pop_grid_id: dict) -> pd.DataFrame:
    df['full_addr_id'] = df.apply(
        lambda row: find_full_addr_id(
            row,
            rd_col="jumin_exr_rd_code",
            main_col="jumin_exr_bdng_orgno",
            sub_col="jumin_exr_bdng_subno",
            regn_col="jumin_exr_regn_code",
            san_col="jumin_exr_san"
        ),
        axis=1
    )
    df['full_addr_name'] = df['full_addr_id'].map(addr_id_map)
    df['grid_id'] = df['full_addr_name'].map(pop_grid_id)
    df['gens'] = (df['age'] // 10 * 10).astype(int)
    df = df[df['grid_id'].notnull() & (df['grid_id'].str.len() == 8)]
    df['grid_id'] = df['grid_id'].astype(str)
    df['gender'] = df['gender'].astype(str)
    df['gens'] = df['gens'].astype(str)
    df = df.groupby(['grid_id', 'gender', 'gens'], as_index=False).agg(
        member_cnt=('jumin_sid', 'count')
    )
    df['reg_dttm'] = datetime.now().strftime('%Y-%m-%d')
    return df

def preprocess_totpop(df: pd.DataFrame, addr_id_map: dict, pop_grid_id: dict) -> pd.DataFrame:
    df['full_addr_id'] = df.apply(find_full_addr_id, axis=1)
    df['full_addr_name'] = df['full_addr_id'].map(addr_id_map)
    df['grid_id'] = df['full_addr_name'].map(pop_grid_id)
    df['gens'] = (df['age'] // 10 * 10).astype(int)
    df = df[df['grid_id'].notnull() & (df['grid_id'].str.len() == 8)]
    df['grid_id'] = df['grid_id'].astype(str)
    df['gender'] = df['gender'].astype(str)
    df['gens'] = df['gens'].astype(str)
    df = df.groupby(['grid_id', 'gens', 'gender'], as_index=False).agg(
        member_cnt=('jumin_sid', 'count')
    )
    df['reg_dttm'] = datetime.now().strftime('%Y-%m-%d')
    return df

def preprocess_one_person(df: pd.DataFrame, addr_id_map: dict, pop_grid_id: dict) -> pd.DataFrame:
    # 1) 주소 매핑
    df['full_addr_id'] = df.apply(find_full_addr_id, axis=1)
    df['full_addr_name'] = df['full_addr_id'].map(addr_id_map)

    # 2) grid 매핑
    df['grid_id'] = df['full_addr_name'].map(pop_grid_id)

    # 3) 유효 grid만 남기기
    df = df[df['grid_id'].notnull() & (df['grid_id'].str.len() == 8)]
    df['grid_id'] = df['grid_id'].astype(str)
    
    # 4) 세대 만들기
    df['gens'] = (df['age'] // 10 * 10).astype(str)

    # 5) 1인 가구 집계
    gb_df = df.groupby(['grid_id', 'gens','gender'], as_index=False).agg(
        one_person_cnt=('jumin_head_sid', 'count')
    )
    gb_df['reg_dttm'] = datetime.now().strftime('%Y-%m-%d')
    return gb_df

## ==============================
# 🚀 파이프라인 실행 함수
## ==============================
def run_pipeline_step(step_name: str, query_key: str, preprocess_fn, output_table: str,
                      engine, queries, addr_id_map, pop_grid_id):
    logger.info(f"▶ {step_name} 시작")

    df = run_sql(engine, queries[query_key])
    df = preprocess_fn(df, addr_id_map, pop_grid_id)
    write_to_db(df, output_table, engine)

    logger.info(f"✅ {step_name} 완료")

# ===============================
# 🚀 메인 파이프라인
# ===============================
logger = setup_logger("population")
logger.info("🏁 파이프라인 시작")

engine = get_engine_from_env()

pipeline_steps = [
    ("세대별", "1", preprocess_household, "tb_pop_household_count"),
    ("전입자", "2", preprocess_inflow, "tb_pop_inflow_count"),
    ("전출자", "3", preprocess_outflow, "tb_pop_outflow_count"),
    ("총인구", "4", preprocess_totpop, "tb_pop_total_count"),
    ("1인 가구", "5", preprocess_one_person, "tb_pop_oneperson_count"),
]

for step_name, q_key, fn, table in pipeline_steps:
    run_pipeline_step(step_name, q_key, fn, table,
                      engine, queries, addr_id_map, pop_grid_id)

logger.info("🎯 전체 파이프라인 완료")
