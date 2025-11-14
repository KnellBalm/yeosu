import pandas as pd
from sqlalchemy import text
from datetime import datetime, timedelta
from utils import setup_logger, get_engine_from_env
import re
import json

# ===============================
# 📘 1. SQL 파서
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


# ===============================
# 🧩 2. SQL 실행 / 적재 함수
# ===============================
def run_sql(engine, query: str, params: dict | None = None) -> pd.DataFrame:
    with engine.connect() as conn:
        result = conn.execute(text(query), params or {})
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
    return df


def write_to_db(df: pd.DataFrame, table_name: str, engine, schema: str = None, if_exists: str = "replace"):
    df.to_sql(
        name=table_name,
        con=engine,
        schema=schema,
        if_exists=if_exists,
        index=False,
        method="multi"
    )


# ===============================
# 🧹 3. 전처리 함수
# ===============================

def preprocess_household(df: pd.DataFrame, pop_grid_id: dict) -> pd.DataFrame:
    df = df.dropna()
    df['grid_id'] = df['jumin_rd_code'].map(pop_grid_id)
    df.drop(columns=['jumin_rd_code'],inplace=True)
    df = df[df['grid_id'].str.len() == 8]
    df = df[['grid_id', 'mem_cnt1', 'mem_cnt2', 'mem_cnt3', 'mem_cnt4']]
    return df

def preprocess_inflow(df: pd.DataFrame, pop_grid_id: dict) -> pd.DataFrame:
    df = df.dropna()
    df['gens'] = (df['age'] // 10 * 10).astype(int)
    df['grid_id'] = df['jumin_inr_rd_code'].map(pop_grid_id)
    df = df.groupby(['grid_id', 'gender', 'gens'], as_index=False).agg(
        member_cnt=('jumin_sid', 'count')
    )
    df = df[df['grid_id'].str.len() == 8]
    return df

def preprocess_outflow(df: pd.DataFrame, pop_grid_id: dict) -> pd.DataFrame:
    df = df.dropna()
    df['gens'] = (df['age'] // 10 * 10).astype(int)
    df['grid_id'] = df['jumin_exr_rd_code'].map(pop_grid_id)
    df = df.groupby(['grid_id', 'gender', 'gens'], as_index=False).agg(
        member_cnt=('jumin_sid', 'count')
    )
    df = df[df['grid_id'].str.len() == 8]
    return df

def preprocess_totpop(df: pd.DataFrame, pop_grid_id: dict) -> pd.DataFrame:
    df = df.dropna()
    df['gens'] = (df['age'] // 10 * 10).astype(int)
    df['grid_id'] = df['jumin_rd_code'].map(pop_grid_id)
    df = df.groupby(['grid_id', 'gens', 'gender'], as_index=False).agg(
        member_cnt=('jumin_sid', 'count')
    )
    df = df[df['grid_id'].str.len() == 8]
    return df

# ===============================
# 🚀 메인 파이프라인
# ===============================

logger = setup_logger("population")
logger.info("🏁 파이프라인 시작")

engine = get_engine_from_env()
queries = load_sql_sections('../sql/yeosu_query_251113.sql')
pop_grid_id = json.load(open('../data/json/pop_grid_id.json'))

# 1️⃣ 세대별
house_df = run_sql(engine, queries["1"])
house_df = preprocess_household(house_df, pop_grid_id)
write_to_db(house_df, "tb_pop_household_count", engine)
logger.info("✅ 세대별 완료")

# 2️⃣ 전입자
inflow_df = run_sql(engine, queries["2"])
inflow_df = preprocess_inflow(inflow_df, pop_grid_id)
write_to_db(inflow_df, "tb_pop_inflow_count", engine)
logger.info("✅ 전입자 완료")

# 3️⃣ 전출자
outflow_df = run_sql(engine, queries["3"])
outflow_df = preprocess_outflow(outflow_df, pop_grid_id)
write_to_db(outflow_df, "tb_pop_outflow_count", engine)
logger.info("✅ 전출자 완료")

# 4️⃣ 총인구
totpop_df = run_sql(engine, queries["4"])
totpop_df = preprocess_totpop(totpop_df, pop_grid_id)
write_to_db(totpop_df, "tb_pop_total_count", engine)
logger.info("✅ 총인구 완료")

logger.info("🎯 전체 파이프라인 완료")
