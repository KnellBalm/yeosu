import joblib
import json
import logging
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta

# .env 파일 로드
load_dotenv()

# ============================================
# 🪶 로깅 설정
# ============================================
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

logger = setup_logger("wifi_predict")

# ============================================
# PostgreSQL 연결 생성
# ============================================
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

engine = get_engine()

# ============================================
# 📘 저장된 모델 및 전처리기 로드
# ============================================
bundle = joblib.load("xgb_quantile_bundle.joblib")
model = bundle["model"]
le = bundle["label_encoder"]
numeric_features = bundle["numeric_features"]
categorical_features = bundle["categorical_features"]

with open("xgb_quantile_metadata.json", "r") as f:
    meta = json.load(f)

logger.info(f"✅ 모델 버전 로드 완료 ({meta['trained_at']})")

# ============================================
# 📥 신규 데이터 준비
# ============================================
# 현재 날짜 기준으로 std_date 동적 생성
today = datetime.now()
start_date = (today - timedelta(days=30)).strftime('%Y-%m-%d')  # 30일 전부터 시작
logger.info(f"📅 동적 날짜 설정: {start_date} 이후 데이터 로드")

query = f"""
        SELECT ap_id, std_date, cnt AS acs_cnt
        FROM ap.log_summary
        WHERE LEFT(std_date, 10)::date >= '{start_date}'
        """

new_data = pd.read_sql_query(query, engine)
logger.info(f"✅ 신규 데이터 로드 완료 : {len(new_data)} rows")

# 와이파이 - 격자 매핑 ID 가져오기
wifi_grid_id = json.load(open('../data/json/wifi_grid_id.json'))

new_data['grid_id'] = new_data['ap_id'].map(wifi_grid_id)
new_data['std_date'] = pd.to_datetime(new_data['std_date'])
new_data = new_data.groupby(['grid_id', 'std_date'], as_index=False).agg(acs_cnt=('acs_cnt', 'sum'))

new_data['month'] = new_data['std_date'].dt.month
new_data['dayname'] = new_data['std_date'].dt.day_name()
new_data['hour'] = new_data['std_date'].dt.hour
new_data['is_weekend_group'] = new_data['dayname'].isin(["Friday", "Saturday", "Sunday"]).astype(int)

# 학습 시점의 인코더로 변환 (주의!)
try:
    new_data['dayname_encoded'] = le.transform(new_data['dayname'])
except ValueError:
    logger.warning("⚠️ 신규 데이터에 학습 시점에 없던 요일이 있습니다. 확인 필요.")

X = new_data[numeric_features + categorical_features]

# ============================================
# 📊 예측 및 결과 병합
# ============================================
preds = np.clip(model.predict(X), 0, None)
new_data["predicted_total"] = preds
new_data['grid_id'] = new_data['grid_id'].astype(int)
logger.info("✅ 예측 완료")

# ============================================
# 💾 결과 저장
# ============================================
result_cols = ['grid_id', 'std_date', 'predicted_total', 'acs_cnt']
results = new_data[result_cols]
results["grid_id"] = results["grid_id"].astype(str)
results.to_sql('TB_WIFI_PREDICTION', engine, schema='yeosu_dm', if_exists='append', index=False)
logger.info("✅ 예측 결과 저장 완료 to TB_WIFI_PREDICTION")

if __name__ == "__main__":
    logger.info("🚀 스크립트 실행 시작")