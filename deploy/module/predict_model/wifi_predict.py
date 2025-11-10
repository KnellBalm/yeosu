import joblib
import pandas as pd
import numpy as np
from dotenv import load_dotenv, find_dotenv
import os
from datetime import datetime, timedelta
from module.utils import setup_logger, get_engine_from_env

# .env 파일 로드
bundle_path = "/DATA/jupyter_WorkingDirectory/notebook/yeosu/deploy/module/predict_model/xgb_quantile_bundle.joblib"
metadata_path = "/DATA/jupyter_WorkingDirectory/notebook/yeosu/deploy/module/predict_model/xgb_quantile_metadata.json"
grid_mapping_path = "/DATA/jupyter_WorkingDirectory/notebook/yeosu/deploy/data/json/wifi_grid_id.json"

env_path = find_dotenv(usecwd=True)
if not env_path:
    env_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".env"))
load_dotenv(env_path)
# ============================================
# 🪶 로깅 설정
# ============================================
logger = setup_logger("wifi_predict")

# ============================================
# PostgreSQL 연결 생성
# ============================================
# 소스 DB (와이파이)
source_engine = get_engine_from_env(
    user_env="WIFI_DB_USER",
    pass_env="WIFI_DB_PASS",
    host_env="WIFI_DB_HOST",
    port_env="WIFI_DB_PORT",
    name_env="WIFI_DB_NAME"
)
# 타겟 DB (결과값 적재)
target_engine = get_engine_from_env()

# ============================================
# 📘 저장된 모델 및 전처리기 로드
# ============================================
bundle = joblib.load(bundle_path)
model = bundle["model"]
le = bundle["label_encoder"]
numeric_features = bundle["numeric_features"]
categorical_features = bundle["categorical_features"]

with open(metadata_path, "r") as f:
    meta = json.load(f)

logger.info(f"✅ 모델 버전 로드 완료 ({meta['trained_at']})")

# ============================================
# 📥 신규 데이터 준비
# ============================================
# 현재 날짜 기준으로 std_date 동적 생성
today = datetime.now()
start_date = (today - timedelta(days=60)).strftime('%Y-%m-%d')  # 30일 전부터 시작
logger.info(f"📅 동적 날짜 설정: {start_date} 이후 데이터 로드")

query = f"""
        SELECT ap_id, std_date, cnt AS acs_cnt
        FROM ap.log_summary
        WHERE LEFT(std_date, 10)::date >= '{start_date}'
        """
logger.debug(f"{query=}")
new_data = pd.read_sql_query(query, source_engine)
logger.info(f"✅ 신규 데이터 로드 완료 : {len(new_data):,} rows")

# 와이파이 - 격자 매핑 ID 가져오기
wifi_grid_id = json.load(open(grid_mapping_path, "r"))

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
# 현재 시각을 reg_dttm 컬럼으로 추가
results["reg_dttm"] = datetime.now()  # 현재 시각
# 컬럼 순서 조정 (원하는 순서로)
save_cols = result_cols + ["reg_dttm"]
results = results[save_cols]

results["grid_id"] = results["grid_id"].astype(str)
results.to_sql('tb_wifi_prediction', target_engine, schema='public', if_exists='append', index=False)
logger.info("✅ 예측 결과 저장 완료 to TB_WIFI_PREDICTION")

if __name__ == "__main__":
    pass 
    #logger.info("🚀 스크립트 실행 시작")