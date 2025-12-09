import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import joblib
import pandas as pd
import numpy as np
import json
from dotenv import load_dotenv, find_dotenv
import os
from datetime import datetime, timedelta
from utils import *

BASE_DIR = get_base_dir()

# ============================================
# 🪶 로깅 설정
# ============================================
logger = setup_logger("wifi_predict")

# ============================================
# PostgreSQL 연결 생성
# ============================================
def main():
    logger.info("🚀 Wi-Fi 예측 스크립트 시작")

    # .env 파일 로드
    bundle_path = f"{BASE_DIR}/module/predict_model/xgb_quantile_bundle.joblib"
    metadata_path = f"{BASE_DIR}/module/predict_model/xgb_quantile_metadata.json"
    grid_mapping_path = f"{BASE_DIR}/data/json/wifi_grid_id.json"
    
    logger.debug(f"Bundle path: {bundle_path}")
    logger.debug(f"Metadata path: {metadata_path}")
    logger.debug(f"Grid mapping path: {grid_mapping_path}")

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
    logger.debug(f"Model: {model}")
    logger.debug(f"Numeric features: {numeric_features}")
    logger.debug(f"Categorical features: {categorical_features}")

    # ============================================
    # 📥 신규 데이터 준비
    # ============================================
    query = """
            SELECT std_date, ap_id, cnt, dong_nm, detail_address, "location", weekday, "date", "hour", mac
            FROM ap.log_summary_rukus where std_date = (select max(std_date) from ap.log_summary_rukus);
            """
    logger.debug(f"Executing query: {query}")
    new_data = pd.read_sql_query(query, source_engine)
    new_data['ap_id'] = new_data['ap_id'].astype(str)
    logger.info(f"✅ 신규 데이터 로드 완료 : {len(new_data):,} rows")
    logger.debug(f"Initial new_data shape: {new_data.shape} ")

    # 와이파이 - 격자 매핑 ID 가져오기
    with open(grid_mapping_path, "r", encoding='utf-8') as f:
        wifi_grid_id = json.load(f)

    new_data['grid_id'] = new_data['ap_id'].map(wifi_grid_id)
    logger.debug(f"Data shape after mapping grid_id: {new_data.shape}")
    # grid_id가 없는(NaN) 데이터는 예측에 사용할 수 없으므로 제거
    new_data.dropna(subset=['grid_id'], inplace=True)
    logger.debug(f"Data shape after dropping NaN grid_id: {new_data.shape}")

    new_data['std_date'] = pd.to_datetime(new_data['std_date'])
    new_data = new_data.groupby(['grid_id', 'std_date'], as_index=False).agg(acs_cnt=('cnt', 'sum'))
    logger.debug(f"Data shape after grouping by grid_id, std_date: {new_data.shape} ")

    new_data['month'] = new_data['std_date'].dt.month
    new_data['dayname'] = new_data['std_date'].dt.day_name()
    new_data['hour'] = new_data['std_date'].dt.hour
    new_data['is_weekend_group'] = new_data['dayname'].isin(["Friday", "Saturday", "Sunday"]).astype(int)

    # 학습 시점의 인코더로 변환 (주의!)
    try:
        new_data['dayname_encoded'] = le.transform(new_data['dayname'])
    except ValueError as e:
        logger.warning(f"⚠️ 신규 데이터에 학습 시점에 없던 요일이 있습니다. 확인 필요. Error: {e}")
        # 학습 시점에 없던 요일은 -1 등으로 처리하거나, 해당 데이터를 제외할 수 있음
        # 여기서는 일단 예측에서 제외되도록 NaN을 유발
        new_data['dayname_encoded'] = new_data['dayname'].apply(lambda x: le.transform([x])[0] if x in le.classes_ else -1)

    X = new_data[numeric_features + categorical_features]
    logger.debug(f"Feature matrix X shape: {X.shape}\n{X.head()}")

    # ============================================
    # 📊 예측 및 결과 병합
    # ============================================
    logger.debug(f"X missing values:\n{X.isnull().sum()}")

    try:
        preds_values = model.predict(X)
    except Exception as e:
        logger.exception(f"❌ 예측 중 오류 발생: {e}")
        raise e
    logger.debug(f"Raw predictions shape: {preds_values.shape}")
    logger.debug(f"Raw predictions min: {preds_values.min()}, max: {preds_values.max()}, mean: {preds_values.mean():.2f}")

    preds = np.clip(preds_values, 0, None)
    logger.debug(f"Clipped predictions min: {preds.min()}, max: {preds.max()}, mean: {preds.mean():.2f}")
    new_data["predicted_total"] = preds
    new_data['grid_id'] = new_data['grid_id'].astype(int)
    logger.info("✅ 예측 완료")
    logger.debug(f"Final data shape with predictions: {new_data.shape} ")

    # ============================================
    # 💾 결과 저장
    # ============================================
    result_cols = ['grid_id', 'std_date', 'predicted_total', 'acs_cnt']
    results = new_data[result_cols].copy() # SettingWithCopyWarning 방지를 위해 .copy() 사용
    results["reg_dttm"] = datetime.now()

    results["grid_id"] = results["grid_id"].astype(str)
    
    output_table = 'tb_wifi_prediction'
    logger.debug(f"Writing to table: {output_table}. Data shape: {results.shape}")
    results.to_sql(output_table, target_engine, schema='public', if_exists='append', index=False)
    logger.info(f"✅ 예측 결과 저장 완료 to {output_table}")

if __name__ == "__main__":
    main()