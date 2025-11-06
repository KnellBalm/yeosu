#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
extract_ys_grid_joblib_tqdm.py
---------------------------------
joblib 기반 병렬처리 + tqdm 로그 실시간 출력
pickle 병목 해소 버전
"""

import geopandas as gpd
import pandas as pd
import numpy as np
from shapely import unary_union, wkb
from joblib import Parallel, delayed
from tqdm import tqdm
import logging
import os
from datetime import datetime

def setup_logger():
    log_dir = "./"
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f"filter_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        encoding="utf-8"
    )
    return log_file


def intersect_chunk(chunk, region_wkb):
    """개별 청크 교차 여부"""
    from shapely import wkb
    region_poly = wkb.loads(region_wkb)
    return chunk.geometry.intersects(region_poly)


def main():
    log_file = setup_logger()
    logger = logging.getLogger()
    logger.info("🚀 여수시 격자 필터링 시작")

    # ---------------------------------------------------
    grid_path = "./yeosu_flow_pop/grid_shp/yeosoo_id_wgs84.shp"
    region_path = "../../GIS/sgg/sig.shp"
    output_path = "./yeosu_grid_filtered.geojson"
    simplify_tol = 0.0
    n_jobs = -1
    # ---------------------------------------------------

    grid = gpd.read_file(grid_path)
    region = gpd.read_file(region_path, encoding="cp949")
    region = region[region["SIG_KOR_NM"].str.contains("여수")].set_crs(5179).to_crs(4326)

    if grid.crs != region.crs:
        logger.info("🧭 CRS 불일치 → grid를 region과 동일하게 변환")
        grid = grid.to_crs(region.crs)

    region_poly = unary_union(region.geometry)
    if simplify_tol > 0:
        logger.info(f"🔹 simplify 적용 (tolerance={simplify_tol})")
        region_poly = region_poly.simplify(simplify_tol, preserve_topology=True)
    else:
        logger.info("⚙️ simplify_tol=0 → 단순화 미적용 (원본 유지)")

    # WKB 변환 (pickle보다 훨씬 가벼움)
    region_wkb = wkb.dumps(region_poly)

    # 청크 분할
    n_chunks = os.cpu_count()
    chunks = np.array_split(grid, n_chunks)
    logger.info(f"🧩 총 {len(grid):,}개 격자를 {n_chunks}개 청크로 분할")

    # tqdm + joblib 병행
    results = Parallel(n_jobs=n_jobs, backend="loky")(
        delayed(intersect_chunk)(chunk, region_wkb)
        for chunk in tqdm(chunks, total=len(chunks), desc="여수시 격자 필터링 진행 중", mininterval=2.0)
    )

    mask = pd.concat(results)
    grid_in_region = grid.loc[mask.values].copy()
    logger.info(f"✅ 필터링 완료: {len(grid_in_region):,} / {len(grid):,} 격자 유지")

    grid_in_region.to_file(output_path, driver="GeoJSON")
    logger.info(f"💾 결과 저장 완료: {os.path.abspath(output_path)}")
    logger.info("✅ 스크립트 종료")

    print(f"로그 파일: {log_file}")


if __name__ == "__main__":
    main()
