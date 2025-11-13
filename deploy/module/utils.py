import os
import logging
from sqlalchemy import create_engine
from dotenv import load_dotenv
import geopandas as gpd

# .env 파일 로드 (한 번만)
load_dotenv()

def setup_logger(script_name) -> logging.Logger:
    """
    스크립트 이름을 기반으로 로거를 설정합니다.
    로그 파일은 LOG_DIR 환경변수에 지정된 디렉토리에 저장됩니다.
    디렉토리가 없으면 생성합니다.
    Parameters
    ----------
    script_name : str
        로거 이름 및 로그 파일 이름에 사용될 스크립트 이름
    """
    log_dir = os.getenv("LOG_DIR", "./logs")
    log_file_path = os.path.join(log_dir, f"{script_name}.log")
    if not os.path.exists(log_dir):
        os.makedirs(log_dir, exist_ok=True)
    logger = logging.getLogger(script_name)
    if not logger.handlers:
        logging.basicConfig(
            filename=log_file_path,
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
    return logger

def get_engine_from_env(
    user_env="DB_USER",
    pass_env="DB_PASS",
    host_env="DB_HOST",
    port_env="DB_PORT",
    name_env="DB_NAME"
):
    """
    기본 yeosu_db의 접속 정보를 .env에서 읽어 SQLAlchemy 엔진을 생성합니다.
    """
    db_config = {
        "DB_USER": os.getenv(user_env),
        "DB_PASS": os.getenv(pass_env),
        "DB_HOST": os.getenv(host_env),
        "DB_PORT": os.getenv(port_env),
        "DB_NAME": os.getenv(name_env)
    }
    url = (
        f"postgresql+psycopg2://{db_config['DB_USER']}:{db_config['DB_PASS']}"
        f"@{db_config['DB_HOST']}:{db_config['DB_PORT']}/{db_config['DB_NAME']}"
    )
    return create_engine(url)

def get_src_dir():
    """
    소스 코드 디렉토리 경로를 반환합니다.
    DATA_DIR 환경변수가 설정되어 있으면 해당 값을 반환하고,
    그렇지 않으면 기본값 '../data'를 반환합니다.
    """
    return os.getenv("DATA_DIR", "../data")

def get_grid_id(points_gdf: gpd.GeoDataFrame, grid_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    각 포인트(GeoDataFrame)가 어느 격자(grid_gdf)에 포함되는지를 계산하여,
    원본 points_gdf에 grid_id 컬럼을 추가해 반환합니다.

    Parameters
    ----------
    points_gdf : geopandas.GeoDataFrame
        격자 매핑 대상이 되는 포인트 데이터.
        - 반드시 geometry 컬럼을 포함해야 합니다.
        - 좌표계(CRS)가 grid_gdf와 다를 경우 자동 변환됩니다.

    grid_gdf : geopandas.GeoDataFrame
        격자(폴리곤) 데이터. 반드시 다음 컬럼을 포함해야 합니다.
        - 'id' : 각 격자의 고유 식별자 (함수 내부에서 grid_id로 이름 변경)
        - 'geometry' : Polygon 또는 MultiPolygon

    Returns
    -------
    geopandas.GeoDataFrame
        원본 points_gdf에 grid_id 컬럼이 추가된 GeoDataFrame을 반환합니다.

    Notes
    -----
    - 격자와 포인트 간의 공간관계는 "intersects" 기준입니다.
      즉, 포인트가 격자 경계 위에 있어도 포함으로 처리됩니다.
    - 포함되지 않은 포인트는 grid_id가 NaN으로 반환됩니다.
    - 대용량 데이터의 경우 grid_gdf에 sindex(공간 인덱스)가 자동 생성됩니다.

    Example
    -------
    >>> import geopandas as gpd
    >>> from shapely.geometry import Point
    >>> from spatial_utils import get_grid_id

    >>> # 포인트 데이터 예시
    >>> points = gpd.GeoDataFrame(geometry=[Point(127.03, 37.5), Point(127.05, 37.52)], crs="EPSG:4326")

    >>> # 격자 데이터 (grid_id, geometry)
    >>> grid = gpd.read_file("grid_layer.geojson")

    >>> # 매핑 실행
    >>> result = get_grid_id(points, grid)
    >>> result.head()

    geometry             grid_id
    POINT(127.03 37.5)   10021
    POINT(127.05 37.52)  10022
    """

    # 1️⃣ 좌표계 통일
    if points_gdf.crs != grid_gdf.crs:
        points_gdf = points_gdf.to_crs(grid_gdf.crs)

    # 2️⃣ 필요 컬럼만 남기기 (성능 최적화)
    grid_gdf = grid_gdf[['id', 'geometry']].rename(columns={'id': 'grid_id'})

    # 3️⃣ 공간 인덱스가 없으면 생성 (대용량 대비)
    if not hasattr(grid_gdf, "sindex") or grid_gdf.sindex is None:
        _ = grid_gdf.sindex

    # 4️⃣ 공간 조인 (경계 포함)
    joined = gpd.sjoin(
        points_gdf,
        grid_gdf,
        how="left",             # 모든 포인트 유지
        predicate="intersects"  # 경계 포함
    )

    # 5️⃣ 결과 정리 — 원본 포인트에 grid_id 컬럼 추가
    joined = joined.drop(columns=["index_right"], errors="ignore")
    result = points_gdf.copy()
    result["grid_id"] = joined["grid_id"].values.astype(int)

    return result
