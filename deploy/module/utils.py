import os
import logging
from sqlalchemy import create_engine
from dotenv import load_dotenv

# .env 파일 로드 (한 번만)
load_dotenv()

def setup_logger(script_name) -> logging.Logger:
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
    return os.getenv("SRC_DIR", "../data")