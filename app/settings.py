import logging
import os

import sqlalchemy

DB_USER = os.environ["POSTGRES_USER"]
DB_PWD = os.environ["POSTGRES_PASSWORD"]
DB_HOST = os.environ["POSTGRES_HOST"]
DB_PORT = os.environ["POSTGRES_PORT"]
DB_SCHEMA = os.environ["POSTGRES_DB"]
DB_URL = f"postgresql://{DB_USER}:{DB_PWD}@{DB_HOST}:{DB_PORT}/{DB_SCHEMA}"
BART_API_TOKEN = os.environ["BART_API_TOKEN"]
MAPBOX_ACCESS_TOKEN = os.environ["MAPBOX_ACCESS_TOKEN"]
engine = sqlalchemy.create_engine(DB_URL)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

log = logging.getLogger()
