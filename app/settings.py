import logging
import os

import sqlalchemy
import boto3

ssm = boto3.client('ssm')


def _get_config(key):
    try:
        val = os.environ[key]
    except KeyError:
        val = ssm.get_parameter(Name=f'/bart-ridership/{key}', WithDecryption=True)['Parameter']['Value']
    return val


DB_USER = _get_config("POSTGRES_USER")
DB_PWD = _get_config("POSTGRES_PASSWORD")
DB_HOST = _get_config("POSTGRES_HOST")
DB_PORT = _get_config("POSTGRES_PORT")
DB_SCHEMA = _get_config("POSTGRES_DB")
DB_URL = f"postgresql://{DB_USER}:{DB_PWD}@{DB_HOST}:{DB_PORT}/{DB_SCHEMA}"
BART_API_TOKEN = _get_config("BART_API_TOKEN")
MAPBOX_ACCESS_TOKEN = _get_config("MAPBOX_ACCESS_TOKEN")
engine = sqlalchemy.create_engine(DB_URL)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

log = logging.getLogger()
