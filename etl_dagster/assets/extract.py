from dagster import asset
import polars as pl
import os
from utils import get_config
from dotenv import load_dotenv

load_dotenv()

@asset
def donnees_brutes() -> pl.DataFrame:
    config = get_config()
    source = config["fichiers"]["source"]
    cache = config["fichiers"]["cache"]

    if os.path.exists(cache):
        return pl.read_ipc(cache)
    else:
        df = pl.read_json(source, infer_schema_length=10000, schema_overrides={"references_merimee": pl.Utf8})
        df.write_ipc(cache)
        return df
