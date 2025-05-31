from dagster import job, op, get_dagster_logger, ResourceDefinition
import polars as pl
from dotenv import load_dotenv
import logging, os, urllib
import yaml
from datetime import datetime, timezone
from sqlalchemy import MetaData, create_engine, text, insert

with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

load_dotenv()
fichier_cache = config["fichiers"]["cache"]

@op
def extract_json() -> pl.DataFrame:
    fichier = config["fichiers"]["source"]

    if os.path.exists(fichier_cache):
        df = pl.read_ipc(fichier_cache)
    else:
        df = pl.read_json(fichier, infer_schema_length=10000, schema_overrides={"references_merimee": pl.Utf8})

        # Sauvegarde rapide
        df.write_ipc(fichier_cache)

    return df

@op
def transform_data(df: pl.DataFrame) -> pl.DataFrame:
    df_clean = df.with_columns([
        # Extraire l’année comme entier à partir de la date de création
        pl.col("date_creation").str.extract(r"(\d{4})", 1).cast(pl.Int64).alias("annee_creation"),

        # Normaliser les noms de région : capitaliser la première lettre
        pl.col("region").str.to_titlecase().alias("region_normalisee"),

        # Raccourcir les descriptions trop longues
        pl.when(pl.col("description").str.len_chars() > 200)
        .then(pl.col("description").str.slice(0, 200) + "...")
        .otherwise(pl.col("description"))
        .alias("description_resumee"),

        # Marquer les œuvres avec artiste sous droits ou non
        pl.col("artiste_sous_droits").is_not_null().alias("artiste_protégé"),

        # Ajouter une date d’import (UTC)
        pl.lit(datetime.now(timezone.utc).date()).alias("date_import")
    ])

    return df_clean

@op
def load_to_sqlserver(df: pl.DataFrame):
    staging = config["staging"]

    params = urllib.parse.quote_plus(
        "DRIVER=ODBC Driver 17 for SQL Server;"
        f"SERVER={staging["server"]};"
        f"DATABASE={staging["database"]};"
        f"UID={os.getenv("JOCONDE_IMPORT_USER")};"
        f"PWD={os.getenv("JOCONDE_IMPORT_PWD")};"
        "TrustServerCertificate=yes;"
    )

    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}", fast_executemany=True)

    metadata = MetaData(schema="staging")
    metadata.reflect(bind=engine)

    joconde_table = metadata.tables[config["staging"]["table"]]

    records = [
        {
            **row,
            "source_system": config["audit"]["source_system"],
            "load_process": config["audit"]["load_process"]
        }
        for row in df.to_dicts()
    ]

    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {config['staging']['table']};"))
        conn.execute(insert(joconde_table), records)

    # df.iloc[[0]].to_sql('joconde', con=engine, schema='staging', if_exists='append', index=False)
    # df.to_pandas().to_sql(staging["table_prefect"], engine, if_exists="replace", index=False, schema="staging")
    # engine.close()

@job
def etl_flow():
    df_raw = extract_json()
    df_clean = transform_data(df_raw)
    load_to_sqlserver(df_clean)

