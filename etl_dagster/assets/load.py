from dagster import asset
from sqlalchemy import MetaData, create_engine, text, insert
import urllib, os
from utils import get_config
from dotenv import load_dotenv

load_dotenv()

@asset
def chargement_sqlserver(donnees_transformees):
    config = get_config()
    staging = config["staging"]

    params = urllib.parse.quote_plus(
        "DRIVER=ODBC Driver 17 for SQL Server;"
        f"SERVER={staging['server']};"
        f"DATABASE={staging['database']};"
        f"UID={os.getenv('JOCONDE_IMPORT_USER')};"
        f"PWD={os.getenv('JOCONDE_IMPORT_PWD')};"
        "TrustServerCertificate=yes;"
    )

    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}", fast_executemany=True)
    metadata = MetaData(schema="staging")
    metadata.reflect(bind=engine)
    table = metadata.tables[staging["table"]]

    records = [
        {
            **row,
            "source_system": config["audit"]["source_system"],
            "load_process": config["audit"]["load_process"]
        }
        for row in donnees_transformees.to_dicts()
    ]

    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {staging['table']};"))
        conn.execute(insert(table), records)
