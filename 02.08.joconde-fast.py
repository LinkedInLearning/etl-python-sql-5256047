import polars as pl
import outils
from sqlalchemy import MetaData, Table, Column, String, Text, create_engine, insert, delete
from dotenv import load_dotenv
import urllib, logging, os
import yaml

with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

load_dotenv()

params = urllib.parse.quote_plus(
    "DRIVER=ODBC Driver 17 for SQL Server;"
    "SERVER=localhost;"
    "DATABASE=Joconde;"
    "UID=rudi;"
    f"PWD={os.getenv("SQLSERVER_PASSWORD")};"
    "TrustServerCertificate=yes;"
)

@outils.chronometre_logging
def charger_fichier(path):
    return pl.read_json(path, infer_schema_length=10000, schema_overrides={"references_merimee": pl.Utf8})

fichier = 'C:\\Users\\linkedin\\base-joconde-extrait.json'
df = charger_fichier(fichier)

engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}"
                       ,fast_executemany=True
                       )

metadata = MetaData()

joconde_table = Table("joconde", metadata,
    Column("reference", String),
    Column("appellation", String),
    Column("auteur", String),
    Column("date_creation", String),
    Column("denomination", String),
    Column("region", String),
    Column("departement", String),
    Column("ville", String),
    Column("description", Text),
)
metadata.create_all(engine)

records = df.to_dicts()

with engine.begin() as conn:
    conn.execute(delete(joconde_table))
    outils.chronometre_logging_lambda("Import SQL Server", lambda: conn.execute(insert(joconde_table), records))


logging.info("Données importées")

