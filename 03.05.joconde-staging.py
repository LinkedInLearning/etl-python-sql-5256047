import polars as pl
import outils
from sqlalchemy import MetaData, create_engine, insert, text
from dotenv import load_dotenv
import urllib, logging, os
import yaml

with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

logging.basicConfig(
    filename="staging.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

load_dotenv()

params = urllib.parse.quote_plus(
    "DRIVER=ODBC Driver 17 for SQL Server;"
    f"SERVER={config["staging"]["server"]};"
    f"DATABASE={config["staging"]["database"]};"
    f"UID={os.getenv("JOCONDE_IMPORT_USER")};"
    f"PWD={os.getenv("JOCONDE_IMPORT_PWD")};"
    "TrustServerCertificate=yes;"
)

fichier = 'C:\\Users\\linkedin\\base-joconde-extrait.json'
fichier_cache = "joconde_cache.feather"

if os.path.exists(fichier_cache):
    print("📦 Chargement depuis le cache feather...")
    df = pl.read_ipc(fichier_cache)
else:
    print("📥 Chargement depuis le JSON source...")
    df = pl.read_json(fichier, infer_schema_length=10000, schema_overrides={"references_merimee": pl.Utf8})

    # Sauvegarde rapide
    df.write_ipc(fichier_cache)

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
    outils.chronometre_logging_lambda("Import SQL Server", lambda: conn.execute(insert(joconde_table), records))

logging.info("Données importées")

