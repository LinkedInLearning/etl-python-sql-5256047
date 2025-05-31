import polars as pl
import outils
from sqlalchemy import MetaData, Table, Column, String, Text, create_engine, insert
from dotenv import load_dotenv
import urllib, logging, os, shutil, time, json
import yaml
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

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

engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

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

# --- Traitement d'un fichier JSON ---
def traiter_fichier(filepath):
    logging.info(f"Nouveau fichier détecté : {filepath}")
    try:
        with open(filepath, encoding="utf-8") as f:
            data = json.load(f)

        with engine.begin() as conn:
            for i, record in enumerate(data):
                try:
                    row = {
                        "reference": record.get("reference"),
                        "appellation": record.get("appellation"),
                        "auteur": record.get("auteur"),
                        "date_creation": record.get("date_creation"),
                        "denomination": record.get("denomination"),
                        "region": record.get("region"),
                        "departement": record.get("departement"),
                        "ville": record.get("ville"),
                        "description": record.get("description"),
                    }
                    conn.execute(insert(joconde_table).values(**row))
                    logging.info(f"OK {i+1}/{len(data)} : {row['reference']}")
                except Exception as e:
                    logging.exception(f"Erreur ligne {i+1}: {e}")

        # Déplacer le fichier une fois traité
        shutil.move(filepath, os.path.join(config["watchdog"]["archive_directory"], os.path.basename(filepath)))
        logging.info("Fichier traité et archivé.")

    except Exception as e:
        logging.exception(f"Erreur de traitement : {e}")


# --- Gestionnaire d'événements ---
class Handler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory or not event.src_path.endswith(".json"):
            return
        time.sleep(1)  # attendre que le fichier soit totalement écrit
        traiter_fichier(event.src_path)

# --- Mise en place de l'observateur ---
if __name__ == "__main__":
    os.makedirs(config["watchdog"]["input_directory"], exist_ok=True)
    os.makedirs(config["watchdog"]["archive_directory"], exist_ok=True)

    observer = Observer()
    observer.schedule(Handler(), path=config["watchdog"]["input_directory"], recursive=False)
    observer.start()

    logging.info(f"Surveillance du dossier '{config["watchdog"]["input_directory"]}' pour les nouveaux fichiers JSON...")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

