import polars as pl
import logging, yaml, os, locale, json
from datetime import datetime

locale.setlocale(locale.LC_ALL, 'fr_FR.UTF-8')

with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
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


# print(df.columns)

with open("config_regions.json", encoding="utf-8") as f:
    config = json.load(f)
    regions_valides = config["regions"]

print(regions_valides)

annee_courante = datetime.now().year

# Ajout de colonnes booléennes pour repérer les anomalies
df = df.with_columns([
    (pl.col("reference").is_null()).alias("anomalie_reference_absente"),
    (pl.col("date_creation")
     .cast(pl.Int64, strict=False)
     .is_between(-3000, annee_courante)
     .not_()).alias("anomalie_date_creation"),
    (pl.col("region").is_in(regions_valides).not_()).alias("anomalie_region_inconnue"),
    (pl.col("description").str.len_chars() < 10).alias("anomalie_description_courte")
])

print(
df.select([
    pl.sum("anomalie_reference_absente").alias("références manquantes"),
    pl.sum("anomalie_date_creation").alias("dates incohérentes"),
    pl.sum("anomalie_region_inconnue").alias("régions inconnues"),
    pl.sum("anomalie_description_courte").alias("descriptions trop courtes"),
])
)