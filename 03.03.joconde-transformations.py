import polars as pl
import logging, yaml, os, locale, json
from datetime import datetime, timezone

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

print(df.columns)

df = df.with_columns([
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

print(
df.select(["annee_creation", "region_normalisee",
           "description_resumee", "artiste_protégé", "date_import"]).head()
)