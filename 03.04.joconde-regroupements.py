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

# print(df.select("presence_image").head())

print(
    df.group_by("region").agg(
        pl.len().alias("nombre_oeuvres")
    ).sort("nombre_oeuvres", descending=True)
)

print(
    df.group_by("nom_officiel_musee").agg(
        pl.col("denomination").n_unique().alias("types_objets")
    ).sort("types_objets", descending=True)
)

print(
    df.group_by("departement").agg([
        (pl.col("presence_image") == "oui").sum().alias("avec_image"),
        (pl.col("presence_image") != "oui").sum().alias("sans_image"),
        pl.len().alias("total")
    ]).sort("avec_image", descending=True)
)

print(
    df.group_by("region").agg(
        pl.col("description").str.len_chars().mean().alias("moyenne_longueur_description")
    ).sort("moyenne_longueur_description", descending=True)
)
