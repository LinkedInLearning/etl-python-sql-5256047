import polars as pl
import outils
from sqlalchemy import MetaData, Table, Column, String, Text, create_engine, insert, delete
from dotenv import load_dotenv
import urllib, logging, os
import yaml
from datetime import date

with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

load_dotenv()

fichier = 'C:\\Users\\linkedin\\base-joconde-extrait.json'
df = pl.read_json(fichier, infer_schema_length=10000, schema_overrides={"references_merimee": pl.Utf8})

# Afficher la liste des colonnes
print(df.columns)
df = df.select(["reference", "appellation", "ville", "date_de_mise_a_jour"])
print(df.head(10))

# Convertir la colonne en type date (si ce n'est pas déjà le cas)
df = df.with_columns(
    pl.col("date_de_mise_a_jour").str.strptime(pl.Date, "%Y-%m-%d")
)

# Trouver la date maximale
max_date = df.select(pl.col("date_de_mise_a_jour").max()).to_series()[0]

print("Date maximale :", max_date)

date_seuil = date(2025, 5, 1)

df_filtre = df.filter(
    pl.col("date_de_mise_a_jour") > date_seuil
)

print(df_filtre.head(10))
