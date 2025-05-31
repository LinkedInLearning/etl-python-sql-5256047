import polars as pl
import os

def taille_fichier(path):
    taille_octets = os.path.getsize(path)
    taille_mo = taille_octets / (1024 * 1024)
    return round(taille_mo, 2)

fichier_json = 'C:\\Users\\linkedin\\base-joconde-extrait.json'
fichier_parquet = 'C:\\Users\\linkedin\\joconde.parquet'

df = pl.read_json(fichier_json, infer_schema_length=10000, schema_overrides={"references_merimee": pl.Utf8})

df = df.select([
    "reference", "appellation", "auteur",
    "date_creation", "denomination", "region",
    "departement", "ville", "description"
])

print(df.columns)

df.write_parquet(fichier_parquet)

df_recharge = pl.read_parquet(fichier_parquet)
print(df_recharge.head())

print(f"📄 Taille du fichier JSON     : {taille_fichier(fichier_json)} Mo")
print(f"📦 Taille du fichier Parquet  : {taille_fichier(fichier_parquet)} Mo")