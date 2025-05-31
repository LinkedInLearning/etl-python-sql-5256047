import polars as pl
import logging, yaml, os, locale

# Définir le format local (français standard)
locale.setlocale(locale.LC_ALL, 'fr_FR.UTF-8')
# locale.setlocale(locale.LC_ALL, 'French_France.1252')

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


# compter les null par colonne
print(df.null_count())

# supprimer les lignes nulles
print(f"{locale.format_string("%d", df.height, grouping=True)} lignes dans le DataFrame")
df = df.drop_nulls(subset=["titre"])
print(f"{locale.format_string("%d", df.height, grouping=True)} lignes dans le DataFrame")

print(df.select(["epoque"]).head())

df = df.with_columns(
    pl.col("epoque").fill_null("inconnu")
)

print(df.select(["epoque"]).head())

df = df.unique(subset=["titre", "code_museofile"], keep='first', maintain_order=True)

print(df.head())
