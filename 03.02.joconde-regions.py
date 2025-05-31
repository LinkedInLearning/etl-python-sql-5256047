import polars as pl
import logging, yaml, os, locale, json

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

regions = (
    df.group_by("region")
      .len()
      .sort("region")
      .select("region")
      .to_series()
      .to_list()
)

with open("config_regions.json", "w", encoding="utf-8") as f:
    json.dump({"regions": regions}, f, ensure_ascii=False, indent=2)