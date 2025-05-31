import polars as pl

fichier_parquet = 'tomme-des-pyrenees.parquet'

print(pl.read_parquet_schema(fichier_parquet))

df = pl.read_parquet(fichier_parquet)

# Créer une colonne temporaire avec les deux premiers caractères
df = df.with_columns(
    pl.col("code_insee").str.slice(0, 2).alias("departement")
)

print(df.group_by("departement").len().sort("departement"))

# exporter en sqlite
