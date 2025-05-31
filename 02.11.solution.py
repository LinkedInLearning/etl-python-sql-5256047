import polars as pl

fichier_parquet = 'tomme-des-pyrenees.parquet'

print(pl.read_parquet_schema(fichier_parquet))

df = pl.read_parquet(fichier_parquet)

print(df.filter(pl.col("commune") == "Camurac").select(["produit", "code_insee"]))
