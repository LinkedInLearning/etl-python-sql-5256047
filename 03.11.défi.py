import polars as pl

# https://data.opendatasoft.com/explore/dataset/tomme-des-pyrenees-aire-geographique-siqo%40agriculture-opendatapaysbasque/information/?disjunctive.commune

fichier_parquet = 'tomme-des-pyrenees.parquet'

print(pl.read_parquet_schema(fichier_parquet))

df = pl.read_parquet(fichier_parquet)

# Afficher le nombre de communes de production de tomme des Pyrénées par département