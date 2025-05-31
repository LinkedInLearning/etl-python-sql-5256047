import polars as pl

# https://data.opendatasoft.com/explore/dataset/tomme-des-pyrenees-aire-geographique-siqo%40agriculture-opendatapaysbasque/information/?disjunctive.commune

fichier_parquet = 'tomme-des-pyrenees.parquet'

# chercher la commune de Camurac
# afficher seulement les colonnes produit et code_insee