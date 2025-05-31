import polars as pl
import sqlite3

# Charger les données
df = pl.read_parquet("tomme-des-pyrenees.parquet")

# Ajouter une colonne 'departement'
df = df.with_columns(
    pl.col("code_insee").str.slice(0, 2).alias("departement")
)

# Convertir en DataFrame pandas pour insertion SQLite
df_pd = df.to_pandas()

# Connexion à une base SQLite (créée si elle n'existe pas)
conn = sqlite3.connect("tomme_des_pyrenees.sqlite")

# Insérer les données dans une table
df_pd.to_sql("communes_tomme", conn, if_exists="replace", index=False)

# Fermer la connexion
conn.close()
