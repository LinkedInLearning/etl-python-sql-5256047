from dagster import asset
import polars as pl
from datetime import datetime, timezone

@asset
def donnees_transformees(donnees_brutes: pl.DataFrame) -> pl.DataFrame:
    return donnees_brutes.with_columns([
        pl.col("date_creation").str.extract(r"(\d{4})", 1).cast(pl.Int64).alias("annee_creation"),
        pl.col("region").str.to_titlecase().alias("region_normalisee"),
        pl.when(pl.col("description").str.len_chars() > 200)
          .then(pl.col("description").str.slice(0, 200) + "...")
          .otherwise(pl.col("description")).alias("description_resumee"),
        pl.col("artiste_sous_droits").is_not_null().alias("artiste_protégé"),
        pl.lit(datetime.now(timezone.utc).date()).alias("date_import")
    ])
