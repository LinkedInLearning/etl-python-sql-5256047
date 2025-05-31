import polars as pl
import outils

@outils.chronometre
def charger_fichier(path):
    return pl.read_json(path, infer_schema_length=10000, schema_overrides={"references_merimee": pl.Utf8})

fichier = 'C:\\Users\\linkedin\\base-joconde-extrait.json'
df = charger_fichier(fichier)

print(f"Mémoire utilisée : {round(df.estimated_size(unit='b') / (1024**2), 2)} Mo")

print(df.head(5))
df = df.select(["reference", "appellation", "ville"])
print(df.columns)

# Gérer les valeurs manquantes :
# df = df.drop_nulls()
